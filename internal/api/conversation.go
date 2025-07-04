package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/service"
	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/mshttp"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msutil"
	msproto "github.com/mushanyux/MSIMGoProto"
	"go.uber.org/zap"
)

// conversation conversation
type conversation struct {
	s *Server
	mslog.Log
}

func newConversation(s *Server) *conversation {
	return &conversation{
		s:   s,
		Log: mslog.NewMSLog("conversation"),
	}
}

// Route 路由
func (s *conversation) route(r *mshttp.MSHttp) {
	// r.GET("/conversations", s.conversationsList)                    // 获取会话列表 （此接口作废，使用/conversation/sync）
	r.POST("/conversations/clearUnread", s.clearConversationUnread) // 清空会话未读数量
	r.POST("/conversations/setUnread", s.setConversationUnread)     // 设置会话未读数量
	r.POST("/conversations/delete", s.deleteConversation)           // 删除会话
	r.POST("/conversation/sync", s.syncUserConversation)            // 同步会话
	r.POST("/conversation/syncMessages", s.syncRecentMessages)      // 同步会话最近消息

	r.POST("/conversation/channels", s.conversationChannels) // 获取最近会话的频道集合
}

// // Get a list of recent conversations
// func (s *conversation) conversationsList(c *mshttp.Context) {
// 	uid := c.Query("uid")
// 	if strings.TrimSpace(uid) == "" {
// 		c.ResponseError(errors.New("uid cannot be empty"))
// 		return
// 	}

// 	conversations := s.s.conversationManager.GetConversations(uid, 0, nil)
// 	conversationResps := make([]conversationResp, 0)
// 	if len(conversations) > 0 {
// 		for _, conversation := range conversations {
// 			fakeChannelID := conversation.ChannelID
// 			if conversation.ChannelType == msproto.ChannelTypePerson {
// 				fakeChannelID = GetFakeChannelIDWith(uid, conversation.ChannelID)
// 			}
// 			// 获取到偏移位内的指定最大条数的最新消息
// 			message, err := service.Store.LoadMsg(fakeChannelID, conversation.ChannelType, conversation.LastMsgSeq)
// 			if err != nil {
// 				s.Error("Failed to query recent news", zap.Error(err))
// 				c.ResponseError(err)
// 				return
// 			}
// 			messageResp := &MessageResp{}
// 			if message != nil {
// 				messageResp.from(message.(*Message), service.Store)
// 			}
// 			conversationResps = append(conversationResps, conversationResp{
// 				ChannelID:   conversation.ChannelID,
// 				ChannelType: conversation.ChannelType,
// 				Unread:      conversation.UnreadCount,
// 				Timestamp:   conversation.Timestamp,
// 				LastMessage: messageResp,
// 			})
// 		}
// 	}
// 	c.JSON(http.StatusOK, conversationResps)
// }

// 清楚会话未读数量
func (s *conversation) clearConversationUnread(c *mshttp.Context) {
	var req clearConversationUnreadReq
	bodyBytes, err := BindJSON(&req, c)
	if err != nil {
		s.Error("数据格式有误！", zap.Error(err))
		c.ResponseError(err)
		return
	}
	if err := req.Check(); err != nil {
		c.ResponseError(err)
		return
	}

	leaderInfo, err := service.Cluster.SlotLeaderOfChannel(req.UID, msproto.ChannelTypePerson) // 获取频道的领导节点
	if err != nil {
		s.Error("获取频道所在节点失败！", zap.Error(err), zap.String("channelID", req.UID), zap.Uint8("channelType", msproto.ChannelTypePerson))
		c.ResponseError(errors.New("获取频道所在节点失败！"))
		return
	}
	leaderIsSelf := leaderInfo.Id == options.G.Cluster.NodeId
	if !leaderIsSelf {
		s.Debug("转发请求：", zap.String("url", fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path)))
		c.ForwardWithBody(fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path), bodyBytes)
		return
	}

	fakeChannelId := req.ChannelID
	if req.ChannelType == msproto.ChannelTypePerson {
		fakeChannelId = options.GetFakeChannelIDWith(req.UID, req.ChannelID)

	}

	conversation, err := service.Store.GetConversation(req.UID, fakeChannelId, req.ChannelType)
	if err != nil && err != msdb.ErrNotFound {
		s.Error("Failed to query conversation", zap.Error(err))
		c.ResponseError(err)
		return
	}
	updatedAt := time.Now()
	if msdb.IsEmptyConversation(conversation) {
		createdAt := time.Now()
		conversation = msdb.Conversation{
			Type:        msdb.ConversationTypeChat,
			Uid:         req.UID,
			ChannelId:   fakeChannelId,
			ChannelType: req.ChannelType,
			CreatedAt:   &createdAt,
			UpdatedAt:   &updatedAt,
		}
	}

	// 获取此频道最新的消息
	lastMsgSeq, err := s.getChannelLastMsgSeq(fakeChannelId, req.ChannelType)
	if err != nil {
		s.Error("Failed to query last message", zap.Error(err))
		c.ResponseError(err)
		return
	}

	// 如果已读消息序号大于等于未读消息序号，则不更新
	if conversation.ReadToMsgSeq >= lastMsgSeq {
		c.ResponseOK()
		return
	}

	conversation.ReadToMsgSeq = lastMsgSeq
	conversation.UpdatedAt = &updatedAt

	err = service.Store.AddOrUpdateUserConversations(req.UID, []msdb.Conversation{conversation})
	if err != nil {
		s.Error("Failed to add conversation", zap.Error(err))
		c.ResponseError(err)
		return
	}

	c.ResponseOK()
}

func (s *conversation) setConversationUnread(c *mshttp.Context) {
	var req struct {
		UID         string `json:"uid"`
		ChannelID   string `json:"channel_id"`
		ChannelType uint8  `json:"channel_type"`
		Unread      int    `json:"unread"`
	}
	bodyBytes, err := BindJSON(&req, c)
	if err != nil {
		s.Error("数据格式有误！", zap.Error(err))
		c.ResponseError(err)
		return
	}

	if req.UID == "" {
		c.ResponseError(errors.New("UID cannot be empty"))
		return
	}
	if req.ChannelID == "" || req.ChannelType == 0 {
		c.ResponseError(errors.New("channel_id or channel_type cannot be empty"))
		return
	}

	if options.G.ClusterOn() {
		leaderInfo, err := service.Cluster.SlotLeaderOfChannel(req.UID, msproto.ChannelTypePerson) // 获取频道的领导节点
		if err != nil {
			s.Error("获取频道所在节点失败！", zap.Error(err), zap.String("channelID", req.UID), zap.Uint8("channelType", msproto.ChannelTypePerson))
			c.ResponseError(errors.New("获取频道所在节点失败！"))
			return
		}
		leaderIsSelf := leaderInfo.Id == options.G.Cluster.NodeId
		if !leaderIsSelf {
			s.Debug("转发请求：", zap.String("url", fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path)))
			c.ForwardWithBody(fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path), bodyBytes)
			return
		}
	}

	fakeChannelId := req.ChannelID
	if req.ChannelType == msproto.ChannelTypePerson {
		fakeChannelId = options.GetFakeChannelIDWith(req.UID, req.ChannelID)

	}
	// 获取此频道最新的消息
	lastMsgSeq, err := s.getChannelLastMsgSeq(fakeChannelId, req.ChannelType)
	if err != nil {
		s.Error("Failed to query last message", zap.Error(err))
		c.ResponseError(err)
		return
	}

	conversation, err := service.Store.GetConversation(req.UID, fakeChannelId, req.ChannelType)
	if err != nil && err != msdb.ErrNotFound {
		s.Error("Failed to query conversation", zap.Error(err))
		c.ResponseError(err)
		return
	}
	updatedAt := time.Now()
	if msdb.IsEmptyConversation(conversation) {
		createdAt := time.Now()

		conversation = msdb.Conversation{
			Uid:         req.UID,
			Type:        msdb.ConversationTypeChat,
			ChannelId:   fakeChannelId,
			ChannelType: req.ChannelType,
			CreatedAt:   &createdAt,
			UpdatedAt:   &updatedAt,
		}

	} else {

	}

	var readedMsgSeq uint64 = lastMsgSeq

	if uint64(req.Unread) > lastMsgSeq {
		readedMsgSeq = lastMsgSeq - 1
	} else if req.Unread > 0 {
		readedMsgSeq = lastMsgSeq - uint64(req.Unread)
	}

	// 如果已读消息序号大于等于未读消息序号，则不更新
	if conversation.ReadToMsgSeq >= readedMsgSeq {
		c.ResponseOK()
		return
	}

	conversation.ReadToMsgSeq = readedMsgSeq
	conversation.UpdatedAt = &updatedAt

	err = service.Store.AddOrUpdateUserConversations(req.UID, []msdb.Conversation{conversation})
	if err != nil {
		s.Error("Failed to add conversation", zap.Error(err))
		c.ResponseError(err)
		return
	}

	c.ResponseOK()
}

func (s *conversation) deleteConversation(c *mshttp.Context) {
	var req deleteChannelReq
	bodyBytes, err := BindJSON(&req, c)
	if err != nil {
		s.Error("数据格式有误！", zap.Error(err))
		c.ResponseError(err)
		return
	}
	if err := req.Check(); err != nil {
		c.ResponseError(err)
		return
	}

	if options.G.ClusterOn() {
		leaderInfo, err := service.Cluster.SlotLeaderOfChannel(req.UID, msproto.ChannelTypePerson) // 获取频道的领导节点
		if err != nil {
			s.Error("获取频道所在节点失败！", zap.Error(err), zap.String("channelID", req.UID), zap.Uint8("channelType", msproto.ChannelTypePerson))
			c.ResponseError(errors.New("获取频道所在节点失败！"))
			return
		}
		leaderIsSelf := leaderInfo.Id == options.G.Cluster.NodeId

		if !leaderIsSelf {
			s.Debug("转发请求：", zap.String("url", fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path)))
			c.ForwardWithBody(fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path), bodyBytes)
			return
		}
	}
	fakeChannelId := req.ChannelID
	if req.ChannelType == msproto.ChannelTypePerson {
		fakeChannelId = options.GetFakeChannelIDWith(req.UID, req.ChannelID)

	}

	// 获取频道最后一条消息序号
	lastMsgSeq, err := s.getChannelLastMsgSeq(fakeChannelId, req.ChannelType)
	if err != nil {
		s.Error("获取频道最后一条消息序号失败！", zap.Error(err))
		c.ResponseError(err)
		return
	}

	err = service.Store.UpdateConversationDeletedAtMsgSeq(req.UID, fakeChannelId, req.ChannelType, lastMsgSeq)
	if err != nil {
		s.Error("更新最近会话的已删除的消息序号位置失败！", zap.Error(err))
		c.ResponseError(err)
		return
	}

	c.ResponseOK()
}

func (s *conversation) syncUserConversation(c *mshttp.Context) {
	var req struct {
		UID                 string  `json:"uid"`
		Version             int64   `json:"version"`               // 当前客户端的会话最大版本号(客户端最新会话的时间戳)（TODO: 这个参数可以废弃了,使用OnlyUnread）
		LastMsgSeqs         string  `json:"last_msg_seqs"`         // 客户端所有会话的最后一条消息序列号 格式： channelID:channelType:last_msg_seq|channelID:channelType:last_msg_seq
		MsgCount            int64   `json:"msg_count"`             // 每个会话消息数量
		OnlyUnread          uint8   `json:"only_unread"`           // 只返回未读最近会话 1.只返回未读最近会话 0.不限制
		ExcludeChannelTypes []uint8 `json:"exclude_channel_types"` // 排除的频道类型
	}
	bodyBytes, err := BindJSON(&req, c)
	if err != nil {
		s.Error("数据格式有误！", zap.Error(err))
		c.ResponseError(err)
		return
	}

	leaderInfo, err := service.Cluster.SlotLeaderOfChannel(req.UID, msproto.ChannelTypePerson) // 获取频道的领导节点
	if err != nil {
		s.Error("获取频道所在节点失败！!", zap.Error(err), zap.String("channelID", req.UID), zap.Uint8("channelType", msproto.ChannelTypePerson))
		c.ResponseError(errors.New("获取频道所在节点失败！"))
		return
	}
	leaderIsSelf := leaderInfo.Id == options.G.Cluster.NodeId

	if !leaderIsSelf {
		s.Debug("转发请求：", zap.String("url", fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path)))
		c.ForwardWithBody(fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path), bodyBytes)
		return
	}

	var (
		channelLastMsgMap        = s.getChannelLastMsgSeqMap(req.LastMsgSeqs) // 获取频道对应的最后一条消息的messageSeq
		channelRecentMessageReqs = make([]*channelRecentMessageReq, 0, len(channelLastMsgMap))
	)

	// 获取用户最近会话基础数据

	var (
		resps = make([]*syncUserConversationResp, 0)
	)

	// ==================== 获取用户活跃的最近会话 ====================
	conversations, err := service.Store.GetLastConversations(req.UID, msdb.ConversationTypeChat, 0, nil, options.G.Conversation.UserMaxCount)
	if err != nil && err != msdb.ErrNotFound {
		s.Error("获取conversation失败！", zap.Error(err), zap.String("uid", req.UID))
		c.ResponseError(errors.New("获取conversation失败！"))
		return
	}

	if len(conversations) > options.G.Conversation.UserMaxCount/2 {
		s.Warn("警告：用户会话数量超过最大限制的一半！", zap.String("uid", req.UID), zap.Int("count", len(conversations)))
	}

	// 获取真实的频道ID
	getRealChannelId := func(fakeChannelId string, channelType uint8) string {
		realChannelId := fakeChannelId
		if channelType == msproto.ChannelTypePerson {
			from, to := options.GetFromUIDAndToUIDWith(fakeChannelId)
			if req.UID == from {
				realChannelId = to
			} else {
				realChannelId = from
			}
		}
		return realChannelId
	}

	// 获取用户缓存的最近会话
	cacheChannels, err := service.ConversationManager.GetUserChannelsFromCache(req.UID, msdb.ConversationTypeChat)
	if err != nil {
		s.Error("获取用户缓存的最近会话失败！", zap.Error(err), zap.String("uid", req.UID))
		c.ResponseError(errors.New("获取用户缓存的最近会话失败！"))
		return
	}

	// 是否允许同步
	isAllowSync := func(channelId string, channelType uint8) bool {
		if len(req.ExcludeChannelTypes) > 0 {
			for _, excludeChannelType := range req.ExcludeChannelTypes {
				// 存在排除的频道类型并且没在同步的频道集合里
				if excludeChannelType == channelType {
					realChannelId := getRealChannelId(channelId, channelType)
					_, ok := channelLastMsgMap[fmt.Sprintf("%s-%d", realChannelId, channelType)]
					return ok
				}
			}
		}
		return true
	}

	// 将用户缓存的新的频道添加到会话列表中
	for _, cacheChannel := range cacheChannels {
		notNeedAdd := false
		for _, conversation := range conversations {
			if cacheChannel.ChannelID == conversation.ChannelId && cacheChannel.ChannelType == conversation.ChannelType {
				notNeedAdd = true
				break
			}
		}

		if !isAllowSync(cacheChannel.ChannelID, cacheChannel.ChannelType) {
			continue
		}

		if !notNeedAdd {
			conversations = append(conversations, msdb.Conversation{
				ChannelId:   cacheChannel.ChannelID,
				ChannelType: cacheChannel.ChannelType,
				Uid:         req.UID,
				Type:        msdb.ConversationTypeChat,
			})
		}
	}

	// 去掉重复的会话
	conversations = removeDuplicates(conversations)

	// 设置最近会话已读至的消息序列号
	for _, conversation := range conversations {

		// 如果是CMD频道，则不返回
		if options.G.IsCmdChannel(conversation.ChannelId) {
			continue
		}

		if !isAllowSync(conversation.ChannelId, conversation.ChannelType) {
			continue
		}

		realChannelId := getRealChannelId(conversation.ChannelId, conversation.ChannelType)

		msgSeq := channelLastMsgMap[fmt.Sprintf("%s-%d", realChannelId, conversation.ChannelType)]

		if msgSeq != 0 {
			msgSeq = msgSeq + 1 // 如果客户端传递了messageSeq，则需要获取这个messageSeq之后的消息
		} else if req.Version > 0 || req.OnlyUnread == 1 { // 如果客户端传递了version，则获取有新消息的会话
			msgSeq = conversation.ReadToMsgSeq + 1
		}

		// 如果会话被删除，则获取被删除的消息序号
		if conversation.DeletedAtMsgSeq > 0 && conversation.DeletedAtMsgSeq > msgSeq {
			msgSeq = conversation.DeletedAtMsgSeq + 1
		}

		channelRecentMessageReqs = append(channelRecentMessageReqs, &channelRecentMessageReq{
			ChannelId:   conversation.ChannelId,
			ChannelType: conversation.ChannelType,
			LastMsgSeq:  msgSeq,
		})
		// syncUserConversationR := newSyncUserConversationResp(conversation)
		// resps = append(resps, syncUserConversationR)
	}

	// ==================== 获取最近会话的最近的消息列表 ====================
	if req.MsgCount > 0 {
		var channelRecentMessages []*channelRecentMessage

		// 获取用户最近会话的最近消息
		channelRecentMessages, err = s.s.requset.getRecentMessagesForCluster(req.UID, int(req.MsgCount), channelRecentMessageReqs, true)
		if err != nil {
			s.Error("获取最近消息失败！", zap.Error(err), zap.String("uid", req.UID))
			c.ResponseError(errors.New("获取最近消息失败！"))
			return
		}

		for i := 0; i < len(conversations); i++ {
			conversation := conversations[i]
			realChannelId := getRealChannelId(conversation.ChannelId, conversation.ChannelType)
			if conversation.ChannelType == msproto.ChannelTypePerson && realChannelId == options.G.SystemUID { // 系统消息不返回
				continue
			}
			resp := newSyncUserConversationResp(conversation)

			// 填充最近消息
			for _, channelRecentMessage := range channelRecentMessages {
				if conversation.ChannelId == channelRecentMessage.ChannelId && conversation.ChannelType == channelRecentMessage.ChannelType {
					if len(channelRecentMessage.Messages) > 0 {
						lastMsg := channelRecentMessage.Messages[0]
						resp.LastMsgSeq = uint32(lastMsg.MessageSeq)
						resp.LastClientMsgNo = lastMsg.ClientMsgNo
						resp.Timestamp = int64(lastMsg.Timestamp)
						if lastMsg.MessageSeq > uint64(resp.ReadedToMsgSeq) {
							resp.Unread = int(lastMsg.MessageSeq - uint64(resp.ReadedToMsgSeq))
						}

						resp.Version = time.Unix(int64(lastMsg.Timestamp), 0).UnixNano()
					}

					resp.Recents = channelRecentMessage.Messages
					if len(resp.Recents) > 0 {
						lastMsg := resp.Recents[len(resp.Recents)-1]
						// 如果最后一条消息是自己发送的，则已读序号为最后一条消息的序号
						if lastMsg.FromUID == req.UID {
							resp.ReadedToMsgSeq = uint32(lastMsg.MessageSeq)
							resp.Unread = 0
						}
					}
					break
				}
			}

			// 比较客户端的序号和服务端的序号，如果客户端的序号大于等于服务端的序号，则不返回
			msgSeq := channelLastMsgMap[fmt.Sprintf("%s-%d", conversation.ChannelId, conversation.ChannelType)]

			if msgSeq != 0 && msgSeq >= uint64(resp.LastMsgSeq) {
				continue
			}

			if len(resp.Recents) > 0 {
				resps = append(resps, resp)
			}
		}
	}

	c.JSON(http.StatusOK, resps)
}

func removeDuplicates(conversations []msdb.Conversation) []msdb.Conversation {
	seen := make(map[string]bool)
	result := []msdb.Conversation{}

	for _, conversation := range conversations {
		channelKey := msutil.ChannelToKey(conversation.ChannelId, conversation.ChannelType)
		if !seen[channelKey] {
			seen[channelKey] = true
			result = append(result, conversation)
		}
	}
	return result
}

func (s *conversation) getChannelLastMsgSeqMap(lastMsgSeqs string) map[string]uint64 {
	channelLastMsgSeqStrList := strings.Split(lastMsgSeqs, "|")
	channelLastMsgMap := map[string]uint64{} // 频道对应的messageSeq
	for _, channelLastMsgSeqStr := range channelLastMsgSeqStrList {
		channelLastMsgSeqs := strings.Split(channelLastMsgSeqStr, ":")
		if len(channelLastMsgSeqs) != 3 {
			continue
		}
		channelID := channelLastMsgSeqs[0]
		channelTypeI, _ := strconv.Atoi(channelLastMsgSeqs[1])
		lastMsgSeq, _ := strconv.ParseUint(channelLastMsgSeqs[2], 10, 64)
		channelLastMsgMap[fmt.Sprintf("%s-%d", channelID, channelTypeI)] = lastMsgSeq
	}
	return channelLastMsgMap
}

func (s *conversation) syncRecentMessages(c *mshttp.Context) {
	var req struct {
		UID         string                     `json:"uid"`
		Channels    []*channelRecentMessageReq `json:"channels"`
		MsgCount    int                        `json:"msg_count"`
		OrderByLast int                        `json:"order_by_last"`
	}
	if err := c.BindJSON(&req); err != nil {
		s.Error("数据格式有误！", zap.Error(err))
		c.ResponseError(errors.New("数据格式有误！"))
		return
	}
	msgCount := req.MsgCount
	if msgCount <= 0 {
		msgCount = 15
	}
	channelRecentMessages, err := s.s.requset.getRecentMessages(req.UID, msgCount, req.Channels, msutil.IntToBool(req.OrderByLast))
	if err != nil {
		s.Error("获取最近消息失败！", zap.Error(err))
		c.ResponseError(errors.New("获取最近消息失败！"))
		return
	}
	c.JSON(http.StatusOK, channelRecentMessages)
}

func (s *conversation) conversationChannels(c *mshttp.Context) {
	var req struct {
		UID string `json:"uid"`
	}

	bodyBytes, err := BindJSON(&req, c)
	if err != nil {
		s.Error("数据格式有误！", zap.Error(err))
		c.ResponseError(err)
		return
	}

	leaderInfo, err := service.Cluster.SlotLeaderOfChannel(req.UID, msproto.ChannelTypePerson) // 获取频道的领导节点
	if err != nil {
		s.Error("获取频道所在节点失败！!", zap.Error(err), zap.String("channelID", req.UID), zap.Uint8("channelType", msproto.ChannelTypePerson))
		c.ResponseError(errors.New("获取频道所在节点失败！"))
		return
	}
	leaderIsSelf := leaderInfo.Id == options.G.Cluster.NodeId

	if !leaderIsSelf {
		s.Debug("转发请求：", zap.String("url", fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path)))
		c.ForwardWithBody(fmt.Sprintf("%s%s", leaderInfo.ApiServerAddr, c.Request.URL.Path), bodyBytes)
		return
	}

	// ==================== 获取用户活跃的最近会话 ====================
	conversations, err := service.Store.GetLastConversations(req.UID, msdb.ConversationTypeChat, 0, nil, options.G.Conversation.UserMaxCount)
	if err != nil && err != msdb.ErrNotFound {
		s.Error("获取conversation失败！", zap.Error(err), zap.String("uid", req.UID))
		c.ResponseError(errors.New("获取conversation失败！"))
		return
	}

	// 获取用户缓存的最近会话
	cacheChannels, err := service.ConversationManager.GetUserChannelsFromCache(req.UID, msdb.ConversationTypeChat)
	if err != nil {
		s.Error("获取用户缓存的最近会话失败！", zap.Error(err), zap.String("uid", req.UID))
		c.ResponseError(errors.New("获取用户缓存的最近会话失败！"))
		return
	}

	for _, cacheChannel := range cacheChannels {
		exist := false
		for _, conversation := range conversations {
			if cacheChannel.ChannelID == conversation.ChannelId && cacheChannel.ChannelType == conversation.ChannelType {
				exist = true
				break
			}
		}

		if !exist {
			conversations = append(conversations, msdb.Conversation{
				Uid:         req.UID,
				ChannelId:   cacheChannel.ChannelID,
				ChannelType: cacheChannel.ChannelType,
				Type:        msdb.ConversationTypeChat,
			})
		}
	}

	// 去掉重复的会话
	conversations = removeDuplicates(conversations)

	channels := make([]interface{}, 0, len(conversations))

	for _, conversation := range conversations {
		channels = append(channels, map[string]interface{}{
			"channel_id":   conversation.ChannelId,
			"channel_type": conversation.ChannelType,
		})
	}
	c.JSON(http.StatusOK, channels)
}

// getChannelLastMsgSeqWithCache 使用缓存获取频道最后消息序号
func (s *conversation) getChannelLastMsgSeq(channelId string, channelType uint8) (uint64, error) {

	return service.Store.GetLastMsgSeq(channelId, channelType)
}
