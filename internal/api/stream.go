package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/service"
	"github.com/mushanyux/MSIM/internal/types"
	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/mshttp"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msutil"
	msproto "github.com/mushanyux/MSIMGoProto"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type stream struct {
	s *Server
	mslog.Log
}

func newStream(s *Server) *stream {
	return &stream{
		s:   s,
		Log: mslog.NewMSLog("stream"),
	}
}

// Route route
func (s *stream) route(r *mshttp.MSHttp) {
	r.POST("/stream/open", s.open)   // 流消息开始
	r.POST("/stream/close", s.close) // 流消息结束
}

func (s *stream) open(c *mshttp.Context) {
	var req streamStartReq
	if err := c.BindJSON(&req); err != nil {
		s.Error("数据格式有误！", zap.Error(err))
		c.ResponseError(err)
		return
	}
	if strings.TrimSpace(req.FromUid) == "" {
		req.FromUid = options.G.SystemUID
	}
	channelId := req.ChannelId
	channelType := req.ChannelType
	if strings.TrimSpace(channelId) == "" { //指定了频道 正常发送
		s.Error("无法处理发送消息请求！", zap.Any("req", req))
		c.ResponseError(errors.New("无法处理发送消息请求！"))
		return
	}

	fakeChannelId := channelId
	if channelType == msproto.ChannelTypePerson {
		fakeChannelId = options.GetFakeChannelIDWith(channelId, req.FromUid)
	}

	clientMsgNo := req.ClientMsgNo
	if strings.TrimSpace(clientMsgNo) == "" {
		clientMsgNo = fmt.Sprintf("%s0", msutil.GenUUID())
	}
	streamNo := msutil.GenUUID()

	messageId := options.G.GenMessageId()

	var setting msproto.Setting
	if strings.TrimSpace(streamNo) != "" {
		setting = setting.Set(msproto.SettingStream)
	}

	msg := msdb.Message{
		RecvPacket: msproto.RecvPacket{
			Framer: msproto.Framer{
				RedDot:    msutil.IntToBool(req.Header.RedDot),
				SyncOnce:  msutil.IntToBool(req.Header.SyncOnce),
				NoPersist: msutil.IntToBool(req.Header.NoPersist),
			},
			Setting:     setting,
			MessageID:   messageId,
			ClientMsgNo: clientMsgNo,
			FromUID:     req.FromUid,
			ChannelID:   fakeChannelId,
			ChannelType: channelType,
			Timestamp:   int32(time.Now().Unix()),
			StreamNo:    streamNo,
			StreamId:    uint64(messageId),
			Payload:     req.Payload,
		},
	}

	// 保存消息
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	results, err := service.Store.AppendMessages(timeoutCtx, fakeChannelId, channelType, []msdb.Message{
		msg,
	})
	cancel()
	if err != nil {
		s.Error("保存消息失败！", zap.Error(err))
		c.ResponseError(err)
		return
	}
	if len(results) == 0 {
		s.Error("保存消息失败！没返回结果", zap.Error(errors.New("保存消息失败")))
		c.ResponseError(errors.New("保存消息失败"))
		return
	}
	result := results[0]

	// 添加流元数据
	streamMeta := &msdb.StreamMeta{
		StreamNo:    streamNo,
		ChannelId:   channelId,
		ChannelType: channelType,
		FromUid:     req.FromUid,
		ClientMsgNo: clientMsgNo,
		MessageId:   messageId,
		MessageSeq:  int64(result.Index),
	}
	err = service.Store.AddStreamMeta(streamMeta)
	if err != nil {
		s.Error("添加流元数据失败！", zap.Error(err))
		c.ResponseError(err)
		return
	}

	sendPacket := &msproto.SendPacket{
		Framer: msproto.Framer{
			RedDot:    msutil.IntToBool(req.Header.RedDot),
			SyncOnce:  msutil.IntToBool(req.Header.SyncOnce),
			NoPersist: msutil.IntToBool(req.Header.NoPersist),
		},
		Setting:     setting,
		StreamNo:    streamNo,
		ClientMsgNo: clientMsgNo,
		ChannelID:   channelId,
		ChannelType: channelType,
		Payload:     req.Payload,
	}

	eventbus.Channel.SendMessage(fakeChannelId, channelType, &eventbus.Event{
		Conn: &eventbus.Conn{
			Uid:      req.FromUid,
			DeviceId: options.G.SystemDeviceId,
		},
		Type:       eventbus.EventChannelOnSend,
		Frame:      sendPacket,
		MessageId:  messageId,
		StreamNo:   streamNo,
		StreamFlag: msproto.StreamFlagStart,
	})
	eventbus.Channel.Advance(fakeChannelId, channelType)

	time.Sleep(time.Millisecond * 100) // TODO: 这里等待一下，让消息发送出去,要不然可能会出现流的开始消息还没收到，流消息先到了

	c.JSON(http.StatusOK, gin.H{
		"stream_no": streamNo,
	})

}

func (s *stream) close(c *mshttp.Context) {
	var req streamCloseReq
	if err := c.BindJSON(&req); err != nil {
		s.Error("数据格式有误！", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.ResponseOK()
}

type streamStartReq struct {
	Header      types.MessageHeader `json:"header"`        // 消息头
	ClientMsgNo string              `json:"client_msg_no"` // 客户端消息编号（相同编号，客户端只会显示一条）
	FromUid     string              `json:"from_uid"`      // 发送者UID
	ChannelId   string              `json:"channel_id"`    // 频道ID
	ChannelType uint8               `json:"channel_type"`  // 频道类型
	Payload     []byte              `json:"payload"`       // 消息内容
}

type streamCloseReq struct {
	StreamNo    string `json:"stream_no"`    // 消息流编号
	ChannelId   string `json:"channel_id"`   // 频道ID
	ChannelType uint8  `json:"channel_type"` // 频道类型
}
