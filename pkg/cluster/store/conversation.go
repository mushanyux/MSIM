package store

import (
	"context"
	"time"

	"github.com/mushanyux/MSIM/pkg/msdb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (s *Store) AddOrUpdateConversations(conversations []msdb.Conversation) error {
	// 将会话按照slotId来分组
	slotConversationsMap := make(map[uint32][]msdb.Conversation)

	for _, c := range conversations {
		slotId := s.opts.Slot.GetSlotId(c.Uid)
		slotConversationsMap[slotId] = append(slotConversationsMap[slotId], c)
	}

	// 提交最近会话
	timeoutctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	g, _ := errgroup.WithContext(timeoutctx)
	g.SetLimit(100)

	for slotId, conversations := range slotConversationsMap {

		slotId, conversations := slotId, conversations

		g.Go(func() error {
			data, err := EncodeCMDAddOrUpdateConversations(conversations)
			if err != nil {
				return err
			}
			cmd := NewCMD(CMDAddOrUpdateConversations, data)
			cmdData, err := cmd.Marshal()
			if err != nil {
				return err
			}
			_, err = s.opts.Slot.ProposeUntilAppliedTimeout(timeoutctx, slotId, cmdData)
			if err != nil {
				s.Error("ProposeUntilAppliedTimeout failed", zap.Error(err), zap.Uint32("slotId", slotId), zap.Int("conversations", len(conversations)))
				return err
			}
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		s.Error("AddOrUpdateConversations failed", zap.Error(err), zap.Int("conversations", len(conversations)))
		return err
	}
	return err
}

func (s *Store) AddOrUpdateUserConversations(uid string, conversations []msdb.Conversation) error {
	if len(conversations) == 0 {
		return nil
	}
	for i, c := range conversations {
		if c.Id == 0 {
			conversations[i].Id = s.wdb.NextPrimaryKey() // 如果id为0，生成一个新的id
		}
	}
	data, err := EncodeCMDAddOrUpdateUserConversations(uid, conversations)
	if err != nil {
		return err
	}
	cmd := NewCMD(CMDAddOrUpdateUserConversations, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return err
	}
	slotId := s.opts.Slot.GetSlotId(uid)
	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	return err
}

// AddConversationsIfNotExist 添加最近会话，如果存在则不添加
func (s *Store) AddConversationsIfNotExist(conversations []msdb.Conversation) error {

	// 将会话按照slotId来分组
	slotConversationsMap := make(map[uint32][]msdb.Conversation)

	for _, c := range conversations {
		exist, err := s.wdb.ExistConversation(c.Uid, c.ChannelId, c.ChannelType)
		if err != nil {
			return err
		}
		if exist {
			continue
		}
		if c.Id == 0 {
			c.Id = s.wdb.NextPrimaryKey()
		}
		slotId := s.opts.Slot.GetSlotId(c.Uid)
		slotConversationsMap[slotId] = append(slotConversationsMap[slotId], c)
	}

	if len(slotConversationsMap) == 0 {
		return nil
	}

	timeoutctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for slotId, conversations := range slotConversationsMap {
		data, err := EncodeCMDAddOrUpdateConversations(conversations)
		if err != nil {
			return err
		}
		cmd := NewCMD(CMDAddOrUpdateConversationsBatchIfNotExist, data)
		cmdData, err := cmd.Marshal()
		if err != nil {
			return err
		}
		_, err = s.opts.Slot.ProposeUntilAppliedTimeout(timeoutctx, slotId, cmdData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) UpdateConversationDeletedAtMsgSeq(uid string, channelId string, channelType uint8, deletedAtMsgSeq uint64) error {
	data := EncodeCMDUpdateConversationDeletedAtMsgSeq(uid, channelId, channelType, deletedAtMsgSeq)
	cmd := NewCMD(CMDUpdateConversationDeletedAtMsgSeq, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return err
	}
	slotId := s.opts.Slot.GetSlotId(uid)
	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	if err != nil {
		s.Error("UpdateConversationDeletedAtMsgSeq failed", zap.Error(err), zap.String("uid", uid), zap.String("channelId", channelId), zap.Uint8("channelType", channelType), zap.Uint64("deletedAtMsgSeq", deletedAtMsgSeq))
		return err
	}
	return nil
}

// func (s *Store) AddOrUpdateConversationsWithChannel(channelId string, channelType uint8, subscribers []string, readToMsgSeq uint64, conversationType msdb.ConversationType, unreadCount int) error {

// 	// 按照slotId来分组subscribers
// 	slotSubscriberMap := make(map[uint32][]string)

// 	for _, uid := range subscribers {
// 		slotId := s.opts.GetSlotId(uid)
// 		slotSubscriberMap[slotId] = append(slotSubscriberMap[slotId], uid)
// 	}

// 	// 提按最近会话
// 	timeoutctx, cancel := context.WithTimeout(s.ctx, time.Second*10)
// 	defer cancel()

// 	g, _ := errgroup.WithContext(timeoutctx)

// 	nw := time.Now().UnixNano()

// 	for slotId, subscribers := range slotSubscriberMap {

// 		slotId, subscribers := slotId, subscribers

// 		g.Go(func() error {
// 			data := EncodeCMDAddOrUpdateConversationsWithChannel(channelId, channelType, subscribers, readToMsgSeq, conversationType, unreadCount, nw, nw)
// 			cmd := NewCMD(CMDAddOrUpdateConversationsWithChannel, data)
// 			cmdData, err := cmd.Marshal()
// 			if err != nil {
// 				return err
// 			}
// 			_, err = s.opts.Cluster.ProposeDataToSlot(s.ctx, slotId, cmdData)
// 			if err != nil {
// 				return err
// 			}
// 			return nil
// 		})

// 	}
// 	err := g.Wait()
// 	return err
// }

func (s *Store) DeleteConversation(uid string, channelID string, channelType uint8) error {
	data := EncodeCMDDeleteConversation(uid, channelID, channelType)
	cmd := NewCMD(CMDDeleteConversation, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return err
	}
	slotId := s.opts.Slot.GetSlotId(uid)
	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	return err
}

func (s *Store) DeleteConversations(uid string, channels []msdb.Channel) error {
	data := EncodeCMDDeleteConversations(uid, channels)
	cmd := NewCMD(CMDDeleteConversations, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return err
	}
	slotId := s.opts.Slot.GetSlotId(uid)
	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	return err
}

func (s *Store) GetConversations(uid string) ([]msdb.Conversation, error) {
	return s.wdb.GetConversations(uid)
}

func (s *Store) GetConversationsByType(uid string, tp msdb.ConversationType) ([]msdb.Conversation, error) {
	return s.wdb.GetConversationsByType(uid, tp)
}

func (s *Store) GetConversation(uid string, channelId string, channelType uint8) (msdb.Conversation, error) {
	return s.wdb.GetConversation(uid, channelId, channelType)
}

func (s *Store) GetLastConversations(uid string, tp msdb.ConversationType, updatedAt uint64, excludeChannelTypes []uint8, limit int) ([]msdb.Conversation, error) {

	return s.wdb.GetLastConversations(uid, tp, updatedAt, excludeChannelTypes, limit)
}

func (s *Store) GetChannelLastMessageSeq(channelId string, channelType uint8) (uint64, error) {
	seq, _, err := s.wdb.GetChannelLastMessageSeq(channelId, channelType)
	return seq, err
}

func (s *Store) GetChannelConversationLocalUsers(channelId string, channelType uint8) ([]string, error) {

	return s.wdb.GetChannelConversationLocalUsers(channelId, channelType)
}

func (s *Store) UpdateConversationIfSeqGreaterAsync(uid string, channelId string, channelType uint8, readToMsgSeq uint64) error {
	return s.wdb.UpdateConversationIfSeqGreaterAsync(uid, channelId, channelType, readToMsgSeq)
}
