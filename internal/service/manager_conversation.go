package service

import (
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/pkg/msdb"
	msproto "github.com/mushanyux/MSIMGoProto"
)

var ConversationManager IConversationManager

type IConversationManager interface {
	// Push 更新最近会话
	Push(fakeChannelId string, channelType uint8, tagKey string, events []*eventbus.Event)
	// GetUserChannelsFromCache 从缓存中获取用户的某一类型的最近会话集合
	GetUserChannelsFromCache(uid string, conversationType msdb.ConversationType) ([]msproto.Channel, error)
	// DeleteFromCache 删除用户的某个频道的最近会话
	DeleteFromCache(uid string, channelId string, channelType uint8) error
}
