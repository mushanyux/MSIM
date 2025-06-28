package service

import (
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/types"
	"github.com/mushanyux/MSIM/pkg/msdb"
	msproto "github.com/mushanyux/MSIMGoProto"
)

var Webhook IWebhook

type IWebhook interface {
	// Online 设备上线
	Online(uid string, deviceFlag msproto.DeviceFlag, connId int64, deviceOnlineCount int, totalOnlineCount int)
	// Offline 设备下线
	Offline(uid string, deviceFlag msproto.DeviceFlag, connId int64, deviceOnlineCount int, totalOnlineCount int)
	// NotifyOfflineMsg 离线消息通知
	NotifyOfflineMsg(events []*eventbus.Event)
	// TriggerEvent 触发事件
	TriggerEvent(event *types.Event)
	// AppendMessageOfNotifyQueue 追加消息到通知队列
	AppendMessageOfNotifyQueue(messages []msdb.Message) error
}
