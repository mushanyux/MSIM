package handler

import (
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/service"
)

func (h *Handler) pushOffline(ctx *eventbus.PushContext) {
	for _, e := range ctx.Events {
		for _, toUid := range e.OfflineUsers {
			fromUid := e.Conn.Uid
			// 是否是AI
			if fromUid != toUid && h.isAI(toUid) && !e.Frame.GetsyncOnce() && !options.G.IsSystemUid(fromUid) {
				// 处理AI推送
				h.processAIPush(toUid, e)
			}
		}
	}
	service.Webhook.NotifyOfflineMsg(ctx.Events)
}
