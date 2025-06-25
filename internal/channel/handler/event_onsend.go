package handler

import (
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/track"
)

func (h *Handler) onSend(ctx *eventbus.ChannelContext) {
	// 记录消息轨迹
	for _, event := range ctx.Events {
		event.Track.Record(track.PositionChannelOnSend)
	}
	// 权限判断
	h.permission(ctx)
	// 消息持久化
	h.persist(ctx)
	// 发送消息回执
	h.sendack(ctx)

}
