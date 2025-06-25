package handler

import (
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/service"
	"github.com/mushanyux/MSIM/internal/types"
	"go.uber.org/zap"
)

func (h *Handler) webhook(ctx *eventbus.ChannelContext) {
	var err error
	if options.G.WebhookOn(types.EventMsgNotify) {
		err = service.Webhook.AppendMessageOfNotifyQueue(h.toPersistMessages(ctx.ChannelId, ctx.ChannelType, ctx.Events))
		if err != nil {
			h.Error("webhook append message of notify queue failed", zap.Error(err), zap.Int("msgs", len(ctx.Events)), zap.String("channelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
		}
	}
}
