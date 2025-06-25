package handler

import (
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/service"
	"github.com/mushanyux/MSIM/pkg/msdb"
	msproto "github.com/mushanyux/MSIMGoProto"
	"go.uber.org/zap"
)

// 收到流消息
func (h *Handler) onStream(ctx *eventbus.ChannelContext) {

	for _, event := range ctx.Events {
		event.ReasonCode = msproto.ReasonSuccess
	}
	// 持久化流消息
	h.persistStreams(ctx)
	// 发送消息回执
	h.sendack(ctx)
}

// 持久化流消息
func (h *Handler) persistStreams(ctx *eventbus.ChannelContext) {

	events := ctx.Events
	// 存储消息
	streams := h.toPersistStreams(events)

	reasonCode := msproto.ReasonSuccess
	if len(streams) > 0 {
		err := service.Store.AddStreams(ctx.ChannelId, ctx.ChannelType, streams)
		if err != nil {
			h.Error("store stream failed", zap.Error(err), zap.Int("events", len(streams)), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
			reasonCode = msproto.ReasonSystemError
		}
	}
	// 修改原因码
	for _, event := range events {
		for _, stream := range streams {
			if event.StreamNo == stream.StreamNo {
				event.ReasonCode = reasonCode
				break
			}
		}
	}

	// 分发
	for _, e := range events {
		if e.ReasonCode != msproto.ReasonSuccess {
			continue
		}
		cloneEvent := e.Clone()
		cloneEvent.Type = eventbus.EventChannelDistribute
		eventbus.Channel.AddEvent(ctx.ChannelId, ctx.ChannelType, cloneEvent)
	}
	eventbus.Channel.Advance(ctx.ChannelId, ctx.ChannelType)

}

// 转换成存储消息
func (h *Handler) toPersistStreams(events []*eventbus.Event) []*msdb.Stream {

	var streams []*msdb.Stream
	for _, e := range events {
		sendPacket := e.Frame.(*msproto.SendPacket)
		if sendPacket.NoPersist || e.ReasonCode != msproto.ReasonSuccess {
			continue
		}
		stream := &msdb.Stream{
			StreamNo: e.StreamNo,
			StreamId: uint64(e.MessageId),
			Payload:  sendPacket.Payload,
		}
		streams = append(streams, stream)
	}
	return streams
}
