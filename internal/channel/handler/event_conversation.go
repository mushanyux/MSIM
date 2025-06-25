package handler

import (
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/service"
)

func (h *Handler) conversation(channelId string, channelType uint8, tagKey string, events []*eventbus.Event) {
	service.ConversationManager.Push(channelId, channelType, tagKey, events)
}
