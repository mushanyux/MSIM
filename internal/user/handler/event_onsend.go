package handler

import (
	"context"
	"errors"

	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/service"
	"github.com/mushanyux/MSIM/internal/track"
	"github.com/mushanyux/MSIM/internal/types"
	"github.com/mushanyux/MSIM/internal/types/pluginproto"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/mushanyux/MSIM/pkg/trace"
	msproto "github.com/mushanyux/MSIMGoProto"
	"github.com/valyala/bytebufferpool"
	"go.uber.org/zap"
)

// onSend 收到发送消息
func (h *Handler) onSend(ctx *eventbus.UserContext) {
	for _, event := range ctx.Events {
		frameType := event.Frame.GetFrameType()
		switch frameType {
		case msproto.SEND:
			h.handleOnSend(event)
		case msproto.RECVACK:
			h.recvack(event)
		case msproto.PING:
			h.ping(event)
		}
	}
}

func (h *Handler) handleOnSend(event *eventbus.Event) {
	// 记录消息路径
	event.Track.Record(track.PositionUserOnSend)

	conn := event.Conn

	sendPacket := event.Frame.(*msproto.SendPacket)
	channelId := sendPacket.ChannelID
	channelType := sendPacket.ChannelType
	fakeChannelId := channelId
	if channelType == msproto.ChannelTypePerson {
		fakeChannelId = options.GetFakeChannelIDWith(channelId, conn.Uid)
	}

	if options.G.Logger.TraceOn {
		h.Trace("用户发送消息...",
			"onSend",
			zap.Int64("messageId", event.MessageId),
			zap.Uint64("messageSeq", event.MessageSeq),
			zap.String("from", event.Conn.Uid),
			zap.String("deviceId", event.Conn.DeviceId),
			zap.String("deviceFlag", event.Conn.DeviceFlag.String()),
			zap.Int64("connId", event.Conn.ConnId),
			zap.String("channelId", fakeChannelId),
			zap.Uint8("channelType", channelType),
		)
	}

	// 根据配置决定是否解密消息
	if !options.G.DisableEncryption && !conn.IsJsonRpc {
		newPayload, err := h.decryptPayload(sendPacket, conn)
		if err != nil {
			h.Error("handleOnSend: Failed to decrypt payload！", zap.Error(err), zap.String("uid", conn.Uid), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
			sendack := &msproto.SendackPacket{
				Framer:      sendPacket.Framer,
				MessageID:   event.MessageId,
				ClientSeq:   sendPacket.ClientSeq,
				ClientMsgNo: sendPacket.ClientMsgNo,
				ReasonCode:  msproto.ReasonPayloadDecodeError,
			}
			eventbus.User.ConnWrite(event.ReqId, conn, sendack)
			return
		}
		sendPacket.Payload = newPayload // 使用解密后的 Payload
	} else {
		// 如果禁用了加密，则直接使用原始 Payload，不做任何操作
		// sendPacket.Payload 保持不变
	}

	// 调用插件
	reason, err := h.pluginInvokeSend(sendPacket, event)
	if err != nil || reason != msproto.ReasonSuccess {
		h.Info("handleOnSend: plugin return error reason", zap.Error(err), zap.Uint8("reason", uint8(reason)), zap.String("uid", conn.Uid), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		sendack := &msproto.SendackPacket{
			Framer:      sendPacket.Framer,
			MessageID:   event.MessageId,
			ClientSeq:   sendPacket.ClientSeq,
			ClientMsgNo: sendPacket.ClientMsgNo,
			ReasonCode:  reason,
		}
		eventbus.User.ConnWrite(event.ReqId, conn, sendack)
		return
	}

	trace.GlobalTrace.Metrics.App().SendPacketCountAdd(1)
	trace.GlobalTrace.Metrics.App().SendPacketBytesAdd(sendPacket.GetFrameSize())
	// 添加消息到频道
	eventbus.Channel.SendMessage(fakeChannelId, channelType, &eventbus.Event{
		Type:      eventbus.EventChannelOnSend,
		Conn:      conn,
		Frame:     sendPacket,
		MessageId: event.MessageId,
		Track:     event.Track,
		ReqId:     event.ReqId,
	})
	// 推进
	eventbus.Channel.Advance(fakeChannelId, channelType)
}

func (h *Handler) pluginInvokeSend(sendPacket *msproto.SendPacket, event *eventbus.Event) (msproto.ReasonCode, error) {
	plugins := service.PluginManager.Plugins(types.PluginSend)

	if len(plugins) == 0 {
		return msproto.ReasonSuccess, nil
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), options.G.Plugin.Timeout)
	defer cancel()

	conn := &pluginproto.Conn{
		Uid:         event.Conn.Uid,
		ConnId:      event.Conn.ConnId,
		DeviceId:    event.Conn.DeviceId,
		DeviceFlag:  uint32(event.Conn.DeviceFlag),
		DeviceLevel: uint32(event.Conn.DeviceLevel),
	}

	pluginPacket := &pluginproto.SendPacket{
		FromUid:     event.Conn.Uid,
		ChannelId:   sendPacket.ChannelID,
		ChannelType: uint32(sendPacket.ChannelType),
		Payload:     sendPacket.Payload,
		Conn:        conn,
		Reason:      uint32(msproto.ReasonSuccess),
	}
	for _, pg := range plugins {
		result, err := pg.Send(timeoutCtx, pluginPacket)
		if err != nil {
			h.Error("pluginInvokeSend: Failed to invoke plugin！", zap.Error(err), zap.String("plugin", pg.GetNo()))
			return msproto.ReasonSystemError, err
		}
		if result == nil {
			continue
		}
		if result.Reason == 0 {
			result.Reason = uint32(msproto.ReasonSuccess)
		}
		pluginPacket = result
	}

	// 使用插件处理后的消息
	sendPacket.Payload = pluginPacket.Payload
	return msproto.ReasonCode(pluginPacket.Reason), nil
}

// decode payload
func (h *Handler) decryptPayload(sendPacket *msproto.SendPacket, conn *eventbus.Conn) ([]byte, error) {
	aesKey, aesIV := conn.AesKey, conn.AesIV
	vail, err := h.sendPacketIsVail(sendPacket, conn)
	if err != nil {
		return nil, err
	}
	if !vail {
		return nil, errors.New("sendPacket is illegal！")
	}
	// decode payload
	decodePayload, err := msutil.AesDecryptPkcs7Base64(sendPacket.Payload, aesKey, aesIV)
	if err != nil {
		h.Error("Failed to decode payload！", zap.Error(err))
		return nil, err
	}

	return decodePayload, nil
}

// send packet is vail
func (h *Handler) sendPacketIsVail(sendPacket *msproto.SendPacket, conn *eventbus.Conn) (bool, error) {
	aesKey, aesIV := conn.AesKey, conn.AesIV
	signStr := sendPacket.VerityString()

	signBuff := bytebufferpool.Get()
	_, _ = signBuff.WriteString(signStr)

	defer bytebufferpool.Put(signBuff)

	actMsgKey, err := msutil.AesEncryptPkcs7Base64(signBuff.Bytes(), aesKey, aesIV)
	if err != nil {
		h.Error("msgKey is illegal！", zap.Error(err), zap.String("sign", signStr), zap.String("aesKey", string(aesKey)), zap.String("aesIV", string(aesIV)), zap.Any("conn", conn))
		return false, err
	}
	actMsgKeyStr := sendPacket.MsgKey
	exceptMsgKey := msutil.MD5Bytes(actMsgKey)
	if actMsgKeyStr != exceptMsgKey {
		h.Error("msgKey is illegal！", zap.String("except", exceptMsgKey), zap.String("act", actMsgKeyStr), zap.String("sign", signStr), zap.String("aesKey", string(aesKey)), zap.String("aesIV", string(aesIV)), zap.Any("conn", conn))
		return false, errors.New("msgKey is illegal！")
	}
	return true, nil
}
