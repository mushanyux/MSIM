package handler

import (
	"github.com/mushanyux/MSIM/pkg/jsonrpc"

	"github.com/mushanyux/MSIM/internal/common"
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/track"
	"github.com/mushanyux/MSIM/pkg/msnet"
	"go.uber.org/zap"
)

func (h *Handler) writeFrame(ctx *eventbus.UserContext) {
	for _, event := range ctx.Events {
		conn := event.Conn
		frame := event.Frame
		if conn == nil || frame == nil {
			h.Error("write frame conn or frame is nil")
			continue
		}
		if conn.NodeId == 0 {
			h.Error("writeFrame: conn node id is 0")
			continue
		}
		// 如果不是本地节点，则转发写请求
		if !options.G.IsLocalNode(conn.NodeId) {
			// 统计
			h.totalOut(conn, frame)
			h.forwardsToNode(conn.NodeId, conn.Uid, []*eventbus.Event{event})
			continue
		}
		// 本地节点写请求
		h.writeLocalFrame(event)
	}
}

func (h *Handler) writeLocalFrame(event *eventbus.Event) {
	conn := event.Conn
	frame := event.Frame
	var (
		data []byte
		err  error
	)
	if conn.IsJsonRpc {
		req, err := jsonrpc.FromFrame(event.ReqId, frame)
		if err != nil {
			h.Error("writeFrame jsonrpc: from frame err", zap.Error(err))
			return
		}
		data, err = jsonrpc.Encode(req)
		if err != nil {
			h.Error("writeFrame jsonrpc: encode err", zap.Error(err))
			return
		}
	} else {
		data, err = eventbus.Proto.EncodeFrame(frame, conn.ProtoVersion)
		if err != nil {
			h.Error("writeFrame: encode frame err", zap.Error(err))
		}
	}

	// 统计
	h.totalOut(conn, frame)
	// 记录消息路径
	event.Track.Record(track.PositionConnWrite)

	// 获取到真实连接
	realConn, err := common.CheckConnValidAndGetRealConn(conn)
	if err != nil {
		h.Warn("writeFrame: conn invalid", zap.Error(err), zap.String("uid", conn.Uid), zap.String("deviceId", conn.DeviceId), zap.Int64("connId", conn.ConnId))
		return
	}
	if realConn == nil {
		h.Info("writeFrame: conn not exist", zap.String("uid", conn.Uid), zap.Uint64("nodeId", conn.NodeId), zap.Int64("connId", conn.ConnId), zap.Uint64("sourceNodeId", event.SourceNodeId))
		// 如果连接不存在了，并且写入事件是其他节点发起的，说明其他节点还不知道连接已经关闭，需要通知其他节点关闭连接
		if event.SourceNodeId != 0 && !options.G.IsLocalNode(event.SourceNodeId) {
			h.forwardToNode(event.SourceNodeId, conn.Uid, &eventbus.Event{
				Type:         eventbus.EventConnRemove,
				Conn:         conn,
				SourceNodeId: options.G.Cluster.NodeId,
			})
		} else if event.SourceNodeId == 0 || options.G.IsLocalNode(event.SourceNodeId) { // 如果是本节点事件，直接删除连接
			eventbus.User.DirectRemoveConn(conn)
		}
		return
	}

	// 开始写入数据
	wsConn, wsok := realConn.(msnet.IWSConn) // websocket连接
	if wsok {
		if conn.IsJsonRpc {
			err = wsConn.WriteServerText(data)
		} else {
			err = wsConn.WriteServerBinary(data)
		}
		if err != nil {
			h.Warn("writeFrame: Failed to ws write the message", zap.Error(err))
		}
	} else {
		_, err := realConn.WriteToOutboundBuffer(data)
		if err != nil {
			h.Warn("writeFrame: Failed to write the message", zap.Error(err))
		}
	}
	_ = realConn.WakeWrite()
}
