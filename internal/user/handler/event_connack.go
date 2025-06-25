package handler

import (
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/service"
	msproto "github.com/mushanyux/MSIMGoProto"
	"go.uber.org/zap"
)

func (h *Handler) connack(ctx *eventbus.UserContext) {
	for _, event := range ctx.Events {
		conn := event.Conn
		frame := event.Frame
		uid := conn.Uid
		if conn.NodeId == 0 {
			h.Error("processConnack: from node is 0", zap.String("uid", uid))
			return
		}
		if frame == nil {
			h.Error("processConnack: frame is nil", zap.String("uid", uid))
			return
		}
		connack := frame.(*msproto.ConnackPacket)
		if connack.ReasonCode == msproto.ReasonSuccess {
			// 设置连接最大空闲时间
			if options.G.IsLocalNode(conn.NodeId) {
				realConn := service.ConnManager.GetConn(conn.ConnId)
				if realConn != nil {
					realConn.SetMaxIdle(options.G.ConnIdleTime)
					realConn.SetContext(conn)
				}
			}
			connack.NodeId = options.G.Cluster.NodeId
			// 更新连接
			eventbus.User.UpdateConn(conn)
		}
		eventbus.User.ConnWrite(event.ReqId, conn, connack)
	}

}
