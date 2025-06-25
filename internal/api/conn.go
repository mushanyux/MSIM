package api

import (
	"errors"
	"fmt"
	"time"

	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/service"
	"github.com/mushanyux/MSIM/pkg/mshttp"
	"github.com/mushanyux/MSIM/pkg/mslog"
	msproto "github.com/mushanyux/MSIMGoProto"
	"go.uber.org/zap"
)

type connApi struct {
	mslog.Log
	s *Server
}

func newConnApi(s *Server) *connApi {
	return &connApi{
		Log: mslog.NewMSLog("connApi"),
		s:   s,
	}
}

// 路由配置
func (cn *connApi) route(r *mshttp.MSHttp) {
	// 移除连接
	r.POST("/conn/remove", cn.remove)

	// 踢掉
	r.POST("/conn/kick", cn.kick)
}

// 移除连接

func (cn *connApi) remove(c *mshttp.Context) {
	var req struct {
		Uid      string `json:"uid"`        // 用户id
		ConnID   int64  `json:"conn_id"`    // 连接id
		NodeId   uint64 `json:"node_id"`    // 连接所在节点id
		OpNodeId uint64 `json:"op_node_id"` // 操作节点id
	}
	bodyBytes, err := BindJSON(&req, c)
	if err != nil {
		cn.Error("remove conn bind json error", zap.Error(err))
		c.ResponseError(err)
		return
	}

	// 不属于本节点则转发给对应节点
	if req.OpNodeId != 0 && !options.G.IsLocalNode(req.OpNodeId) {
		nodeInfo := service.Cluster.NodeInfoById(req.OpNodeId)
		if nodeInfo == nil {
			cn.Error("remove conn node not found", zap.Uint64("node_id", req.OpNodeId))
			c.ResponseError(errors.New("node not found"))
			return
		}
		c.ForwardWithBody(fmt.Sprintf("%s%s", nodeInfo.ApiServerAddr, c.Request.URL.Path), bodyBytes)
		return
	}

	if req.ConnID == 0 {
		cn.Error("remove conn conn_id is 0")
		c.ResponseError(errors.New("conn_id is 0"))
		return
	}

	if req.NodeId == 0 {
		req.NodeId = options.G.Cluster.NodeId
	}

	conn := eventbus.User.ConnById(req.Uid, req.NodeId, req.ConnID)
	if conn != nil {
		eventbus.User.CloseConn(conn)
	}
	c.ResponseOK()
}

func (cn *connApi) kick(c *mshttp.Context) {
	var req struct {
		Uid      string `json:"uid"`        // 用户id
		ConnID   int64  `json:"conn_id"`    // 连接id
		NodeId   uint64 `json:"node_id"`    // 连接所在节点id
		OpNodeId uint64 `json:"op_node_id"` // 操作节点id
	}
	bodyBytes, err := BindJSON(&req, c)
	if err != nil {
		cn.Error("kick conn bind json error", zap.Error(err))
		c.ResponseError(err)
		return
	}

	// 不属于本节点则转发给对应节点
	if req.OpNodeId != 0 && !options.G.IsLocalNode(req.OpNodeId) {
		nodeInfo := service.Cluster.NodeInfoById(req.OpNodeId)
		if nodeInfo == nil {
			cn.Error("kick conn node not found", zap.Uint64("node_id", req.OpNodeId))
			c.ResponseError(errors.New("node not found"))
			return
		}
		c.ForwardWithBody(fmt.Sprintf("%s%s", nodeInfo.ApiServerAddr, c.Request.URL.Path), bodyBytes)
		return
	}

	if req.ConnID == 0 {
		cn.Error("kick conn conn_id is 0")
		c.ResponseError(errors.New("conn_id is 0"))
		return
	}

	if req.NodeId == 0 {
		req.NodeId = options.G.Cluster.NodeId
	}

	conn := eventbus.User.ConnById(req.Uid, req.NodeId, req.ConnID)
	if conn != nil {
		eventbus.User.ConnWrite("", conn, &msproto.DisconnectPacket{
			ReasonCode: msproto.ReasonConnectKick,
			Reason:     "server kick",
		})
		service.CommonService.AfterFunc(time.Second*2, func(od *eventbus.Conn) func() {
			return func() {
				eventbus.User.CloseConn(od)
			}
		}(conn))
	}
	c.ResponseOK()
}
