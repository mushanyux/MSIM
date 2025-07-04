package event

import (
	"fmt"
	"hash/fnv"

	"github.com/mushanyux/MSIM/internal/common"
	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msnet"
	msproto "github.com/mushanyux/MSIMGoProto"
	"go.uber.org/zap"
)

type EventPool struct {
	pollers []*poller
	mslog.Log
	handler eventbus.UserEventHandler
}

func NewEventPool(handler eventbus.UserEventHandler) *EventPool {
	u := &EventPool{
		handler: handler,
		Log:     mslog.NewMSLog("EventPool"),
	}
	for i := 0; i < options.G.Poller.UserCount; i++ {
		p := newPoller(i, u)
		u.pollers = append(u.pollers, p)
	}
	return u
}

// Start 启动
func (e *EventPool) Start() error {
	for _, p := range e.pollers {
		err := p.start()
		if err != nil {
			e.Error("start poller failed", zap.Error(err))
			return err
		}
	}
	return nil
}

// Stop 停止
func (e *EventPool) Stop() {
	for _, p := range e.pollers {
		p.stop()
	}
}

// AddEvent 添加事件
func (e *EventPool) AddEvent(uid string, event *eventbus.Event) {
	p := e.pollerByUid(uid)
	p.addEvent(uid, event)
}

// Advance 推进事件
func (e *EventPool) Advance(uid string) {
	p := e.pollerByUid(uid)
	p.advance()
}

func (e *EventPool) pollerByUid(uid string) *poller {
	h := fnv.New32a()
	h.Write([]byte(uid))
	i := h.Sum32() % uint32(len(e.pollers))
	return e.pollers[i]
}

// 查询连接信息
func (e *EventPool) ConnsByUid(uid string) []*eventbus.Conn {
	return e.pollerByUid(uid).connsByUid(uid)
}
func (e *EventPool) AuthedConnsByUid(uid string) []*eventbus.Conn {
	return e.pollerByUid(uid).authedConnsByUid(uid)
}
func (e *EventPool) ConnCountByUid(uid string) int {
	return e.pollerByUid(uid).connCountByUid(uid)
}
func (e *EventPool) ConnsByDeviceFlag(uid string, deviceFlag msproto.DeviceFlag) []*eventbus.Conn {
	return e.pollerByUid(uid).connsByDeviceFlag(uid, deviceFlag)
}
func (e *EventPool) ConnCountByDeviceFlag(uid string, deviceFlag msproto.DeviceFlag) int {
	return e.pollerByUid(uid).connCountByDeviceFlag(uid, deviceFlag)
}
func (e *EventPool) ConnById(uid string, nodeId uint64, id int64) *eventbus.Conn {
	return e.pollerByUid(uid).connById(uid, nodeId, id)
}
func (e *EventPool) LocalConnById(uid string, id int64) *eventbus.Conn {
	return e.pollerByUid(uid).localConnById(uid, id)
}
func (e *EventPool) LocalConnByUid(uid string) []*eventbus.Conn {
	return e.pollerByUid(uid).localConnByUid(uid)
}

func (e *EventPool) AllConn() []*eventbus.Conn {
	var conns []*eventbus.Conn
	for _, p := range e.pollers {
		conns = append(conns, p.allConn()...)
	}
	return conns
}

func (e *EventPool) UpdateConn(conn *eventbus.Conn) {
	e.pollerByUid(conn.Uid).updateConn(conn)
}

func (e *EventPool) AllUserCount() int {
	count := 0
	for _, p := range e.pollers {
		count += p.allUserCount()
	}
	return count
}
func (e *EventPool) AllConnCount() int {
	count := 0
	for _, p := range e.pollers {
		count += p.allConnCount()
	}
	return count
}

func (e *EventPool) RemoveConn(conn *eventbus.Conn) {
	e.pollerByUid(conn.Uid).removeConn(conn)
}

func (e *EventPool) WriteLocalData(conn *eventbus.Conn, data []byte) error {

	if !options.G.IsLocalNode(conn.NodeId) {
		e.Error("writeLocalData: conn not local node", zap.String("uid", conn.Uid), zap.Uint64("nodeId", conn.NodeId), zap.Int64("connId", conn.ConnId))
		return fmt.Errorf("writeLocalData: conn not local node")
	}

	realConn, err := common.CheckConnValidAndGetRealConn(conn)
	if err != nil {
		e.Warn("WriteLocalData: conn invaild", zap.Error(err), zap.String("uid", conn.Uid), zap.String("deviceId", conn.DeviceId), zap.Int64("connId", conn.ConnId))
		return err
	}
	if realConn == nil {
		e.Error("writeLocalData: conn not exist", zap.String("uid", conn.Uid), zap.Uint64("nodeId", conn.NodeId), zap.Int64("connId", conn.ConnId))
		return fmt.Errorf("writeLocalData: conn not exist")
	}
	wsConn, wsok := realConn.(msnet.IWSConn) // websocket连接
	if wsok {
		if conn.IsJsonRpc {
			err = wsConn.WriteServerText(data)
		} else {
			err = wsConn.WriteServerBinary(data)
		}
		if err != nil {
			e.Warn("writeFrame: Failed to ws write the message", zap.Error(err))
		}
	} else {
		_, err := realConn.WriteToOutboundBuffer(data)
		if err != nil {
			e.Warn("writeFrame: Failed to write the message", zap.Error(err))
			return err
		}
	}
	return realConn.WakeWrite()
}
