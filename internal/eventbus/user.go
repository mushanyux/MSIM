package eventbus

import (
	"github.com/mushanyux/MSIM/internal/options"
	msproto "github.com/mushanyux/MSIMGoProto"
)

// RegisterUser 注册用户事件
func RegisterUser(user IUser) {
	User = newUserPlus(user)
}

var User *userPlus

type IUser interface {
	// AddEvent 添加事件
	AddEvent(uid string, event *Event)
	// Advance 推进事件（让事件池不需要等待直接执行下一轮事件）
	Advance(uid string)
	// 查询连接信息
	ConnsByUid(uid string) []*Conn
	// AuthedConnsByUid 查询已认证的连接
	AuthedConnsByUid(uid string) []*Conn
	// ConnCountByUid 查询连接数量
	ConnCountByUid(uid string) int
	// ConnsByDeviceFlag 根据设备标识获取连接
	ConnsByDeviceFlag(uid string, deviceFlag msproto.DeviceFlag) []*Conn
	// ConnCountByDeviceFlag 根据设备标识获取连接数量
	ConnCountByDeviceFlag(uid string, deviceFlag msproto.DeviceFlag) int
	// ConnById 获取指定节点和连接id的连接
	ConnById(uid string, fromNode uint64, id int64) *Conn
	// LocalConnById 获取本地连接
	LocalConnById(uid string, id int64) *Conn
	// LocalConnByUid 获取本地连接
	LocalConnByUid(uid string) []*Conn

	// RemoveConn 移除连接
	RemoveConn(conn *Conn)

	// UpdateConn 更新连接
	UpdateConn(conn *Conn)
	// AllUserCount 所有用户数量
	AllUserCount() int
	// AllConnCount 所有连接数量
	AllConnCount() int
	// AllConn 获取所有连接
	AllConn() []*Conn

	// WriteLocalData 写入本地数据(conn 是本地连接)
	WriteLocalData(conn *Conn, data []byte) error
}

type userPlus struct {
	user IUser
}

func newUserPlus(user IUser) *userPlus {
	return &userPlus{
		user: user,
	}
}

func (u *userPlus) AddEvent(uid string, event *Event) {
	u.user.AddEvent(uid, event)
}

func (u *userPlus) AddEvents(uid string, events []*Event) {
	for _, event := range events {
		u.user.AddEvent(uid, event)
	}
	u.user.Advance(uid)
}

func (u *userPlus) Advance(uid string) {
	u.user.Advance(uid)
}

// ========================================== conn ==========================================
// Connect 请求连接
func (u *userPlus) Connect(reqId string, conn *Conn, connectPacket *msproto.ConnectPacket) {
	u.user.AddEvent(conn.Uid, &Event{
		Type:         EventConnect,
		Frame:        connectPacket,
		Conn:         conn,
		SourceNodeId: options.G.Cluster.NodeId,
		ReqId:        reqId,
	})
}

// ConnsByDeviceFlag 根据设备标识获取连接
func (u *userPlus) ConnsByDeviceFlag(uid string, deviceFlag msproto.DeviceFlag) []*Conn {
	return u.user.ConnsByDeviceFlag(uid, deviceFlag)
}

func (u *userPlus) ConnCountByDeviceFlag(uid string, deviceFlag msproto.DeviceFlag) int {
	return u.user.ConnCountByDeviceFlag(uid, deviceFlag)
}

// ConnsByUid 根据用户uid获取连接
func (u *userPlus) ConnsByUid(uid string) []*Conn {
	return u.user.ConnsByUid(uid)
}

// AuthedConnsByUid 根据用户uid获取已认证的连接
func (u *userPlus) AuthedConnsByUid(uid string) []*Conn {
	return u.user.AuthedConnsByUid(uid)
}

func (u *userPlus) ConnCountByUid(uid string) int {
	return u.user.ConnCountByUid(uid)
}

// LocalConnById 获取本地连接
func (u *userPlus) LocalConnById(uid string, id int64) *Conn {
	return u.user.LocalConnById(uid, id)
}

// LocalConnByUid 获取本地连接
func (u *userPlus) LocalConnByUid(uid string) []*Conn {
	return u.user.LocalConnByUid(uid)
}

// ConnById 获取连接
func (u *userPlus) ConnById(uid string, fromNode uint64, id int64) *Conn {
	return u.user.ConnById(uid, fromNode, id)
}

// UpdateConn 更新连接
func (u *userPlus) UpdateConn(conn *Conn) {
	u.user.UpdateConn(conn)
}

// ConnWrite 连接写包
func (u *userPlus) ConnWrite(reqId string, conn *Conn, frame msproto.Frame) {
	u.user.AddEvent(conn.Uid, &Event{
		Type:         EventConnWriteFrame,
		Conn:         conn,
		Frame:        frame,
		SourceNodeId: options.G.Cluster.NodeId,
		ReqId:        reqId,
	})
}

// CloseConn 关闭连接
func (u *userPlus) CloseConn(conn *Conn) {
	u.user.AddEvent(conn.Uid, &Event{
		Type:         EventConnClose,
		Conn:         conn,
		SourceNodeId: options.G.Cluster.NodeId,
	})
}

// DirectRemoveConn 移除连接 (直接移除)
func (u *userPlus) DirectRemoveConn(conn *Conn) {
	u.user.RemoveConn(conn)
}

// RemoveConn 移除连接（触发移除事件）
func (u *userPlus) RemoveConn(conn *Conn) {
	u.user.AddEvent(conn.Uid, &Event{
		Type:         EventConnRemove,
		Conn:         conn,
		SourceNodeId: options.G.Cluster.NodeId,
	})
}

// RemoveLeaderConn 移除leader节点中的连接
func (u *userPlus) RemoveLeaderConn(conn *Conn) {
	u.user.AddEvent(conn.Uid, &Event{
		Type:         EventConnLeaderRemove,
		Conn:         conn,
		SourceNodeId: options.G.Cluster.NodeId,
	})
}

func (u *userPlus) AllUserCount() int {
	return u.user.AllUserCount()
}

func (u *userPlus) AllConnCount() int {
	return u.user.AllConnCount()
}

func (u *userPlus) AllConn() []*Conn {
	return u.user.AllConn()
}

func (u *userPlus) WriteLocalData(conn *Conn, data []byte) error {
	return u.user.WriteLocalData(conn, data)
}
