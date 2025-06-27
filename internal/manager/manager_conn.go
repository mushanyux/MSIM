package manager

import (
	"sync"

	"github.com/mushanyux/MSIM/pkg/msnet"
)

type ConnManager struct {
	bluckets []*connBlucket
	engine   *msnet.Engine // 长连接引擎
}

func NewConnManager(blucketCount int, engine *msnet.Engine) *ConnManager {
	connBluckets := make([]*connBlucket, blucketCount)
	for i := 0; i < blucketCount; i++ {
		connBluckets[i] = &connBlucket{
			connMap: make(map[int64]msnet.Conn),
		}
	}
	return &ConnManager{
		bluckets: connBluckets,
		engine:   engine,
	}
}

func (m *ConnManager) AddConn(conn msnet.Conn) {
	m.blucket(conn.ID()).addConn(conn)
}

func (m *ConnManager) RemoveConn(conn msnet.Conn) {
	m.blucket(conn.ID()).removeConn(conn)
}

func (m *ConnManager) GetConn(connID int64) msnet.Conn {
	return m.blucket(connID).getConn(connID)
}

func (m *ConnManager) GetConnByFd(fd int) msnet.Conn {
	conn := m.engine.GetConn(fd)
	return conn
}

func (m *ConnManager) blucket(connId int64) *connBlucket {
	blucketIndex := connId % int64(len(m.bluckets))
	return m.bluckets[blucketIndex]
}

func (m *ConnManager) ConnCount() int {
	connCount := 0
	for _, b := range m.bluckets {
		connCount += b.connCount()
	}
	return connCount
}

func (m *ConnManager) GetAllConn() []msnet.Conn {
	var conns []msnet.Conn
	for _, b := range m.bluckets {
		b.RLock()
		for _, conn := range b.connMap {
			conns = append(conns, conn)
		}
		b.RUnlock()
	}
	return conns
}

type connBlucket struct {
	connMap map[int64]msnet.Conn
	sync.RWMutex
}

func (c *connBlucket) getConn(connId int64) msnet.Conn {
	c.RLock()
	defer c.RUnlock()
	return c.connMap[connId]
}

func (c *connBlucket) addConn(conn msnet.Conn) {
	c.Lock()
	defer c.Unlock()
	c.connMap[conn.ID()] = conn
}

func (c *connBlucket) removeConn(conn msnet.Conn) {
	c.Lock()
	defer c.Unlock()
	delete(c.connMap, conn.ID())
}

func (c *connBlucket) connCount() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.connMap)
}
