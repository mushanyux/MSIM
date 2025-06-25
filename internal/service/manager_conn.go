package service

import "github.com/mushanyux/MSIM/pkg/msnet"

var ConnManager IConnManager

type IConnManager interface {
	// AddConn 添加连接
	AddConn(conn msnet.Conn)
	// RemoveConn 移除连接
	RemoveConn(conn msnet.Conn)
	// GetConn 获取连接
	GetConn(connID int64) msnet.Conn

	// GetConnByFd 根据fd获取连接
	GetConnByFd(fd int) msnet.Conn
	// ConnCount 获取连接数量
	ConnCount() int

	GetAllConn() []msnet.Conn
}
