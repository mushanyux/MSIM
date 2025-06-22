package icluster

import "github.com/mushanyux/MSIM/pkg/msdb"

type IStore interface {
	// SaveChannelClusterConfig 保存频道分布式配置
	SaveChannelClusterConfig(cfg msdb.ChannelClusterConfig) error
	// 获取mushanimdb对象
	MSDB() msdb.DB
}
