package channel

import (
	"github.com/mushanyux/MSIM/pkg/cluster/icluster"
	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/raft/raftgroup"
	"github.com/mushanyux/MSIM/pkg/raft/types"
)

type Options struct {
	// 节点ID
	NodeId uint64
	// slot的接口
	Slot icluster.Slot
	// 节点接口
	Node icluster.Node
	// 存储
	DB msdb.DB
	// 分布式接口
	Cluster icluster.ICluster
	// api接口
	RPC icluster.RPC

	// raft group 的数量
	GroupCount int
	// 传输层
	Transport raftgroup.ITransport

	//频道最大副本数量
	ChannelMaxReplicaCount uint32

	// OnSaveConfig 保存频道配置
	OnSaveConfig func(channelId string, channelType uint8, cfg types.Config) error

	//DestoryAfterIdleTick 频道空闲多久后销毁（如果TickInterval是100ms, 那么10 * 60 * 30这个值是30分钟，具体时间根据TickInterval来定）
	DestoryAfterIdleTick int
}

func NewOptions(opt ...Option) *Options {
	opts := &Options{
		GroupCount:             100,
		ChannelMaxReplicaCount: 3,
		DestoryAfterIdleTick:   10 * 60 * 30, // 大约30分钟，如果raft的TickInterval是100ms
	}
	for _, o := range opt {
		o(opts)
	}
	return opts
}

type Option func(*Options)

func WithNodeId(nodeId uint64) Option {
	return func(o *Options) {
		o.NodeId = nodeId
	}
}

func WithSlot(slot icluster.Slot) Option {
	return func(o *Options) {
		o.Slot = slot
	}
}

func WithGroupCount(groupCount int) Option {
	return func(o *Options) {
		o.GroupCount = groupCount
	}
}

func WithTransport(transport raftgroup.ITransport) Option {
	return func(o *Options) {
		o.Transport = transport
	}
}

func WithDB(db msdb.DB) Option {
	return func(o *Options) {
		o.DB = db
	}
}

func WithChannelMaxReplicaCount(count uint32) Option {
	return func(o *Options) {
		o.ChannelMaxReplicaCount = count
	}
}

func WithNode(node icluster.Node) Option {
	return func(o *Options) {
		o.Node = node
	}
}

func WithCluster(cluster icluster.ICluster) Option {
	return func(o *Options) {
		o.Cluster = cluster
	}
}

func WithRPC(rpc icluster.RPC) Option {
	return func(o *Options) {
		o.RPC = rpc
	}
}

func WithOnSaveConfig(fn func(channelId string, channelType uint8, cfg types.Config) error) Option {
	return func(o *Options) {
		o.OnSaveConfig = fn
	}
}

func WithDestoryAfterIdleTick(tick int) Option {
	return func(o *Options) {
		o.DestoryAfterIdleTick = tick
	}
}
