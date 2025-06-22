package store

import (
	"github.com/mushanyux/MSIM/pkg/cluster/icluster"
	"github.com/mushanyux/MSIM/pkg/msdb"
)

type Options struct {
	NodeId uint64 // 节点ID

	Slot icluster.Slot

	DB msdb.DB

	Channel icluster.Channel

	IsCmdChannel func(channel string) bool
}

func NewOptions(opt ...Option) *Options {
	opts := &Options{}
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

func WithChannel(channel icluster.Channel) Option {
	return func(o *Options) {
		o.Channel = channel
	}
}

func WithDB(db msdb.DB) Option {
	return func(o *Options) {
		o.DB = db
	}
}

func WithIsCmdChannel(isCmdChannel func(channel string) bool) Option {
	return func(o *Options) {
		o.IsCmdChannel = isCmdChannel
	}
}
