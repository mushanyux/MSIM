package icluster

import "github.com/mushanyux/MSIM/pkg/cluster/node/types"

type Node interface {

	// AllowVoteAndJoinedNodes 允许投票并且已加入的节点
	AllowVoteAndJoinedNodes() []*types.Node

	// 槽数量
	SlotCount() uint32

	// Slots 获取所有槽位
	Slots() []*types.Slot
}
