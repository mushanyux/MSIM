package slot

import (
	"github.com/mushanyux/MSIM/pkg/cluster/node/types"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/raft/raft"
	"go.uber.org/zap"
)

type Slot struct {
	*raft.Node
	slot    *types.Slot
	shardNo string
	mslog.Log
}

func newSlot(slot *types.Slot, s *Server) *Slot {
	shardNo := SlotIdToKey(slot.Id)
	st := &Slot{
		slot:    slot.Clone(),
		shardNo: shardNo,
		Log:     mslog.NewMSLog("slot"),
	}
	state, err := s.storage.GetState(shardNo)
	if err != nil {
		st.Panic("get state failed", zap.Error(err))
	}
	lastLogIndex, err := s.storage.GetTermStartIndex(shardNo, state.LastTerm)
	if err != nil {
		st.Panic("get last term failed", zap.Error(err))
	}
	node := raft.NewNode(lastLogIndex, state, raft.NewOptions(raft.WithKey(shardNo), raft.WithNodeId(s.opts.NodeId)))
	st.Node = node

	return st
}

// needUpdate 判断是否需要更新
func (s *Slot) needUpdate(newSlot *types.Slot) bool {

	return !s.slot.Equal(newSlot)
}

func (s *Slot) LastLogIndexAndTerm() (uint64, uint32) {
	return s.LastLogIndex(), s.LastLogTerm()
}
