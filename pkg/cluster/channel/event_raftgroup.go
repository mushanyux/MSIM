package channel

import (
	"github.com/mushanyux/MSIM/pkg/raft/raftgroup"
	"github.com/mushanyux/MSIM/pkg/trace"
)

// OnAddRaft 添加raft
func (s *Server) OnAddRaft(r raftgroup.IRaft) {
	if trace.GlobalTrace != nil {
		trace.GlobalTrace.Metrics.Cluster().ChannelActiveCountAdd(1)
	}
}

// OnRemoveRaft 移除raft
func (s *Server) OnRemoveRaft(r raftgroup.IRaft) {
	if trace.GlobalTrace != nil {
		trace.GlobalTrace.Metrics.Cluster().ChannelActiveCountAdd(-1)
	}
}
