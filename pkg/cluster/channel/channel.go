package channel

import (
	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/mushanyux/MSIM/pkg/raft/raft"
	"github.com/mushanyux/MSIM/pkg/raft/raftgroup"
	"github.com/mushanyux/MSIM/pkg/raft/types"
	rafttype "github.com/mushanyux/MSIM/pkg/raft/types"
	"go.uber.org/zap"
)

type Channel struct {
	*raft.Node
	// 分布式配置
	cfg msdb.ChannelClusterConfig
	s   *Server
	mslog.Log
	rg         *raftgroup.RaftGroup
	channelKey string
}

func createChannel(cfg msdb.ChannelClusterConfig, s *Server, rg *raftgroup.RaftGroup) (*Channel, error) {
	channelKey := msutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	ch := &Channel{
		cfg:        cfg,
		s:          s,
		Log:        mslog.NewMSLog("channel"),
		rg:         rg,
		channelKey: channelKey,
	}

	state, err := s.storage.GetState(cfg.ChannelId, cfg.ChannelType)
	if err != nil {
		ch.Error("get state failed", zap.String("channelKey", channelKey), zap.Error(err))
		return nil, err
	}

	lastLogStartIndex, err := s.storage.GetTermStartIndex(channelKey, state.LastTerm)
	if err != nil {
		ch.Error("get last term failed", zap.String("channelKey", channelKey), zap.Error(err))
		return nil, err
	}

	ch.Node = raft.NewNode(
		lastLogStartIndex,
		state,
		raft.NewOptions(
			raft.WithKey(channelKey),
			raft.WithAutoSuspend(true),
			raft.WithAutoDestory(true),
			raft.WithNodeId(s.opts.NodeId),
			raft.WithDestoryAfterIdleTick(s.opts.DestoryAfterIdleTick),
		))

	return ch, nil
}

func (ch *Channel) switchConfig(cfg rafttype.Config) error {

	return ch.rg.AddEventWait(ch.channelKey, rafttype.Event{
		Type:   rafttype.ConfChange,
		Config: cfg,
	})

}

// needUpdate 判断是否需要更新
func (ch *Channel) needUpdate(newCfg msdb.ChannelClusterConfig) bool {
	return !ch.cfg.Equal(newCfg)
}

func channelConfigToRaftConfig(currentNodeId uint64, cfg msdb.ChannelClusterConfig) rafttype.Config {

	var role rafttype.Role
	if msutil.ArrayContainsUint64(cfg.Learners, currentNodeId) {
		role = rafttype.RoleLearner
	} else {
		if cfg.LeaderId == currentNodeId {
			role = rafttype.RoleLeader
		} else {
			role = rafttype.RoleFollower
		}
	}

	return types.Config{
		MigrateFrom: cfg.MigrateFrom,
		MigrateTo:   cfg.MigrateTo,
		Replicas:    cfg.Replicas,
		Learners:    cfg.Learners,
		Term:        cfg.Term,
		Leader:      cfg.LeaderId,
		Role:        role,
		Version:     cfg.ConfVersion,
	}
}
