package channel

import (
	"sync"

	"github.com/mushanyux/MSIM/pkg/fasthash"
	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/mushanyux/MSIM/pkg/raft/raftgroup"
	rafttype "github.com/mushanyux/MSIM/pkg/raft/types"
	"github.com/mushanyux/MSIM/pkg/ringlock"
	"go.uber.org/zap"
)

type Server struct {
	raftGroups []*raftgroup.RaftGroup
	opts       *Options
	storage    *storage
	mslog.Log

	// 正在唤醒的频道
	wake struct {
		sync.RWMutex
		channels map[string]bool
	}

	wakeLeaderLock *ringlock.RingLock
}

func NewServer(opts *Options) *Server {
	s := &Server{
		opts:           opts,
		Log:            mslog.NewMSLog("channel.Server"),
		wakeLeaderLock: ringlock.NewRingLock(1024),
	}
	s.storage = newStorage(opts.DB, s)
	for i := 0; i < opts.GroupCount; i++ {
		rg := raftgroup.New(
			raftgroup.NewOptions(
				raftgroup.WithLogPrefix("channel"),
				raftgroup.WithNotNeedApplied(true),
				raftgroup.WithTransport(opts.Transport),
				raftgroup.WithStorage(s.storage),
				raftgroup.WithEvent(s)),
		)
		s.raftGroups = append(s.raftGroups, rg)
	}
	s.wake.channels = make(map[string]bool)

	return s
}

func (s *Server) Start() error {

	for _, rg := range s.raftGroups {
		err := rg.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Stop() {
	for _, rg := range s.raftGroups {
		rg.Stop()
	}
}

// 唤醒频道领导
func (s *Server) WakeLeaderIfNeed(clusterConfig msdb.ChannelClusterConfig) error {

	s.wakeLeaderLock.Lock(clusterConfig.ChannelId)
	defer s.wakeLeaderLock.Unlock(clusterConfig.ChannelId)

	channelKey := msutil.ChannelToKey(clusterConfig.ChannelId, clusterConfig.ChannelType)
	rg := s.getRaftGroup(channelKey)

	raft := rg.GetRaft(channelKey)
	if raft != nil {
		ch := raft.(*Channel)
		if ch.needUpdate(clusterConfig) {
			return ch.switchConfig(channelConfigToRaftConfig(s.opts.NodeId, clusterConfig))
		}
		return nil
	}

	if clusterConfig.LeaderId != s.opts.NodeId {
		return nil
	}
	ch, err := createChannel(clusterConfig, s, rg)
	if err != nil {
		return err
	}
	rg.AddRaft(ch)

	err = ch.switchConfig(channelConfigToRaftConfig(s.opts.NodeId, clusterConfig))
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) WakeFollowerfNeed(channelId string, channelType uint8) error {
	clusterConfig, err := s.opts.Cluster.GetOrCreateChannelClusterConfigFromSlotLeader(channelId, channelType)
	if err != nil {
		return err
	}
	isReplica := false
	for _, nodeId := range clusterConfig.Replicas {
		if nodeId == s.opts.NodeId {
			isReplica = true
			break
		}
	}
	if !isReplica {
		for _, nodeId := range clusterConfig.Learners {
			if nodeId == s.opts.NodeId {
				isReplica = true
				break
			}
		}
	}

	channelKey := msutil.ChannelToKey(clusterConfig.ChannelId, clusterConfig.ChannelType)
	rg := s.getRaftGroup(channelKey)
	if isReplica {
		ch, err := createChannel(clusterConfig, s, rg)
		if err != nil {
			return err
		}
		rg.AddRaft(ch)

		err = ch.switchConfig(channelConfigToRaftConfig(s.opts.NodeId, clusterConfig))
		if err != nil {
			return err
		}

		// 立马同步
		ch.rg.AddEvent(channelKey, rafttype.Event{
			Type: rafttype.NotifySync,
		})
		ch.rg.Advance()
	}
	return nil
}

// 异步唤醒频道副本
func (s *Server) wakeFollowerIfNeedAsync(channelId string, channelType uint8) {

	s.wake.Lock()
	channelKey := msutil.ChannelToKey(channelId, channelType)
	if ok := s.wake.channels[channelKey]; ok {
		s.wake.Unlock()
		return
	}
	s.wake.channels[channelKey] = true
	s.wake.Unlock()

	go func() {
		err := s.WakeFollowerfNeed(channelId, channelType)
		if err != nil {
			s.Error("wake channel failed", zap.Error(err))
		}
		s.wake.Lock()
		delete(s.wake.channels, channelKey)
		s.wake.Unlock()
	}()
}

func (s *Server) AddEvent(channelKey string, e rafttype.Event) {

	// 添加事件到对应的频道
	channelId, channelType := msutil.ChannelFromlKey(channelKey)
	rg := s.getRaftGroup(channelKey)

	raft := rg.GetRaft(channelKey)

	// 如果领导发过来的ping消息，则需要判断是否需要唤醒副本频道
	if raft == nil && (e.Type == rafttype.Ping || e.Type == rafttype.NotifySync) {
		s.wakeFollowerIfNeedAsync(channelId, channelType)
	} else {
		rg.AddEvent(channelKey, e)
		rg.Advance()
	}

}

func (s *Server) getRaftGroup(channelKey string) *raftgroup.RaftGroup {
	index := int(fasthash.Hash(channelKey) % uint32(s.opts.GroupCount))
	return s.raftGroups[index]
}

func (s *Server) ChannelCount() int {
	var count int

	for _, rg := range s.raftGroups {
		count += rg.GetRaftCount()
	}
	return count
}

func (s *Server) ExistChannel(channelId string, channelType uint8) bool {
	channelKey := msutil.ChannelToKey(channelId, channelType)
	rg := s.getRaftGroup(channelKey)
	return rg.GetRaft(channelKey) != nil
}

func (s *Server) Channel(channelId string, channelType uint8) *Channel {
	channelKey := msutil.ChannelToKey(channelId, channelType)
	rg := s.getRaftGroup(channelKey)
	raft := rg.GetRaft(channelKey)
	if raft != nil {
		return raft.(*Channel)
	}
	return nil
}

func (s *Server) RemoveChannel(channelId string, channelType uint8) {
	channelKey := msutil.ChannelToKey(channelId, channelType)
	rg := s.getRaftGroup(channelKey)
	raft := rg.GetRaft(channelKey)
	if raft != nil {
		rg.RemoveRaft(raft)
	}

}

func (s *Server) LastIndexAndAppendTime(channelId string, channelType uint8) (uint64, uint64, error) {
	channelKey := msutil.ChannelToKey(channelId, channelType)
	return s.storage.LastIndexAndAppendTime(channelKey)
}

func (s *Server) LastLogIndexAndTerm(channelId string, channelType uint8) (uint32, uint64, error) {
	msg, err := s.storage.getLastMessage(channelId, channelType)
	if err != nil && err != msdb.ErrNotFound {
		return 0, 0, err
	}

	return uint32(msg.Term), uint64(msg.MessageSeq), nil
}
