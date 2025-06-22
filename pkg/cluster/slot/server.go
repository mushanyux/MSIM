package slot

import (
	"context"
	"path"
	"strconv"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/mushanyux/MSIM/pkg/raft/raftgroup"
	"github.com/mushanyux/MSIM/pkg/raft/types"
	"go.uber.org/zap"
)

type Server struct {
	raftGroup *raftgroup.RaftGroup
	storage   *PebbleShardLogStorage
	opts      *Options
	mslog.Log
	genLogId *snowflake.Node

	slotUpdateLock sync.Mutex
}

func NewServer(opts *Options) *Server {
	s := &Server{
		opts: opts,
		Log:  mslog.NewMSLog("slot.Server"),
	}
	s.storage = NewPebbleShardLogStorage(s, path.Join(opts.DataDir, "logdb"), uint32(opts.SlotDbShardNum))
	s.raftGroup = raftgroup.New(raftgroup.NewOptions(raftgroup.WithLogPrefix("slot"), raftgroup.WithStorage(s.storage), raftgroup.WithTransport(opts.Transport)))
	var err error
	s.genLogId, err = snowflake.NewNode(int64(opts.NodeId))
	if err != nil {
		s.Panic("snowflake.NewNode failed", zap.Error(err))
	}
	return s
}

func (s *Server) Start() error {

	err := s.storage.Open()
	if err != nil {
		return err
	}

	err = s.raftGroup.Start()
	if err != nil {
		return err
	}

	slots := s.opts.Node.Slots()
	for _, slot := range slots {
		s.AddOrUpdateSlotRaft(slot)
	}

	return nil
}

func (s *Server) Stop() {
	s.raftGroup.Stop()

	err := s.storage.Close()
	if err != nil {
		s.Error("storage close failed", zap.Error(err))
	}
}

func (s *Server) AddEvent(shardNo string, event types.Event) {
	s.raftGroup.AddEvent(shardNo, event)
	s.raftGroup.Advance()
}

func SlotIdToKey(slotId uint32) string {
	return strconv.FormatUint(uint64(slotId), 10)
}

func KeyToSlotId(key string) uint32 {
	slotId, _ := strconv.ParseUint(key, 10, 32)
	return uint32(slotId)
}

func (s *Server) WaitAllSlotReady(ctx context.Context, slotCount int) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ready := true
		rafts := s.raftGroup.GetRafts()
		if slotCount == len(rafts) {
			for _, raft := range rafts {
				if raft.LeaderId() == 0 {
					ready = false
					break
				}
			}
		} else {
			ready = false
		}

		if ready {
			return nil
		}
	}
}

func (s *Server) GenLogId() uint64 {
	return uint64(s.genLogId.Generate().Int64())
}

// 获取频道所在的slotId
func (s *Server) getSlotId(v string) uint32 {
	var slotCount uint32 = s.opts.Node.SlotCount()
	if slotCount == 0 {
		slotCount = s.opts.SlotCount
	}
	return msutil.GetSlotNum(int(slotCount), v)
}

func (s *Server) AppliedIndex(slotId uint32) (uint64, error) {
	shardNo := SlotIdToKey(slotId)
	return s.storage.AppliedIndex(shardNo)
}

func (s *Server) LastIndex(slotId uint32) (uint64, error) {
	shardNo := SlotIdToKey(slotId)
	return s.storage.LastIndex(shardNo)
}

func (s *Server) LastLog(slotId uint32) (types.Log, error) {
	return s.storage.LastLog(SlotIdToKey(slotId))
}

func (s *Server) GetSlotRaft(slotId uint32) *Slot {
	raft := s.raftGroup.GetRaft(SlotIdToKey(slotId))
	if raft == nil {
		return nil
	}
	return raft.(*Slot)
}

func (s *Server) GetLogsInReverseOrder(slotId uint32, startLogIndex uint64, endLogIndex uint64, limit int) ([]types.Log, error) {
	shardNo := SlotIdToKey(slotId)
	return s.storage.GetLogsInReverseOrder(shardNo, startLogIndex, endLogIndex, limit)
}
