package channel

import (
	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/mushanyux/MSIM/pkg/raft/types"
	"go.uber.org/zap"
)

type storage struct {
	db msdb.DB
	s  *Server
	mslog.Log
}

func newStorage(db msdb.DB, s *Server) *storage {
	return &storage{db: db, s: s, Log: mslog.NewMSLog("channel_storage")}
}

func (s *storage) GetState(channelId string, channelType uint8) (types.RaftState, error) {
	lastMsg, err := s.db.GetLastMsg(channelId, channelType)
	if err != nil {
		return types.RaftState{}, err
	}

	return types.RaftState{
		LastLogIndex: uint64(lastMsg.MessageSeq),
		LastTerm:     uint32(lastMsg.Term),
		AppliedIndex: uint64(lastMsg.MessageSeq),
	}, nil
}

func (s *storage) AppendLogs(key string, logs []types.Log, termStartIndexInfo *types.TermStartIndexInfo) error {

	channelId, channelType := msutil.ChannelFromlKey(key)

	messages := make([]msdb.Message, 0, len(logs))
	for _, log := range logs {
		var msg msdb.Message
		err := msg.Unmarshal(log.Data)
		if err != nil {
			s.Error("AppendLogs: unmarshal message error", zap.Error(err))
			return err
		}
		msg.MessageSeq = uint32(log.Index)
		msg.Term = uint64(log.Term)
		messages = append(messages, msg)
	}
	err := s.db.AppendMessages(channelId, channelType, messages)
	if err != nil {
		return err
	}
	if termStartIndexInfo != nil {
		err = s.db.SetLeaderTermStartIndex(key, termStartIndexInfo.Term, termStartIndexInfo.Index)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) GetTermStartIndex(key string, term uint32) (uint64, error) {

	return s.db.LeaderTermStartIndex(key, term)
}

func (s *storage) LeaderLastLogTerm(key string) (uint32, error) {
	return s.db.LeaderLastTerm(key)
}

func (s *storage) LeaderTermGreaterEqThan(key string, term uint32) (uint32, error) {
	return s.db.LeaderLastTermGreaterEqThan(key, term)
}

func (s *storage) GetLogs(key string, startLogIndex uint64, endLogIndex uint64, limitSize uint64) ([]types.Log, error) {
	channelID, channelType := msutil.ChannelFromlKey(key)
	var (
		messages []msdb.Message
		err      error
	)

	lastIdx, err := s.LastIndex(key)
	if err != nil {
		return nil, err
	}

	if endLogIndex == 0 || endLogIndex > lastIdx+1 {
		endLogIndex = lastIdx + 1
	}

	messages, err = s.db.LoadNextRangeMsgsForSize(channelID, channelType, startLogIndex, endLogIndex, limitSize)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, nil
	}
	logs := make([]types.Log, len(messages))
	for i, msg := range messages {
		data, err := msg.Marshal()
		if err != nil {
			return nil, err
		}
		logs[i] = types.Log{
			Id:    uint64(msg.MessageID),
			Index: uint64(msg.MessageSeq),
			Term:  uint32(msg.Term),
			Data:  data,
		}
	}
	return logs, nil
}

func (s *storage) Apply(key string, logs []types.Log) error {
	return nil
}

func (s *storage) SaveConfig(key string, cfg types.Config) error {
	if s.s.opts.OnSaveConfig != nil {
		channelId, channelType := msutil.ChannelFromlKey(key)
		err := s.s.opts.OnSaveConfig(channelId, channelType, cfg)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *storage) TruncateLogTo(key string, index uint64) error {
	channelId, channelType := msutil.ChannelFromlKey(key)
	return s.db.TruncateLogTo(channelId, channelType, index)
}

// 最后一条日志的索引
func (s *storage) LastIndex(key string) (uint64, error) {
	channelId, channelType := msutil.ChannelFromlKey(key)

	lastMsgSeq, _, err := s.db.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		return 0, err
	}
	return uint64(lastMsgSeq), nil
}

// DeleteLeaderTermStartIndexGreaterThanTerm 删除大于term的领导任期和开始索引
func (s *storage) DeleteLeaderTermStartIndexGreaterThanTerm(key string, term uint32) error {

	return s.db.DeleteLeaderTermStartIndexGreaterThanTerm(key, term)
}

func (s *storage) LastIndexAndAppendTime(shardNo string) (uint64, uint64, error) {
	channelId, channelType := msutil.ChannelFromlKey(shardNo)
	lastMsgSeq, appendTime, err := s.db.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		return 0, 0, err
	}
	return uint64(lastMsgSeq), appendTime, nil
}

func (s *storage) getLastMessage(channelId string, channelType uint8) (msdb.Message, error) {

	return s.db.GetLastMsg(channelId, channelType)
}
