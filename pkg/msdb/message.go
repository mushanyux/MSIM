package msdb

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
	msproto "github.com/mushanyux/MSIMGoProto"
	"go.uber.org/zap"
)

func (ms *mushanDB) AppendMessages(channelId string, channelType uint8, msgs []Message) error {

	ms.metrics.AppendMessagesAdd(1)

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			cost := time.Since(start)
			if cost.Milliseconds() > 1000 {
				ms.Info("appendMessages done", zap.Duration("cost", cost), zap.String("channelId", channelId), zap.Uint8("channelType", channelType), zap.Int("msgCount", len(msgs)))
			}
		}()
	}

	batch := ms.channelBatchDb(channelId, channelType).NewBatch()
	for _, msg := range msgs {
		if err := ms.writeMessage(channelId, channelType, msg, batch); err != nil {
			return err
		}

	}
	lastMsg := msgs[len(msgs)-1]
	err := ms.setChannelLastMessageSeq(channelId, channelType, uint64(lastMsg.MessageSeq), batch)
	if err != nil {
		return err
	}

	err = batch.CommitWait()
	if err != nil {
		return err
	}

	ms.channelSeqCache.setChannelLastSeq(channelId, channelType, uint64(lastMsg.MessageSeq))

	return nil
}

func (ms *mushanDB) channelDb(channelId string, channelType uint8) *pebble.DB {
	dbIndex := ms.channelDbIndex(channelId, channelType)
	return ms.shardDBById(uint32(dbIndex))
}

func (ms *mushanDB) channelBatchDb(channelId string, channelType uint8) *BatchDB {
	dbIndex := ms.channelDbIndex(channelId, channelType)
	return ms.shardBatchDBById(uint32(dbIndex))
}

func (ms *mushanDB) channelDbIndex(channelId string, channelType uint8) uint32 {
	return uint32(key.ChannelToNum(channelId, channelType) % uint64(len(ms.dbs)))
}

func (ms *mushanDB) GetMessage(messageId uint64) (Message, error) {

	ms.metrics.GetMessageAdd(1)

	messageIdKey := key.NewMessageIndexMessageIdKey(messageId)

	for _, db := range ms.dbs {
		result, closer, err := db.Get(messageIdKey)
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return EmptyMessage, err
		}
		defer closer.Close()

		if len(result) != 16 {
			return EmptyMessage, fmt.Errorf("invalid message index key")
		}
		var arr [16]byte
		copy(arr[:], result)
		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewMessageColumnKeyWithPrimary(arr, key.MinColumnKey),
			UpperBound: key.NewMessageColumnKeyWithPrimary(arr, key.MaxColumnKey),
		})
		defer iter.Close()

		var msg Message
		err = ms.iteratorChannelMessages(iter, 0, func(m Message) bool {
			msg = m
			return false
		})

		if err != nil {
			return EmptyMessage, err
		}
		if IsEmptyMessage(msg) {
			return EmptyMessage, ErrNotFound
		}
		return msg, nil
	}
	return EmptyMessage, ErrNotFound
}

// 情况1: startMessageSeq=100, endMessageSeq=0, limit=10 返回的消息seq为91-100的消息 (limit生效)
// 情况2: startMessageSeq=5, endMessageSeq=0, limit=10 返回的消息seq为1-5的消息（消息无）

// 情况3: startMessageSeq=100, endMessageSeq=95, limit=10 返回的消息seq为96-100的消息（endMessageSeq生效）
// 情况4: startMessageSeq=100, endMessageSeq=50, limit=10 返回的消息seq为91-100的消息（limit生效）
func (ms *mushanDB) LoadPrevRangeMsgs(channelId string, channelType uint8, startMessageSeq, endMessageSeq uint64, limit int) ([]Message, error) {

	ms.metrics.LoadPrevRangeMsgsAdd(1)

	if startMessageSeq == 0 {
		return nil, fmt.Errorf("start messageSeq[%d] must be greater than 0", startMessageSeq)

	}
	if endMessageSeq != 0 && endMessageSeq > startMessageSeq {
		return nil, fmt.Errorf("end messageSeq[%d] must be less than start messageSeq[%d]", endMessageSeq, startMessageSeq)
	}

	var minSeq uint64
	var maxSeq uint64

	if endMessageSeq == 0 {
		maxSeq = startMessageSeq + 1
		if startMessageSeq < uint64(limit) {
			minSeq = 1
		} else {
			minSeq = startMessageSeq - uint64(limit) + 1
		}
	} else {
		maxSeq = startMessageSeq + 1
		if startMessageSeq-endMessageSeq > uint64(limit) {
			minSeq = startMessageSeq - uint64(limit) + 1
		} else {
			minSeq = endMessageSeq + 1
		}

	}

	// 获取频道的最大的messageSeq，超过这个的消息都视为无效
	lastSeq, _, err := ms.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		return nil, err
	}

	if maxSeq > lastSeq {
		maxSeq = lastSeq + 1
	}

	db := ms.channelDb(channelId, channelType)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewMessagePrimaryKey(channelId, channelType, minSeq),
		UpperBound: key.NewMessagePrimaryKey(channelId, channelType, maxSeq),
	})
	defer iter.Close()

	msgs := make([]Message, 0)
	err = ms.iteratorChannelMessages(iter, limit, func(m Message) bool {
		msgs = append(msgs, m)
		return true
	})
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

func (ms *mushanDB) LoadNextRangeMsgs(channelId string, channelType uint8, startMessageSeq, endMessageSeq uint64, limit int) ([]Message, error) {

	ms.metrics.LoadNextRangeMsgsAdd(1)

	minSeq := startMessageSeq
	maxSeq := endMessageSeq
	if endMessageSeq == 0 {
		maxSeq = math.MaxUint64
	}

	// 获取频道的最大的messageSeq，超过这个的消息都视为无效
	lastSeq, _, err := ms.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		return nil, err
	}

	if maxSeq > lastSeq {
		maxSeq = lastSeq + 1
	}

	db := ms.channelDb(channelId, channelType)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewMessagePrimaryKey(channelId, channelType, minSeq),
		UpperBound: key.NewMessagePrimaryKey(channelId, channelType, maxSeq),
	})
	defer iter.Close()

	msgs := make([]Message, 0)

	err = ms.iteratorChannelMessages(iter, limit, func(m Message) bool {
		msgs = append(msgs, m)
		return true
	})
	if err != nil {
		return nil, err
	}
	return msgs, nil

}

func (ms *mushanDB) LoadMsg(channelId string, channelType uint8, seq uint64) (Message, error) {

	ms.metrics.LoadMsgAdd(1)

	db := ms.channelDb(channelId, channelType)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewMessagePrimaryKey(channelId, channelType, seq),
		UpperBound: key.NewMessagePrimaryKey(channelId, channelType, seq+1),
	})
	defer iter.Close()
	var msg Message
	err := ms.iteratorChannelMessages(iter, 1, func(m Message) bool {
		msg = m
		return false
	})
	if err != nil {
		return EmptyMessage, err
	}
	if IsEmptyMessage(msg) {
		return EmptyMessage, ErrNotFound
	}
	return msg, nil
}

func (ms *mushanDB) LoadLastMsgs(channelId string, channelType uint8, limit int) ([]Message, error) {
	ms.metrics.LoadLastMsgsAdd(1)

	lastSeq, _, err := ms.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		return nil, err
	}
	if lastSeq == 0 {
		return nil, nil
	}
	return ms.LoadPrevRangeMsgs(channelId, channelType, lastSeq, 0, limit)
}

// 获取最新的一条消息
func (ms *mushanDB) GetLastMsg(channelId string, channelType uint8) (Message, error) {
	lastSeq, _, err := ms.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		return EmptyMessage, err
	}
	if lastSeq == 0 {
		return EmptyMessage, nil
	}
	return ms.LoadMsg(channelId, channelType, lastSeq)
}

func (ms *mushanDB) LoadLastMsgsWithEnd(channelID string, channelType uint8, endMessageSeq uint64, limit int) ([]Message, error) {

	ms.metrics.LoadLastMsgsWithEndAdd(1)

	lastSeq, _, err := ms.GetChannelLastMessageSeq(channelID, channelType)
	if err != nil {
		return nil, err
	}
	if lastSeq == 0 {
		return nil, nil
	}
	if endMessageSeq > lastSeq {
		return nil, nil
	}
	return ms.LoadPrevRangeMsgs(channelID, channelType, lastSeq, endMessageSeq, limit)
}

func (ms *mushanDB) LoadNextRangeMsgsForSize(channelId string, channelType uint8, startMessageSeq, endMessageSeq uint64, limitSize uint64) ([]Message, error) {
	ms.metrics.LoadNextRangeMsgsForSizeAdd(1)

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			cost := time.Since(start)
			if cost.Milliseconds() > 200 {
				ms.Info("loadNextRangeMsgsForSize done", zap.Duration("cost", time.Since(start)), zap.String("channelId", channelId), zap.Uint8("channelType", channelType), zap.Uint64("startMessageSeq", startMessageSeq), zap.Uint64("endMessageSeq", endMessageSeq))
			}
		}()
	}

	minSeq := startMessageSeq
	maxSeq := endMessageSeq

	if endMessageSeq == 0 {
		maxSeq = math.MaxUint64
	}
	db := ms.channelDb(channelId, channelType)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewMessagePrimaryKey(channelId, channelType, minSeq),
		UpperBound: key.NewMessagePrimaryKey(channelId, channelType, maxSeq),
	})
	defer iter.Close()
	return ms.parseChannelMessagesWithLimitSize(iter, limitSize)
}

func (ms *mushanDB) TruncateLogTo(channelId string, channelType uint8, messageSeq uint64) error {
	ms.metrics.TruncateLogToAdd(1)

	if messageSeq == 0 {
		return fmt.Errorf("messageSeq[%d] must be greater than 0", messageSeq)

	}

	// 获取最新的消息seq
	lastMsgSeq, _, err := ms.GetChannelLastMessageSeq(channelId, channelType)
	if err != nil {
		ms.Error("TruncateLogTo: getChannelLastMessageSeq", zap.Error(err))
		return err
	}

	if messageSeq >= lastMsgSeq {
		return nil
	}

	ms.Warn("truncateLogTo message", zap.Uint64("messageSeq", messageSeq), zap.Uint64("lastMsgSeq", lastMsgSeq), zap.String("channelId", channelId), zap.Uint8("channelType", channelType), zap.Uint64("messageSeq", messageSeq))

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			ms.Info("truncateLogTo done", zap.Duration("cost", time.Since(start)), zap.String("channelId", channelId), zap.Uint8("channelType", channelType), zap.Uint64("messageSeq", messageSeq))
		}()
	}

	db := ms.channelBatchDb(channelId, channelType)
	batch := db.NewBatch()
	batch.DeleteRange(key.NewMessagePrimaryKey(channelId, channelType, messageSeq+1), key.NewMessagePrimaryKey(channelId, channelType, math.MaxUint64))

	err = ms.setChannelLastMessageSeq(channelId, channelType, messageSeq, batch)
	if err != nil {
		return err
	}

	err = batch.CommitWait()
	if err != nil {
		return err
	}

	ms.channelSeqCache.invalidateChannelLastSeq(channelId, channelType)

	return nil
}

func (ms *mushanDB) GetChannelLastMessageSeq(channelId string, channelType uint8) (uint64, uint64, error) {
	ms.metrics.GetChannelLastMessageSeqAdd(1)

	seq, setTime, ok := ms.channelSeqCache.getChannelLastSeq(channelId, channelType)
	if ok {
		return seq, setTime, nil
	}

	db := ms.channelDb(channelId, channelType)
	result, closer, err := db.Get(key.NewChannelLastMessageSeqKey(channelId, channelType))
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	defer closer.Close()

	if len(result) == 0 {
		return 0, 0, nil
	}

	seq = ms.endian.Uint64(result[0:8])
	setTime = ms.endian.Uint64(result[8:16])

	ms.channelSeqCache.setChannelLastSeqWithBytes(channelId, channelType, result)

	return seq, setTime, nil
}

func (ms *mushanDB) SetChannelLastMessageSeq(channelId string, channelType uint8, seq uint64) error {
	ms.metrics.SetChannelLastMessageSeqAdd(1)

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			cost := time.Since(start)
			if cost.Milliseconds() > 200 {
				ms.Info("SetChannelLastMessageSeq done", zap.Duration("cost", cost), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
			}
		}()
	}
	batch := ms.channelBatchDb(channelId, channelType).NewBatch()
	err := ms.setChannelLastMessageSeq(channelId, channelType, seq, batch)
	if err != nil {
		return err
	}
	err = batch.CommitWait()
	if err != nil {
		return err
	}
	ms.channelSeqCache.setChannelLastSeq(channelId, channelType, seq)
	return nil
}

var minMessagePrimaryKey = [16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var maxMessagePrimaryKey = [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

func (ms *mushanDB) searchMessageByIndex(req MessageSearchReq, db *pebble.DB, iterFnc func(m Message) bool) (bool, error) {
	var lomsey []byte
	var highKey []byte

	var existKey = false

	if strings.TrimSpace(req.FromUid) != "" {

		lomsey = key.NewMessageSecondIndexFromUidKey(req.FromUid, minMessagePrimaryKey)
		highKey = key.NewMessageSecondIndexFromUidKey(req.FromUid, maxMessagePrimaryKey)
		existKey = true
	}

	if strings.TrimSpace(req.ClientMsgNo) != "" && !existKey {
		lomsey = key.NewMessageSecondIndexClientMsgNoKey(req.ClientMsgNo, minMessagePrimaryKey)
		highKey = key.NewMessageSecondIndexClientMsgNoKey(req.ClientMsgNo, maxMessagePrimaryKey)
		existKey = true
	}

	if !existKey {
		return false, nil
	}

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: lomsey,
		UpperBound: highKey,
	})
	defer iter.Close()

	for iter.Last(); iter.Valid(); iter.Prev() {
		primaryBytes, err := key.ParseMessageSecondIndexKey(iter.Key())
		if err != nil {
			ms.Error("parseMessageIndexKey", zap.Error(err))
			continue
		}

		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewMessageColumnKeyWithPrimary(primaryBytes, key.MinColumnKey),
			UpperBound: key.NewMessageColumnKeyWithPrimary(primaryBytes, key.MaxColumnKey),
		})

		defer iter.Close()

		var msg Message
		err = ms.iteratorChannelMessages(iter, 0, func(m Message) bool {
			msg = m
			return false
		})
		if err != nil {
			return false, err
		}
		if iterFnc != nil {
			if !iterFnc(msg) {
				break
			}
		}
	}

	return true, nil
}

func (ms *mushanDB) SearchMessages(req MessageSearchReq) ([]Message, error) {
	ms.metrics.SearchMessagesAdd(1)

	if req.MessageId > 0 { // 如果指定了messageId，则直接查询messageId，这种情况要么没有要么只有一条
		msg, err := ms.GetMessage(uint64(req.MessageId))
		if err != nil {
			if err == ErrNotFound {
				return nil, nil
			}
			return nil, err
		}
		return []Message{msg}, nil
	}

	iterFnc := func(msgs *[]Message) func(m Message) bool {
		currSize := 0
		return func(m Message) bool {
			if strings.TrimSpace(req.ChannelId) != "" && m.ChannelID != req.ChannelId {
				return true
			}

			if req.ChannelType != 0 && req.ChannelType != m.ChannelType {
				return true
			}

			if strings.TrimSpace(req.FromUid) != "" && m.FromUID != req.FromUid {
				return true
			}

			if strings.TrimSpace(req.ClientMsgNo) != "" && m.ClientMsgNo != req.ClientMsgNo {
				return true
			}

			if len(req.Payload) > 0 && !bytes.Contains(m.Payload, req.Payload) {
				return true
			}

			if req.MessageId > 0 && req.MessageId != m.MessageID {
				return true
			}

			if req.Pre {
				if req.OffsetMessageId > 0 && m.MessageID <= req.OffsetMessageId { // 当前消息小于等于req.MessageId时停止查询
					return false
				}
			} else {
				if req.OffsetMessageId > 0 && m.MessageID >= req.OffsetMessageId { // 当前消息小于等于req.MessageId时停止查询
					return false
				}
			}

			if currSize >= req.Limit { // 消息数量大于等于limit时停止查询
				return false
			}
			currSize++

			*msgs = append(*msgs, m)

			return true
		}
	}

	if strings.TrimSpace(req.ChannelId) != "" && req.ChannelType != 0 {
		db := ms.channelDb(req.ChannelId, req.ChannelType)
		msgs := make([]Message, 0, req.Limit)
		fnc := iterFnc(&msgs)

		startSeq := req.OffsetMessageSeq
		var endSeq uint64 = math.MaxUint64

		if req.OffsetMessageSeq > 0 {
			if req.Pre {
				startSeq = req.OffsetMessageSeq + 1
				endSeq = math.MaxUint64
			} else {
				startSeq = 0
				endSeq = req.OffsetMessageSeq
			}
		}

		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewMessagePrimaryKey(req.ChannelId, req.ChannelType, startSeq),
			UpperBound: key.NewMessagePrimaryKey(req.ChannelId, req.ChannelType, endSeq),
		})
		defer iter.Close()

		err := ms.iteratorChannelMessagesDirection(iter, 0, !req.Pre, fnc)
		if err != nil {
			return nil, err
		}

		return msgs, nil

	}

	allMsgs := make([]Message, 0, req.Limit*len(ms.dbs))
	for _, db := range ms.dbs {
		msgs := make([]Message, 0)
		fnc := iterFnc(&msgs)
		// 通过索引查询
		has, err := ms.searchMessageByIndex(req, db, fnc)
		if err != nil {
			return nil, err
		}

		if !has { // 如果有触发索引，则无需全局查询
			startMessageId := uint64(req.OffsetMessageId)
			var endMessageId uint64 = math.MaxUint64

			if req.OffsetMessageId > 0 {
				if req.Pre {
					startMessageId = uint64(req.OffsetMessageId + 1)
					endMessageId = math.MaxUint64
				} else {
					startMessageId = 0
					endMessageId = uint64(req.OffsetMessageId)
				}
			}

			iter := db.NewIter(&pebble.IterOptions{
				LowerBound: key.NewMessageIndexMessageIdKey(startMessageId),
				UpperBound: key.NewMessageIndexMessageIdKey(endMessageId),
			})
			defer iter.Close()

			var pkey [16]byte
			var iterStepFnc func() bool
			if req.Pre {
				if !iter.First() {
					continue
				}
				iterStepFnc = iter.Next
			} else {
				if !iter.Last() {
					continue
				}
				iterStepFnc = iter.Prev
			}

			for ; iter.Valid(); iterStepFnc() {
				copy(pkey[:], iter.Value())
				resultIter := db.NewIter(&pebble.IterOptions{
					LowerBound: key.NewMessageColumnKeyWithPrimary(pkey, key.MinColumnKey),
					UpperBound: key.NewMessageColumnKeyWithPrimary(pkey, key.MaxColumnKey),
				})
				defer resultIter.Close()
				err = ms.iteratorChannelMessages(resultIter, 0, fnc)
				if err != nil {
					return nil, err
				}
				if len(msgs) >= req.Limit {
					break
				}
			}
		}

		// 将msgs里消息时间比allMsgs里的消息时间早的消息插入到allMsgs里
		allMsgs = append(allMsgs, msgs...)
	}

	// 按照messageId降序排序
	sort.Slice(allMsgs, func(i, j int) bool {
		return allMsgs[i].MessageID > allMsgs[j].MessageID
	})

	// 如果allMsgs的数量大于limit，则截取前limit个
	if req.Limit > 0 && len(allMsgs) > req.Limit {
		if req.Pre {
			allMsgs = allMsgs[len(allMsgs)-req.Limit:]
		} else {
			allMsgs = allMsgs[:req.Limit]
		}

	}

	return allMsgs, nil
}

func (ms *mushanDB) setChannelLastMessageSeq(channelId string, channelType uint8, seq uint64, w *Batch) error {
	data := make([]byte, 16)
	ms.endian.PutUint64(data[0:8], seq)
	setTime := time.Now().UnixNano()
	ms.endian.PutUint64(data[8:16], uint64(setTime))

	w.Set(key.NewChannelLastMessageSeqKey(channelId, channelType), data)
	return nil
}

func (ms *mushanDB) iteratorChannelMessages(iter *pebble.Iterator, limit int, iterFnc func(m Message) bool) error {
	return ms.iteratorChannelMessagesDirection(iter, limit, false, iterFnc)
}

func (ms *mushanDB) iteratorChannelMessagesDirection(iter *pebble.Iterator, limit int, reverse bool, iterFnc func(m Message) bool) error {
	var (
		size           int
		preMessageSeq  uint64
		preMessage     Message
		lastNeedAppend bool = true
		hasData        bool = false
	)

	if reverse {
		if !iter.Last() {
			return nil
		}
	} else {
		if !iter.First() {
			return nil
		}
	}
	for iter.Valid() {
		if reverse {
			if !iter.Prev() {
				break
			}
		} else {
			if !iter.Next() {
				break
			}
		}
		messageSeq, coulmnName, err := key.ParseMessageColumnKey(iter.Key())
		if err != nil {
			return err
		}

		if preMessageSeq != messageSeq {
			if preMessageSeq != 0 {
				size++
				if iterFnc != nil {
					if !iterFnc(preMessage) {
						lastNeedAppend = false
						break
					}
				}
				if limit != 0 && size >= limit {
					lastNeedAppend = false
					break
				}
			}

			preMessageSeq = messageSeq
			preMessage = Message{}
			preMessage.MessageSeq = uint32(messageSeq)
		}

		switch coulmnName {
		case key.TableMessage.Column.Header:
			preMessage.RecvPacket.Framer = msproto.FramerFromUint8(iter.Value()[0])
		case key.TableMessage.Column.Setting:
			preMessage.RecvPacket.Setting = msproto.Setting(iter.Value()[0])
		case key.TableMessage.Column.Expire:
			preMessage.RecvPacket.Expire = ms.endian.Uint32(iter.Value())
		case key.TableMessage.Column.MessageId:
			preMessage.MessageID = int64(ms.endian.Uint64(iter.Value()))
		case key.TableMessage.Column.ClientMsgNo:
			preMessage.ClientMsgNo = string(iter.Value())
		case key.TableMessage.Column.StreamNo:
			preMessage.StreamNo = string(iter.Value())
		case key.TableMessage.Column.Timestamp:
			preMessage.Timestamp = int32(ms.endian.Uint32(iter.Value()))
		case key.TableMessage.Column.ChannelId:
			preMessage.ChannelID = string(iter.Value())
		case key.TableMessage.Column.ChannelType:
			preMessage.ChannelType = iter.Value()[0]
		case key.TableMessage.Column.Topic:
			preMessage.Topic = string(iter.Value())
		case key.TableMessage.Column.FromUid:
			preMessage.RecvPacket.FromUID = string(iter.Value())
		case key.TableMessage.Column.Payload:
			// 这里必须复制一份，否则会被pebble覆盖
			var payload = make([]byte, len(iter.Value()))
			copy(payload, iter.Value())
			preMessage.Payload = payload
		case key.TableMessage.Column.Term:
			preMessage.Term = ms.endian.Uint64(iter.Value())

		}
		hasData = true
	}
	if lastNeedAppend && hasData {
		if iterFnc != nil {

			_ = iterFnc(preMessage)
		}
	}

	return nil

}

func (ms *mushanDB) parseChannelMessagesWithLimitSize(iter *pebble.Iterator, limitSize uint64) ([]Message, error) {
	var (
		msgs           = make([]Message, 0)
		preMessageSeq  uint64
		preMessage     Message
		lastNeedAppend bool = false
	)

	var size uint64 = 0
	for iter.First(); iter.Valid(); iter.Next() {
		lastNeedAppend = true
		messageSeq, coulmnName, err := key.ParseMessageColumnKey(iter.Key())
		if err != nil {
			return nil, err
		}

		if messageSeq == 0 {
			ms.Panic("messageSeq is 0", zap.Any("key", iter.Key()), zap.Any("coulmnName", coulmnName))
		}

		if preMessageSeq != messageSeq {
			if preMessageSeq != 0 {
				size += uint64(preMessage.Size())
				msgs = append(msgs, preMessage)
				if limitSize != 0 && size >= limitSize {
					lastNeedAppend = false
					break
				}
			}

			preMessageSeq = messageSeq
			preMessage = Message{}
			preMessage.MessageSeq = uint32(messageSeq)
		}

		switch coulmnName {
		case key.TableMessage.Column.Header:
			preMessage.RecvPacket.Framer = msproto.FramerFromUint8(iter.Value()[0])
		case key.TableMessage.Column.Setting:
			preMessage.RecvPacket.Setting = msproto.Setting(iter.Value()[0])
		case key.TableMessage.Column.Expire:
			preMessage.RecvPacket.Expire = ms.endian.Uint32(iter.Value())
		case key.TableMessage.Column.MessageId:
			preMessage.MessageID = int64(ms.endian.Uint64(iter.Value()))
		case key.TableMessage.Column.ClientMsgNo:
			preMessage.ClientMsgNo = string(iter.Value())
		case key.TableMessage.Column.StreamNo:
			preMessage.StreamNo = string(iter.Value())
		case key.TableMessage.Column.Timestamp:
			preMessage.Timestamp = int32(ms.endian.Uint32(iter.Value()))
		case key.TableMessage.Column.ChannelId:
			preMessage.ChannelID = string(iter.Value())
		case key.TableMessage.Column.ChannelType:
			preMessage.ChannelType = iter.Value()[0]
		case key.TableMessage.Column.Topic:
			preMessage.Topic = string(iter.Value())
		case key.TableMessage.Column.FromUid:
			preMessage.RecvPacket.FromUID = string(iter.Value())
		case key.TableMessage.Column.Payload:
			// 这里必须复制一份，否则会被pebble覆盖
			var payload = make([]byte, len(iter.Value()))
			copy(payload, iter.Value())
			preMessage.Payload = payload
		case key.TableMessage.Column.Term:
			preMessage.Term = ms.endian.Uint64(iter.Value())
		}
	}

	if lastNeedAppend {
		msgs = append(msgs, preMessage)
	}

	return msgs, nil

}

func (ms *mushanDB) writeMessage(channelId string, channelType uint8, msg Message, w *Batch) error {
	var (
		messageIdBytes = make([]byte, 8)
	)

	// header
	header := msproto.ToFixHeaderUint8(msg.RecvPacket.Framer)
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.Header), []byte{header})

	// setting
	setting := msg.RecvPacket.Setting.Uint8()
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.Setting), []byte{setting})

	// expire
	expireBytes := make([]byte, 4)
	ms.endian.PutUint32(expireBytes, msg.RecvPacket.Expire)
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.Expire), expireBytes)

	// messageId
	ms.endian.PutUint64(messageIdBytes, uint64(msg.MessageID))
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.MessageId), messageIdBytes)

	// messageSeq
	messageSeqBytes := make([]byte, 8)
	ms.endian.PutUint64(messageSeqBytes, uint64(msg.MessageSeq))
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.MessageSeq), messageSeqBytes)

	// clientMsgNo
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.ClientMsgNo), []byte(msg.ClientMsgNo))

	// streamNo
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.StreamNo), []byte(msg.StreamNo))

	// timestamp
	timestampBytes := make([]byte, 4)
	ms.endian.PutUint32(timestampBytes, uint32(msg.Timestamp))
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.Timestamp), timestampBytes)

	// channelId
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.ChannelId), []byte(msg.ChannelID))

	// channelType
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.ChannelType), []byte{msg.ChannelType})

	// topic
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.Topic), []byte(msg.Topic))

	// fromUid
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.FromUid), []byte(msg.RecvPacket.FromUID))

	// payload
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.Payload), msg.Payload)

	// term
	termBytes := make([]byte, 8)
	ms.endian.PutUint64(termBytes, msg.Term)
	w.Set(key.NewMessageColumnKey(channelId, channelType, uint64(msg.MessageSeq), key.TableMessage.Column.Term), termBytes)

	var primaryValue = [16]byte{}
	ms.endian.PutUint64(primaryValue[:], key.ChannelToNum(channelId, channelType))
	ms.endian.PutUint64(primaryValue[8:], uint64(msg.MessageSeq))

	// index fromUid
	w.Set(key.NewMessageSecondIndexFromUidKey(msg.FromUID, primaryValue), nil)

	// index messageId
	w.Set(key.NewMessageIndexMessageIdKey(uint64(msg.MessageID)), primaryValue[:])

	// index clientMsgNo
	w.Set(key.NewMessageSecondIndexClientMsgNoKey(msg.ClientMsgNo, primaryValue), nil)

	// index timestamp
	w.Set(key.NewMessageIndexTimestampKey(uint64(msg.Timestamp), primaryValue), nil)

	return nil
}
