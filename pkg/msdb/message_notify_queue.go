package msdb

import (
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
	"go.uber.org/zap"
)

// AppendMessageOfNotifyQueue 添加消息到通知队列
func (ms *mushanDB) AppendMessageOfNotifyQueue(messages []Message) error {

	ms.metrics.AppendMessageOfNotifyQueueAdd(1)

	batch := ms.defaultShardBatchDB().NewBatch()
	for _, msg := range messages {
		if err := ms.writeMessageOfNotifyQueue(msg, batch); err != nil {
			return err
		}
	}
	return batch.CommitWait()
}

// GetMessagesOfNotifyQueue 获取通知队列的消息
func (ms *mushanDB) GetMessagesOfNotifyQueue(count int) ([]Message, error) {

	ms.metrics.GetMessagesOfNotifyQueueAdd(1)

	iter := ms.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewMessageNotifyQueueKey(0),
		UpperBound: key.NewMessageNotifyQueueKey(math.MaxUint64),
	})
	defer iter.Close()

	messages, errorKeys, err := ms.parseMessageOfNotifyQueue(iter, count)
	if err != nil {
		return nil, err
	}

	if len(errorKeys) > 0 {
		batch := ms.defaultShardDB().NewBatch()
		defer batch.Close()
		for _, key := range errorKeys {
			if err := batch.Delete(key, ms.noSync); err != nil {
				return nil, err
			}
		}
		if err := batch.Commit(ms.sync); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

// RemoveMessagesOfNotifyQueueCount 移除指定数量的通知队列的消息
func (ms *mushanDB) RemoveMessagesOfNotifyQueueCount(count int) error {

	iter := ms.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewMessageNotifyQueueKey(0),
		UpperBound: key.NewMessageNotifyQueueKey(math.MaxUint64),
	})
	defer iter.Close()

	batch := ms.defaultShardDB().NewBatch()
	defer batch.Close()

	for i := 0; iter.First() && i < count; i++ {
		if err := batch.Delete(iter.Key(), ms.noSync); err != nil {
			return err
		}
		iter.Next()
	}
	return batch.Commit(ms.sync)
}

// RemoveMessagesOfNotifyQueue 移除通知队列的消息
func (ms *mushanDB) RemoveMessagesOfNotifyQueue(messageIDs []int64) error {

	ms.metrics.RemoveMessagesOfNotifyQueueAdd(1)

	batch := ms.defaultShardDB().NewBatch()
	defer batch.Close()

	for _, messageID := range messageIDs {
		if err := batch.Delete(key.NewMessageNotifyQueueKey(uint64(messageID)), ms.noSync); err != nil {
			return err
		}
	}
	return batch.Commit(ms.sync)
}

func (ms *mushanDB) writeMessageOfNotifyQueue(msg Message, w *Batch) error {
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	w.Set(key.NewMessageNotifyQueueKey(uint64(msg.MessageID)), data)
	return nil
}

func (ms *mushanDB) parseMessageOfNotifyQueue(iter *pebble.Iterator, limit int) ([]Message, [][]byte, error) {

	msgs := make([]Message, 0, limit)
	errorKeys := make([][]byte, 0, limit)
	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()
		// 解析消息
		var msg Message
		if err := msg.Unmarshal(value); err != nil {
			ms.Warn("queue message unmarshal failed", zap.Error(err), zap.Int("len", len(value)))
			errorKeys = append(errorKeys, iter.Key())
			continue
		}
		msgs = append(msgs, msg)
		if limit > 0 && len(msgs) >= limit {
			return msgs, errorKeys, nil
		}
	}

	return msgs, errorKeys, nil
}
