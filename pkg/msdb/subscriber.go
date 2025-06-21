package msdb

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"go.uber.org/zap"
)

func (ms *mushanDB) AddSubscribers(channelId string, channelType uint8, subscribers []Member) error {

	ms.metrics.AddSubscribersAdd(1)

	db := ms.channelDb(channelId, channelType)

	channelPrimaryId, err := ms.getChannelPrimaryKey(channelId, channelType)
	if err != nil {
		return err
	}
	if channelPrimaryId == 0 {
		return fmt.Errorf("AddSubscribers: channelId: %s channelType: %d not found", channelId, channelType)
	}

	w := db.NewBatch()
	defer w.Close()

	for _, subscriber := range subscribers {
		id := key.HashWithString(subscriber.Uid)
		subscriber.Id = id
		if err := ms.writeSubscriber(channelId, channelType, subscriber, w); err != nil {
			return err
		}
	}

	return w.Commit(ms.sync)
}

func (ms *mushanDB) GetSubscribers(channelId string, channelType uint8) ([]Member, error) {

	ms.metrics.GetSubscribersAdd(1)

	iter := ms.channelDb(channelId, channelType).NewIter(&pebble.IterOptions{
		LowerBound: key.NewSubscriberColumnKey(channelId, channelType, 0, key.MinColumnKey),
		UpperBound: key.NewSubscriberColumnKey(channelId, channelType, math.MaxUint64, key.MaxColumnKey),
	})
	defer iter.Close()

	members := make([]Member, 0)
	err := ms.iterateSubscriber(iter, func(member Member) bool {
		members = append(members, member)
		return true
	})
	if err != nil {
		return nil, err
	}
	return members, nil
}

// 获取订阅者数量
func (ms *mushanDB) GetSubscriberCount(channelId string, channelType uint8) (int, error) {
	iter := ms.channelDb(channelId, channelType).NewIter(&pebble.IterOptions{
		LowerBound: key.NewSubscriberColumnKey(channelId, channelType, 0, key.TableSubscriber.Column.Uid),
		UpperBound: key.NewSubscriberColumnKey(channelId, channelType, math.MaxUint64, key.TableSubscriber.Column.Uid),
	})
	defer iter.Close()

	var count int
	err := ms.iterateSubscriber(iter, func(member Member) bool {
		count++
		return true
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (ms *mushanDB) RemoveSubscribers(channelId string, channelType uint8, subscribers []string) error {

	ms.metrics.RemoveSubscribersAdd(1)

	subscribers = msutil.RemoveRepeatedElement(subscribers) // 去重复

	// channelPrimaryId, err := ms.getChannelPrimaryKey(channelId, channelType)
	// if err != nil {
	// 	return err
	// }

	// 通过uids获取订阅者对象
	members, err := ms.getSubscribersByUids(channelId, channelType, subscribers)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil
		}
		return err
	}
	db := ms.channelDb(channelId, channelType)
	w := db.NewIndexedBatch()
	defer w.Close()
	for _, member := range members {
		if err := ms.removeSubscriber(channelId, channelType, member, w); err != nil {
			return err
		}
	}
	// err = ms.incChannelInfoSubscriberCount(channelPrimaryId, -len(members), w)
	// if err != nil {
	// 	ms.Error("RemoveSubscribers: incChannelInfoSubscriberCount failed", zap.Error(err))
	// 	return err
	// }
	return w.Commit(ms.sync)
}

func (ms *mushanDB) ExistSubscriber(channelId string, channelType uint8, uid string) (bool, error) {

	ms.metrics.ExistSubscriberAdd(1)

	uidIndexKey := key.NewSubscriberIndexKey(channelId, channelType, key.TableSubscriber.Index.Uid, key.HashWithString(uid))
	_, closer, err := ms.channelDb(channelId, channelType).Get(uidIndexKey)
	if closer != nil {
		defer closer.Close()
	}
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (ms *mushanDB) RemoveAllSubscriber(channelId string, channelType uint8) error {

	ms.metrics.RemoveAllSubscriberAdd(1)

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			cost := time.Since(start)
			if cost.Milliseconds() > 200 {
				ms.Info("RemoveAllSubscriber done", zap.Duration("cost", cost), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
			}
		}()
	}

	channelPrimaryId, err := ms.getChannelPrimaryKey(channelId, channelType)
	if err != nil {
		return err
	}
	if channelPrimaryId == 0 {
		return fmt.Errorf("RemoveAllSubscriber: channelId: %s channelType: %d not found", channelId, channelType)
	}

	db := ms.channelDb(channelId, channelType)
	batch := db.NewIndexedBatch()
	defer batch.Close()

	// 删除数据
	err = batch.DeleteRange(key.NewSubscriberColumnKey(channelId, channelType, 0, key.MinColumnKey), key.NewSubscriberColumnKey(channelId, channelType, math.MaxUint64, key.MaxColumnKey), ms.noSync)
	if err != nil {
		return err
	}

	// 删除索引
	err = ms.deleteAllSubscriberIndex(channelId, channelType, batch)
	if err != nil {
		return err
	}

	// // 订阅者数量设置为0
	// err = ms.incChannelInfoSubscriberCount(channelPrimaryId, 0, batch)
	// if err != nil {
	// 	ms.Error("RemoveAllSubscriber: incChannelInfoSubscriberCount failed", zap.Error(err))
	// 	return err
	// }

	return batch.Commit(ms.sync)
}

func (ms *mushanDB) removeSubscriber(channelId string, channelType uint8, member Member, w pebble.Writer) error {
	var (
		err error
	)
	// remove all column
	if err = w.DeleteRange(key.NewSubscriberColumnKey(channelId, channelType, member.Id, key.MinColumnKey), key.NewSubscriberColumnKey(channelId, channelType, member.Id, key.MaxColumnKey), ms.noSync); err != nil {
		return err
	}

	// delete index
	if err = ms.deleteSubscriberIndex(channelId, channelType, member, w); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) getSubscribersByUids(channelId string, channelType uint8, uids []string) ([]Member, error) {
	members := make([]Member, 0, len(uids))
	db := ms.channelDb(channelId, channelType)
	for _, uid := range uids {
		id := key.HashWithString(uid)

		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewSubscriberColumnKey(channelId, channelType, id, key.MinColumnKey),
			UpperBound: key.NewSubscriberColumnKey(channelId, channelType, id, key.MaxColumnKey),
		})
		defer iter.Close()

		err := ms.iterateSubscriber(iter, func(member Member) bool {
			members = append(members, member)
			return true
		})
		if err != nil {
			return nil, err
		}

	}
	return members, nil
}

// 增加频道白名单数量
// func (ms *mushanDB) incChannelInfoSubscriberCount(id uint64, count int, batch *Batch) error {
// 	ms.dblock.subscriberCountLock.lock(id)
// 	defer ms.dblock.subscriberCountLock.unlock(id)

// 	return ms.incChannelInfoColumnCount(id, key.TableChannelInfo.Column.SubscriberCount, key.TableChannelInfo.SecondIndex.SubscriberCount, count, batch)
// }

func (ms *mushanDB) iterateSubscriber(iter *pebble.Iterator, iterFnc func(member Member) bool) error {

	var (
		preId          uint64
		preMember      Member
		lastNeedAppend bool = true
		hasData        bool = false
	)

	for iter.First(); iter.Valid(); iter.Next() {
		id, columnName, err := key.ParseSubscriberColumnKey(iter.Key())
		if err != nil {
			return err
		}

		if id != preId {
			if preId != 0 {
				if !iterFnc(preMember) {
					lastNeedAppend = false
					break
				}
			}
			preId = id
			preMember = Member{
				Id: id,
			}
		}

		switch columnName {
		case key.TableSubscriber.Column.Uid:
			preMember.Uid = string(iter.Value())
		case key.TableSubscriber.Column.CreatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preMember.CreatedAt = &t
			}

		case key.TableSubscriber.Column.UpdatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preMember.UpdatedAt = &t
			}
		}
		hasData = true
	}
	if lastNeedAppend && hasData {
		_ = iterFnc(preMember)
	}
	return nil
}

func (ms *mushanDB) writeSubscriber(channelId string, channelType uint8, member Member, w pebble.Writer) error {

	if member.Id == 0 {
		return errors.New("writeSubscriber: member.Id is 0")
	}
	var err error
	// uid
	if err = w.Set(key.NewSubscriberColumnKey(channelId, channelType, member.Id, key.TableSubscriber.Column.Uid), []byte(member.Uid), ms.noSync); err != nil {
		return err
	}

	// uid index
	idBytes := make([]byte, 8)
	ms.endian.PutUint64(idBytes, member.Id)
	if err = w.Set(key.NewSubscriberIndexKey(channelId, channelType, key.TableSubscriber.Index.Uid, member.Id), idBytes, ms.noSync); err != nil {
		return err
	}

	// createdAt
	if member.CreatedAt != nil {
		ct := uint64(member.CreatedAt.UnixNano())
		createdAt := make([]byte, 8)
		ms.endian.PutUint64(createdAt, ct)
		if err := w.Set(key.NewSubscriberColumnKey(channelId, channelType, member.Id, key.TableSubscriber.Column.CreatedAt), createdAt, ms.noSync); err != nil {
			return err
		}

		// createdAt second index
		if err := w.Set(key.NewSubscriberSecondIndexKey(channelId, channelType, key.TableSubscriber.SecondIndex.CreatedAt, ct, member.Id), nil, ms.noSync); err != nil {
			return err
		}

	}

	if member.UpdatedAt != nil {
		// updatedAt
		updatedAt := make([]byte, 8)
		ms.endian.PutUint64(updatedAt, uint64(member.UpdatedAt.UnixNano()))
		if err = w.Set(key.NewSubscriberColumnKey(channelId, channelType, member.Id, key.TableSubscriber.Column.UpdatedAt), updatedAt, ms.noSync); err != nil {
			return err
		}
		// updatedAt second index
		if err = w.Set(key.NewSubscriberSecondIndexKey(channelId, channelType, key.TableSubscriber.SecondIndex.UpdatedAt, uint64(member.UpdatedAt.UnixNano()), member.Id), nil, ms.noSync); err != nil {
			return err
		}
	}
	return nil
}

func (ms *mushanDB) deleteAllSubscriberIndex(channelId string, channelType uint8, w pebble.Writer) error {

	var err error
	// uid index
	if err = w.DeleteRange(key.NewSubscriberIndexKey(channelId, channelType, key.TableSubscriber.Index.Uid, 0), key.NewSubscriberIndexKey(channelId, channelType, key.TableSubscriber.Index.Uid, math.MaxUint64), ms.noSync); err != nil {
		return err
	}

	// createdAt second index
	if err = w.DeleteRange(key.NewSubscriberSecondIndexKey(channelId, channelType, key.TableSubscriber.SecondIndex.CreatedAt, 0, 0), key.NewSubscriberSecondIndexKey(channelId, channelType, key.TableSubscriber.SecondIndex.CreatedAt, math.MaxUint64, 0), ms.noSync); err != nil {
		return err
	}

	// updatedAt second index
	if err = w.DeleteRange(key.NewSubscriberSecondIndexKey(channelId, channelType, key.TableSubscriber.SecondIndex.UpdatedAt, 0, 0), key.NewSubscriberSecondIndexKey(channelId, channelType, key.TableSubscriber.SecondIndex.UpdatedAt, math.MaxUint64, 0), ms.noSync); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) deleteSubscriberIndex(channelId string, channelType uint8, oldMember Member, w pebble.Writer) error {
	var err error
	// uid index
	if err = w.Delete(key.NewSubscriberIndexKey(channelId, channelType, key.TableSubscriber.Index.Uid, key.HashWithString(oldMember.Uid)), ms.noSync); err != nil {
		return err
	}

	// createdAt second index
	if oldMember.CreatedAt != nil {
		if err = w.Delete(key.NewSubscriberSecondIndexKey(channelId, channelType, key.TableSubscriber.SecondIndex.CreatedAt, uint64(oldMember.CreatedAt.UnixNano()), oldMember.Id), ms.noSync); err != nil {
			return err
		}
	}

	// updatedAt second index
	if oldMember.UpdatedAt != nil {
		if err = w.Delete(key.NewSubscriberSecondIndexKey(channelId, channelType, key.TableSubscriber.SecondIndex.UpdatedAt, uint64(oldMember.UpdatedAt.UnixNano()), oldMember.Id), ms.noSync); err != nil {
			return err
		}
	}

	return nil
}
