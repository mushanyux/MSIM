package msdb

import (
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
	"go.uber.org/zap"
)

func (ms *mushanDB) AddAllowlist(channelId string, channelType uint8, members []Member) error {

	ms.metrics.AddAllowlistAdd(1)

	db := ms.channelDb(channelId, channelType)

	channelPrimaryId, err := ms.getChannelPrimaryKey(channelId, channelType)
	if err != nil {
		return err
	}

	w := db.NewIndexedBatch()
	defer w.Close()
	for _, member := range members {
		member.Id = key.HashWithString(member.Uid)
		if err := ms.writeAllowlist(channelId, channelType, member, w); err != nil {
			return err
		}
	}

	err = ms.incChannelInfoAllowlistCount(channelPrimaryId, len(members), w)
	if err != nil {
		ms.Error("incChannelInfoAllowlistCount failed", zap.Error(err))
		return err
	}

	return w.Commit(ms.sync)
}

func (ms *mushanDB) GetAllowlist(channelId string, channelType uint8) ([]Member, error) {

	ms.metrics.GetAllowlistAdd(1)

	iter := ms.channelDb(channelId, channelType).NewIter(&pebble.IterOptions{
		LowerBound: key.NewAllowlistPrimaryKey(channelId, channelType, 0),
		UpperBound: key.NewAllowlistPrimaryKey(channelId, channelType, math.MaxUint64),
	})
	defer iter.Close()
	members := make([]Member, 0)
	err := ms.iterateAllowlist(iter, func(m Member) bool {
		members = append(members, m)
		return true
	})
	return members, err
}

func (ms *mushanDB) HasAllowlist(channelId string, channelType uint8) (bool, error) {
	ms.metrics.HasAllowlistAdd(1)

	iter := ms.channelDb(channelId, channelType).NewIter(&pebble.IterOptions{
		LowerBound: key.NewAllowlistPrimaryKey(channelId, channelType, 0),
		UpperBound: key.NewAllowlistPrimaryKey(channelId, channelType, math.MaxUint64),
	})
	defer iter.Close()
	var exist = false
	err := ms.iterateAllowlist(iter, func(m Member) bool {
		exist = true
		return false
	})
	return exist, err
}

func (ms *mushanDB) ExistAllowlist(channeId string, channelType uint8, uid string) (bool, error) {

	ms.metrics.ExistAllowlistAdd(1)

	uidIndexKey := key.NewAllowlistIndexKey(channeId, channelType, key.TableAllowlist.Index.Uid, key.HashWithString(uid))
	_, closer, err := ms.channelDb(channeId, channelType).Get(uidIndexKey)
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

func (ms *mushanDB) RemoveAllowlist(channelId string, channelType uint8, uids []string) error {

	ms.metrics.RemoveAllowlistAdd(1)

	db := ms.channelDb(channelId, channelType)

	channelPrimaryId, err := ms.getChannelPrimaryKey(channelId, channelType)
	if err != nil {
		return err
	}

	members, err := ms.getAllowlistByUids(channelId, channelType, uids)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil
		}
		return err
	}
	w := db.NewIndexedBatch()
	defer w.Close()
	for _, member := range members {
		if err := ms.removeAllowlist(channelId, channelType, member, w); err != nil {
			return err
		}
	}

	err = ms.incChannelInfoAllowlistCount(channelPrimaryId, -len(members), w)
	if err != nil {
		ms.Error("RemoveAllowlist: incChannelInfoAllowlistCount failed", zap.Error(err))
		return err
	}

	return w.Commit(ms.sync)
}

func (ms *mushanDB) RemoveAllAllowlist(channelId string, channelType uint8) error {

	ms.metrics.RemoveAllAllowlistAdd(1)

	db := ms.channelDb(channelId, channelType)

	channelPrimaryId, err := ms.getChannelPrimaryKey(channelId, channelType)
	if err != nil {
		return err
	}

	batch := db.NewIndexedBatch()
	defer batch.Close()

	// 删除数据
	err = batch.DeleteRange(key.NewAllowlistPrimaryKey(channelId, channelType, 0), key.NewAllowlistPrimaryKey(channelId, channelType, math.MaxUint64), ms.sync)
	if err != nil {
		return err
	}

	// 删除索引
	err = ms.deleteAllAllowlistIndex(channelId, channelType, batch)
	if err != nil {
		return err
	}

	// 白名单数量设置为0
	err = ms.incChannelInfoAllowlistCount(channelPrimaryId, 0, batch)
	if err != nil {
		ms.Error("RemoveAllAllowlist: incChannelInfoAllowlistCount failed", zap.Error(err))
		return err
	}

	return batch.Commit(ms.sync)
}

func (ms *mushanDB) removeAllowlist(channelId string, channelType uint8, member Member, w pebble.Writer) error {
	var (
		err error
	)
	// remove all column
	if err = w.DeleteRange(key.NewAllowlistColumnKey(channelId, channelType, member.Id, key.MinColumnKey), key.NewAllowlistColumnKey(channelId, channelType, member.Id, key.MaxColumnKey), ms.noSync); err != nil {
		return err
	}

	// delete index
	if err = ms.deleteAllowlistIndex(channelId, channelType, member, w); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) getAllowlistByUids(channelId string, channelType uint8, uids []string) ([]Member, error) {
	members := make([]Member, 0, len(uids))
	db := ms.channelDb(channelId, channelType)
	for _, uid := range uids {
		id := key.HashWithString(uid)
		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewAllowlistColumnKey(channelId, channelType, id, key.MinColumnKey),
			UpperBound: key.NewAllowlistColumnKey(channelId, channelType, id, key.MaxColumnKey),
		})
		defer iter.Close()

		err := ms.iterateAllowlist(iter, func(member Member) bool {
			members = append(members, member)
			return true
		})
		if err != nil {
			return nil, err
		}

	}
	return members, nil
}

func (ms *mushanDB) writeAllowlist(channelId string, channelType uint8, member Member, w pebble.Writer) error {
	var (
		err error
	)
	// uid
	if err = w.Set(key.NewAllowlistColumnKey(channelId, channelType, member.Id, key.TableAllowlist.Column.Uid), []byte(member.Uid), ms.noSync); err != nil {
		return err
	}

	// uid index
	idBytes := make([]byte, 8)
	ms.endian.PutUint64(idBytes, member.Id)
	if err = w.Set(key.NewAllowlistIndexKey(channelId, channelType, key.TableAllowlist.Index.Uid, key.HashWithString(member.Uid)), idBytes, ms.noSync); err != nil {
		return err
	}

	// createdAt
	if member.CreatedAt != nil {
		ct := uint64(member.CreatedAt.UnixNano())
		createdAt := make([]byte, 8)
		ms.endian.PutUint64(createdAt, ct)
		if err = w.Set(key.NewAllowlistColumnKey(channelId, channelType, member.Id, key.TableAllowlist.Column.CreatedAt), createdAt, ms.noSync); err != nil {
			return err
		}

		// createdAt second index
		if err = w.Set(key.NewAllowlistSecondIndexKey(channelId, channelType, key.TableAllowlist.SecondIndex.CreatedAt, ct, member.Id), nil, ms.noSync); err != nil {
			return err
		}

	}

	if member.UpdatedAt != nil {
		// updatedAt
		updatedAt := make([]byte, 8)
		ms.endian.PutUint64(updatedAt, uint64(member.UpdatedAt.UnixNano()))
		if err = w.Set(key.NewAllowlistColumnKey(channelId, channelType, member.Id, key.TableAllowlist.Column.UpdatedAt), updatedAt, ms.noSync); err != nil {
			return err
		}

		// updatedAt second index
		if err = w.Set(key.NewAllowlistSecondIndexKey(channelId, channelType, key.TableAllowlist.SecondIndex.UpdatedAt, uint64(member.UpdatedAt.UnixNano()), member.Id), nil, ms.noSync); err != nil {
			return err
		}
	}

	return nil

}

func (ms *mushanDB) iterateAllowlist(iter *pebble.Iterator, iterFnc func(member Member) bool) error {
	var (
		preId          uint64
		preMember      Member
		lastNeedAppend bool = true
		hasData        bool = false
	)
	for iter.First(); iter.Valid(); iter.Next() {
		id, columnName, err := key.ParseAllowlistColumnKey(iter.Key())
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
		case key.TableAllowlist.Column.Uid:
			preMember.Uid = string(iter.Value())
		case key.TableAllowlist.Column.CreatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preMember.CreatedAt = &t
			}

		case key.TableAllowlist.Column.UpdatedAt:
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

// 增加频道白名单数量
func (ms *mushanDB) incChannelInfoAllowlistCount(id uint64, count int, db *pebble.Batch) error {
	ms.dblock.allowlistCountLock.lock(id)
	defer ms.dblock.allowlistCountLock.unlock(id)
	return ms.incChannelInfoColumnCount(id, key.TableChannelInfo.Column.AllowlistCount, key.TableChannelInfo.SecondIndex.AllowlistCount, count, db)
}

func (ms *mushanDB) deleteAllAllowlistIndex(channelId string, channelType uint8, w pebble.Writer) error {
	var err error
	// uid index
	if err = w.DeleteRange(key.NewAllowlistIndexKey(channelId, channelType, key.TableAllowlist.Index.Uid, 0), key.NewAllowlistIndexKey(channelId, channelType, key.TableAllowlist.Index.Uid, math.MaxUint64), ms.noSync); err != nil {
		return err
	}

	// createdAt second index
	if err = w.DeleteRange(key.NewAllowlistSecondIndexKey(channelId, channelType, key.TableAllowlist.SecondIndex.CreatedAt, 0, 0), key.NewAllowlistSecondIndexKey(channelId, channelType, key.TableAllowlist.SecondIndex.CreatedAt, math.MaxUint64, 0), ms.noSync); err != nil {
		return err
	}

	// updatedAt second index
	if err = w.DeleteRange(key.NewAllowlistSecondIndexKey(channelId, channelType, key.TableAllowlist.SecondIndex.UpdatedAt, 0, 0), key.NewAllowlistSecondIndexKey(channelId, channelType, key.TableAllowlist.SecondIndex.UpdatedAt, math.MaxUint64, 0), ms.noSync); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) deleteAllowlistIndex(channelId string, channelType uint8, member Member, w pebble.Writer) error {
	var (
		err error
	)
	// uid index
	if err = w.Delete(key.NewAllowlistIndexKey(channelId, channelType, key.TableAllowlist.Index.Uid, member.Id), ms.noSync); err != nil {
		return err
	}

	// createdAt
	if member.CreatedAt != nil {
		ct := uint64(member.CreatedAt.UnixNano())
		if err = w.Delete(key.NewAllowlistSecondIndexKey(channelId, channelType, key.TableAllowlist.SecondIndex.CreatedAt, ct, member.Id), ms.noSync); err != nil {
			return err
		}

	}

	if member.UpdatedAt != nil {
		// updatedAt
		if err = w.Delete(key.NewAllowlistSecondIndexKey(channelId, channelType, key.TableAllowlist.SecondIndex.UpdatedAt, uint64(member.UpdatedAt.UnixNano()), member.Id), ms.noSync); err != nil {
			return err
		}
	}

	return nil
}
