package msdb

import (
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
)

func (ms *mushanDB) AddSystemUids(uids []string) error {

	ms.metrics.AddSystemUidsAdd(1)

	w := ms.defaultShardDB().NewBatch()
	defer w.Close()
	for _, uid := range uids {
		id := key.HashWithString(uid)
		if err := ms.writeSystemUid(id, uid, w); err != nil {
			return err
		}
	}
	return w.Commit(ms.sync)
}

func (ms *mushanDB) RemoveSystemUids(uids []string) error {

	ms.metrics.RemoveSystemUidsAdd(1)

	w := ms.defaultShardDB().NewBatch()
	defer w.Close()
	for _, uid := range uids {
		id := key.HashWithString(uid)
		if err := ms.removeSystemUid(id, w); err != nil {
			return err
		}
	}
	return w.Commit(ms.sync)
}

func (ms *mushanDB) GetSystemUids() ([]string, error) {

	ms.metrics.GetSystemUidsAdd(1)

	iter := ms.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewSystemUidColumnKey(0, key.TableSystemUid.Column.Uid),
		UpperBound: key.NewSystemUidColumnKey(math.MaxUint64, key.TableSystemUid.Column.Uid),
	})
	defer iter.Close()
	return ms.parseSystemUid(iter)
}

func (ms *mushanDB) writeSystemUid(id uint64, uid string, w *pebble.Batch) error {
	return w.Set(key.NewSystemUidColumnKey(id, key.TableSystemUid.Column.Uid), []byte(uid), ms.noSync)
}

func (ms *mushanDB) removeSystemUid(id uint64, w *pebble.Batch) error {
	return w.Delete(key.NewSystemUidColumnKey(id, key.TableSystemUid.Column.Uid), ms.noSync)
}

func (ms *mushanDB) parseSystemUid(iter *pebble.Iterator) ([]string, error) {
	var uids []string
	for iter.First(); iter.Valid(); iter.Next() {
		uids = append(uids, string(iter.Value()))
	}
	return uids, nil
}
