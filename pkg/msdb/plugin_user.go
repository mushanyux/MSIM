package msdb

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
)

func (ms *mushanDB) AddOrUpdatePluginUsers(pluginUsers []PluginUser) error {
	db := ms.defaultShardDB()
	batch := db.NewBatch()
	defer batch.Close()

	for _, pluginUser := range pluginUsers {
		if err := ms.writePluginUser(pluginUser, batch); err != nil {
			return err
		}
	}
	return batch.Commit(ms.sync)
}

func (ms *mushanDB) RemovePluginUser(pluginNo string, uid string) error {
	db := ms.defaultShardDB()
	batch := db.NewBatch()
	defer batch.Close()
	id := ms.getPluginId(pluginNo, uid)
	err := batch.DeleteRange(key.NewPluginUserColumnKey(id, key.MinColumnKey), key.NewPluginUserColumnKey(id, key.MaxColumnKey), ms.noSync)
	if err != nil {
		return err
	}
	err = ms.deletePluginUserSecondIndex(id, pluginNo, uid, batch)
	if err != nil {
		return err
	}
	return batch.Commit(ms.sync)
}

func (ms *mushanDB) getPluginId(pluginNo, uid string) uint64 {
	return key.HashWithString(fmt.Sprintf("%s_%s", pluginNo, uid))
}

func (ms *mushanDB) GetPluginUsers(pluginNo string) ([]PluginUser, error) {
	db := ms.defaultShardDB()
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.PluginNo, key.HashWithString(pluginNo), 0),
		UpperBound: key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.PluginNo, key.HashWithString(pluginNo), math.MaxUint64),
	})
	defer iter.Close()

	var pluginUsers []PluginUser
	for iter.First(); iter.Valid(); iter.Next() {
		_, id, err := key.ParsePluginUserSecondIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}
		pluginUser, err := ms.getPluginUserById(id)
		if err != nil {
			return nil, err
		}
		pluginUsers = append(pluginUsers, pluginUser)
	}
	return pluginUsers, nil
}

func (ms *mushanDB) SearchPluginUsers(req SearchPluginUserReq) ([]PluginUser, error) {
	if req.Uid != "" && req.PluginNo != "" {
		id := ms.getPluginId(req.PluginNo, req.Uid)
		pluginUser, err := ms.getPluginUserById(id)
		if err != nil {
			return nil, err
		}
		return []PluginUser{pluginUser}, nil
	}

	if req.Uid != "" {
		return ms.searchPluginUsersByUid(req.Uid)
	}

	if req.PluginNo != "" {
		pluginUsers, err := ms.GetPluginUsers(req.PluginNo)
		if err != nil {
			return nil, err
		}
		return pluginUsers, nil
	}

	db := ms.defaultShardDB()
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewPluginUserColumnKey(0, key.MinColumnKey),
		UpperBound: key.NewPluginUserColumnKey(math.MaxUint64, key.MaxColumnKey),
	})
	defer iter.Close()

	pluginUsers := make([]PluginUser, 0)
	if err := ms.iteratorPluginUser(iter, func(u PluginUser) bool {
		pluginUsers = append(pluginUsers, u)
		return true
	}); err != nil {
		return nil, err
	}

	return pluginUsers, nil

}

func (ms *mushanDB) searchPluginUsersByUid(uid string) ([]PluginUser, error) {
	db := ms.defaultShardDB()
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.Uid, key.HashWithString(uid), 0),
		UpperBound: key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.Uid, key.HashWithString(uid), math.MaxUint64),
	})
	defer iter.Close()

	pluginUsers := make([]PluginUser, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		_, id, err := key.ParsePluginUserSecondIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}
		pluginUser, err := ms.getPluginUserById(id)
		if err != nil {
			return nil, err
		}
		pluginUsers = append(pluginUsers, pluginUser)
	}

	return pluginUsers, nil
}

func (ms *mushanDB) getPluginUserById(id uint64) (PluginUser, error) {
	db := ms.defaultShardDB()
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewPluginUserColumnKey(id, key.MinColumnKey),
		UpperBound: key.NewPluginUserColumnKey(id, key.MaxColumnKey),
	})
	defer iter.Close()

	var pluginUser PluginUser
	if err := ms.iteratorPluginUser(iter, func(u PluginUser) bool {
		pluginUser = u
		return false
	}); err != nil {
		return PluginUser{}, err
	}
	return pluginUser, nil
}

// GetHighestPriorityPluginByUid 获取用户最高优先级的插件
func (ms *mushanDB) GetHighestPriorityPluginByUid(uid string) (string, error) {
	db := ms.defaultShardDB()
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.Uid, key.HashWithString(uid), 0),
		UpperBound: key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.Uid, key.HashWithString(uid), math.MaxUint64),
	})
	defer iter.Close()

	var plugin Plugin
	var maxPriority uint32
	for iter.First(); iter.Valid(); iter.Next() {
		_, id, err := key.ParsePluginUserSecondIndexKey(iter.Key())
		if err != nil {
			return "", err
		}
		pluginNo, closer, err := db.Get(key.NewPluginUserColumnKey(id, key.TablePluginUser.Column.PluginNo))
		if closer != nil {
			defer closer.Close()
		}
		if err != nil {
			return "", err
		}
		p, err := ms.GetPlugin(string(pluginNo))
		if err != nil {
			return "", err
		}
		if plugin.No == "" || p.Priority > maxPriority {
			maxPriority = p.Priority
			plugin = p
		}
	}
	return plugin.No, nil
}

func (ms *mushanDB) ExistPluginByUid(uid string) (bool, error) {
	db := ms.defaultShardDB()
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.Uid, key.HashWithString(uid), 0),
		UpperBound: key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.Uid, key.HashWithString(uid), math.MaxUint64),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		return true, nil
	}
	return false, nil
}

func (ms *mushanDB) writePluginUser(p PluginUser, w pebble.Writer) error {
	id := ms.getPluginId(p.PluginNo, p.Uid)
	var err error
	// pluginNo
	if err = w.Set(key.NewPluginUserColumnKey(id, key.TablePluginUser.Column.PluginNo), []byte(p.PluginNo), ms.noSync); err != nil {
		return err
	}

	// uid
	if err = w.Set(key.NewPluginUserColumnKey(id, key.TablePluginUser.Column.Uid), []byte(p.Uid), ms.noSync); err != nil {
		return err
	}

	// uid second index
	if err = w.Set(key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.Uid, key.HashWithString(p.Uid), id), nil, ms.noSync); err != nil {
		return err
	}

	// pluginNo second index
	if err = w.Set(key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.PluginNo, key.HashWithString(p.PluginNo), id), nil, ms.noSync); err != nil {
		return err
	}

	// createdAt
	if p.CreatedAt != nil {
		createdAtBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(createdAtBytes, uint64(p.CreatedAt.UnixNano()))
		if err = w.Set(key.NewPluginUserColumnKey(id, key.TablePluginUser.Column.CreatedAt), createdAtBytes, ms.noSync); err != nil {
			return err
		}
	}

	// updatedAt
	if p.UpdatedAt != nil {
		updatedAtBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(updatedAtBytes, uint64(p.UpdatedAt.UnixNano()))
		if err = w.Set(key.NewPluginUserColumnKey(id, key.TablePluginUser.Column.UpdatedAt), updatedAtBytes, ms.noSync); err != nil {
			return err
		}
	}

	return nil
}

func (ms *mushanDB) iteratorPluginUser(iter *pebble.Iterator, iterFnc func(PluginUser) bool) error {
	var (
		preId          uint64
		prePlugin      PluginUser
		lastNeedAppend bool = true
		hasData        bool = false
	)

	for iter.First(); iter.Valid(); iter.Next() {
		primaryKey, columnName, err := key.ParsePluginUserColumnKey(iter.Key())
		if err != nil {
			return err
		}

		if preId != primaryKey {
			if preId != 0 {
				if !iterFnc(prePlugin) {
					lastNeedAppend = false
					break
				}
			}
			preId = primaryKey
			prePlugin = PluginUser{}
		}

		switch columnName {
		case key.TablePluginUser.Column.PluginNo:
			prePlugin.PluginNo = string(iter.Value())
		case key.TablePluginUser.Column.Uid:
			prePlugin.Uid = string(iter.Value())
		case key.TablePluginUser.Column.CreatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				prePlugin.CreatedAt = &t
			}
		case key.TablePluginUser.Column.UpdatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				prePlugin.UpdatedAt = &t
			}
		}
		lastNeedAppend = true
		hasData = true
	}

	if lastNeedAppend && hasData {
		_ = iterFnc(prePlugin)
	}
	return nil
}

func (ms *mushanDB) deletePluginUserSecondIndex(id uint64, pluginNo string, uid string, w pebble.Writer) error {
	if err := w.Delete(key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.Uid, key.HashWithString(uid), id), ms.noSync); err != nil {
		return err
	}

	if err := w.Delete(key.NewPluginUserSecondIndexKey(key.TablePluginUser.SecondIndex.PluginNo, key.HashWithString(pluginNo), id), ms.noSync); err != nil {
		return err
	}

	return nil
}
