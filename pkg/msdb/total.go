package msdb

import (
	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
)

func (ms *mushanDB) IncMessageCount(v int) error {
	ms.dblock.totalLock.lockMessageCount()
	defer ms.dblock.totalLock.unlockMessageCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Message)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	} else {
		result = make([]byte, 8)
	}
	count += v
	ms.endian.PutUint64(result, uint64(count))

	return db.Set(keyBytes, result, ms.sync)

}

func (ms *mushanDB) IncUserCount(v int) error {
	ms.dblock.totalLock.lockUserCount()
	defer ms.dblock.totalLock.unlockUserCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.User)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	} else {
		result = make([]byte, 8)
	}
	count += v
	ms.endian.PutUint64(result, uint64(count))

	return db.Set(keyBytes, result, ms.sync)
}

func (ms *mushanDB) IncDeviceCount(v int) error {
	ms.dblock.totalLock.lockDeviceCount()
	defer ms.dblock.totalLock.unlockDeviceCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Device)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	} else {
		result = make([]byte, 8)
	}
	count += v
	ms.endian.PutUint64(result, uint64(count))

	return db.Set(keyBytes, result, ms.sync)
}

func (ms *mushanDB) IncSessionCount(v int) error {
	ms.dblock.totalLock.lockSessionCount()
	defer ms.dblock.totalLock.unlockSessionCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Session)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	} else {
		result = make([]byte, 8)
	}
	count += v
	ms.endian.PutUint64(result, uint64(count))

	return db.Set(keyBytes, result, ms.sync)
}

func (ms *mushanDB) IncChannelCount(v int) error {
	ms.dblock.totalLock.lockChannelCount()
	defer ms.dblock.totalLock.unlockChannelCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Channel)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	} else {
		result = make([]byte, 8)
	}
	count += v
	ms.endian.PutUint64(result, uint64(count))

	return db.Set(keyBytes, result, ms.sync)
}

func (ms *mushanDB) IncConversationCount(v int) error {
	ms.dblock.totalLock.lockConversationCount()
	defer ms.dblock.totalLock.unlockConversationCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Conversation)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	} else {
		result = make([]byte, 8)
	}
	count += v
	ms.endian.PutUint64(result, uint64(count))

	return db.Set(keyBytes, result, ms.sync)
}

func (ms *mushanDB) GetTotalMessageCount() (int, error) {
	ms.dblock.totalLock.lockMessageCount()
	defer ms.dblock.totalLock.unlockMessageCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Message)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	}

	return count, nil
}

func (ms *mushanDB) GetTotalUserCount() (int, error) {
	ms.dblock.totalLock.lockUserCount()
	defer ms.dblock.totalLock.unlockUserCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.User)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	}

	return count, nil
}

func (ms *mushanDB) GetTotalDeviceCount() (int, error) {
	ms.dblock.totalLock.lockDeviceCount()
	defer ms.dblock.totalLock.unlockDeviceCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Device)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	}

	return count, nil
}

func (ms *mushanDB) GetTotalSessionCount() (int, error) {
	ms.dblock.totalLock.lockSessionCount()
	defer ms.dblock.totalLock.unlockSessionCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Session)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	}

	return count, nil

}

func (ms *mushanDB) GetTotalChannelCount() (int, error) {
	ms.dblock.totalLock.lockChannelCount()
	defer ms.dblock.totalLock.unlockChannelCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Channel)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	}

	return count, nil
}

func (ms *mushanDB) GetTotalConversationCount() (int, error) {
	ms.dblock.totalLock.lockConversationCount()
	defer ms.dblock.totalLock.unlockConversationCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.Conversation)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	}

	return count, nil
}

func (ms *mushanDB) IncChannelClusterConfigCount(v int) error {
	ms.dblock.totalLock.lockChannelClusterConfigCount()
	defer ms.dblock.totalLock.unlockChannelClusterConfigCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.ChannelClusterConfig)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	} else {
		result = make([]byte, 8)
	}
	count += v
	ms.endian.PutUint64(result, uint64(count))

	return db.Set(keyBytes, result, ms.sync)
}

func (ms *mushanDB) GetTotalChannelClusterConfigCount() (int, error) {
	ms.dblock.totalLock.lockChannelClusterConfigCount()
	defer ms.dblock.totalLock.unlockChannelClusterConfigCount()

	db := ms.defaultShardDB()

	keyBytes := key.NewTotalColumnKey(key.TableTotal.Column.ChannelClusterConfig)
	result, closer, err := db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	if closer != nil {
		defer closer.Close()
	}

	var count int
	if len(result) > 0 {
		count = int(ms.endian.Uint64(result))
	}

	return count, nil
}
