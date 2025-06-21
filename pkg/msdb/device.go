package msdb

import (
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
)

func (ms *mushanDB) getDeviceId(uid string, deviceFlag uint64) (uint64, error) {
	db := ms.shardDB(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.Uid, key.HashWithString(uid), 0),
		UpperBound: key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.Uid, key.HashWithString(uid), math.MaxUint64),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		_, id, err := key.ParseDeviceSecondIndexKey(iter.Key())
		if err != nil {
			return 0, err
		}
		device, err := ms.getDeviceById(id, db)
		if err != nil {
			return 0, err
		}

		if device.DeviceFlag == deviceFlag {
			return id, nil
		}
	}
	return 0, ErrNotFound
}

func (ms *mushanDB) getDeviceById(id uint64, db *pebble.DB) (Device, error) {
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceColumnKey(id, key.MinColumnKey),
		UpperBound: key.NewDeviceColumnKey(id, key.MaxColumnKey),
	})
	defer iter.Close()

	var device = EmptyDevice
	err := ms.iterDevice(iter, func(d Device) bool {
		device = d
		return false
	})
	if err != nil {
		return EmptyDevice, err
	}
	if IsEmptyDevice(device) {
		return EmptyDevice, ErrNotFound
	}

	return device, nil
}

func (ms *mushanDB) GetDevice(uid string, deviceFlag uint64) (Device, error) {

	ms.metrics.GetDeviceAdd(1)

	// 获取设备id
	id, err := ms.getDeviceId(uid, deviceFlag)
	if err != nil {
		return EmptyDevice, err
	}

	if id == 0 {
		return EmptyDevice, ErrNotFound
	}

	db := ms.shardDB(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceColumnKey(id, key.MinColumnKey),
		UpperBound: key.NewDeviceColumnKey(id, key.MaxColumnKey),
	})
	defer iter.Close()

	var device = EmptyDevice
	err = ms.iterDevice(iter, func(d Device) bool {
		if d.Id == id {
			device = d
			return false
		}
		return true
	})
	if err != nil {
		if err == pebble.ErrNotFound {
			return EmptyDevice, ErrNotFound
		}
		return EmptyDevice, err
	}

	if device == EmptyDevice {
		return EmptyDevice, ErrNotFound
	}
	return device, nil
}

func (ms *mushanDB) GetDevices(uid string) ([]Device, error) {

	ms.metrics.GetDevicesAdd(1)

	db := ms.shardDB(uid)
	uidHash := key.HashWithString(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceSecondIndexKey(key.TableDevice.Column.Uid, uidHash, 0),
		UpperBound: key.NewDeviceSecondIndexKey(key.TableDevice.Column.Uid, uidHash, math.MaxUint64),
	})
	defer iter.Close()

	var devices []Device
	for iter.First(); iter.Valid(); iter.Next() {
		_, id, err := key.ParseDeviceSecondIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}
		device, err := ms.getDeviceById(id, db)
		if err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return devices, nil
}

func (ms *mushanDB) GetDeviceCount(uid string) (int, error) {

	ms.metrics.GetDeviceCountAdd(1)

	db := ms.shardDB(uid)
	uidHash := key.HashWithString(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceSecondIndexKey(key.TableDevice.Column.Uid, uidHash, 0),
		UpperBound: key.NewDeviceSecondIndexKey(key.TableDevice.Column.Uid, uidHash, math.MaxUint64),
	})
	defer iter.Close()

	var count int
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count, nil
}

func (ms *mushanDB) AddDevice(d Device) error {

	ms.metrics.AddDeviceAdd(1)

	if d.Id == 0 {
		return ErrInvalidDeviceId
	}
	db := ms.shardDB(d.Uid)
	batch := db.NewBatch()
	defer batch.Close()
	err := ms.writeDevice(d, batch)
	if err != nil {
		return err
	}
	err = batch.Commit(ms.sync)
	if err != nil {
		return err
	}
	return nil
}

func (ms *mushanDB) UpdateDevice(d Device) error {

	ms.metrics.UpdateDeviceAdd(1)

	if d.Id == 0 {
		return ErrInvalidDeviceId
	}
	d.CreatedAt = nil // 更新时不更新创建时间

	db := ms.shardDB(d.Uid)
	// 获取旧设备信息
	old, err := ms.getDeviceById(d.Id, db)
	if err != nil && err != ErrNotFound {
		return err
	}

	if !IsEmptyDevice(old) {
		old.CreatedAt = nil // 不删除创建时间
		err = ms.deleteDeviceIndex(old, db)
		if err != nil {
			return err
		}
	}

	batch := db.NewBatch()
	defer batch.Close()
	err = ms.writeDevice(d, batch)
	if err != nil {
		return err
	}
	err = batch.Commit(ms.sync)
	if err != nil {
		return err
	}
	return nil
}

func (ms *mushanDB) SearchDevice(req DeviceSearchReq) ([]Device, error) {

	ms.metrics.SearchDeviceAdd(1)

	iterFnc := func(devices *[]Device) func(d Device) bool {
		currentSize := 0
		return func(d Device) bool {
			if req.Pre {
				if req.OffsetCreatedAt > 0 && d.CreatedAt != nil && d.CreatedAt.UnixNano() <= req.OffsetCreatedAt {
					return false
				}
			} else {
				if req.OffsetCreatedAt > 0 && d.CreatedAt != nil && d.CreatedAt.UnixNano() >= req.OffsetCreatedAt {
					return false
				}
			}

			if currentSize > req.Limit {
				return false
			}
			currentSize++
			*devices = append(*devices, d)

			return true
		}
	}

	allDevices := make([]Device, 0, req.Limit*len(ms.dbs))
	for _, db := range ms.dbs {
		devices := make([]Device, 0, req.Limit)
		fnc := iterFnc(&devices)

		has, err := ms.searchDeviceByIndex(req, fnc)
		if err != nil {
			return nil, err
		}
		if has { // 如果有触发索引，则无需全局查询
			allDevices = append(allDevices, devices...)
			continue
		}

		start := uint64(req.OffsetCreatedAt)
		end := uint64(math.MaxUint64)
		if req.OffsetCreatedAt > 0 {
			if req.Pre {
				start = uint64(req.OffsetCreatedAt + 1)
				end = uint64(math.MaxUint64)
			} else {
				start = 0
				end = uint64(req.OffsetCreatedAt)
			}
		}

		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.CreatedAt, start, 0),
			UpperBound: key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.CreatedAt, end, 0),
		})
		defer iter.Close()

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
			_, id, err := key.ParseDeviceSecondIndexKey(iter.Key())
			if err != nil {
				return nil, err
			}

			dataIter := db.NewIter(&pebble.IterOptions{
				LowerBound: key.NewDeviceColumnKey(id, key.MinColumnKey),
				UpperBound: key.NewDeviceColumnKey(id, key.MaxColumnKey),
			})
			defer dataIter.Close()

			var d Device
			err = ms.iterDevice(dataIter, func(device Device) bool {
				d = device
				return false
			})
			if err != nil {
				return nil, err
			}
			if !fnc(d) {
				break
			}
		}
		allDevices = append(allDevices, devices...)
	}
	// 降序排序
	sort.Slice(allDevices, func(i, j int) bool {
		return allDevices[i].CreatedAt.UnixNano() > allDevices[j].CreatedAt.UnixNano()
	})

	if req.Limit > 0 && len(allDevices) > req.Limit {
		if req.Pre {
			allDevices = allDevices[len(allDevices)-req.Limit:]
		} else {
			allDevices = allDevices[:req.Limit]
		}
	}

	return allDevices, nil
}

func (ms *mushanDB) searchDeviceByIndex(req DeviceSearchReq, iterFnc func(d Device) bool) (bool, error) {

	if req.Uid != "" && req.DeviceFlag != 0 {
		device, err := ms.GetDevice(req.Uid, req.DeviceFlag)
		if err != nil {
			return false, err
		}
		if !IsEmptyDevice(device) {
			iterFnc(device)
		}
		return true, nil
	}

	if req.Uid != "" {
		devices, err := ms.GetDevices(req.Uid)
		if err != nil {
			return false, err
		}
		for _, d := range devices {
			if !iterFnc(d) {
				return true, nil
			}
		}
		return true, nil
	}

	return false, nil
}

func (ms *mushanDB) writeDevice(d Device, w pebble.Writer) error {
	var (
		err error
	)
	// uid
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.Uid), []byte(d.Uid), ms.noSync); err != nil {
		return err
	}

	// token
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.Token), []byte(d.Token), ms.noSync); err != nil {
		return err
	}

	// deviceFlag
	var deviceFlagBytes = make([]byte, 8)
	ms.endian.PutUint64(deviceFlagBytes, d.DeviceFlag)
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.DeviceFlag), deviceFlagBytes, ms.noSync); err != nil {
		return err
	}

	// deviceLevel
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.DeviceLevel), []byte{d.DeviceLevel}, ms.noSync); err != nil {
		return err
	}
	// createdAt
	if d.CreatedAt != nil {
		ct := uint64(d.CreatedAt.UnixNano())
		createdAt := make([]byte, 8)
		ms.endian.PutUint64(createdAt, ct)
		if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.CreatedAt), createdAt, ms.noSync); err != nil {
			return err
		}

		// createdAt second index
		if err = w.Set(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.CreatedAt, ct, d.Id), nil, ms.noSync); err != nil {
			return err
		}

	}

	if d.UpdatedAt != nil {
		// updatedAt
		updatedAt := make([]byte, 8)
		ms.endian.PutUint64(updatedAt, uint64(d.UpdatedAt.UnixNano()))
		if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.UpdatedAt), updatedAt, ms.noSync); err != nil {
			return err
		}

		// updatedAt second index
		if err = w.Set(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.UpdatedAt, uint64(d.UpdatedAt.UnixNano()), d.Id), nil, ms.noSync); err != nil {
			return err
		}
	}

	// uid index
	if err = w.Set(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.Uid, key.HashWithString(d.Uid), d.Id), nil, ms.noSync); err != nil {
		return err
	}

	// deviceFlag index
	if err = w.Set(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.DeviceFlag, d.DeviceFlag, d.Id), nil, ms.noSync); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) deleteDeviceIndex(old Device, w pebble.Writer) error {
	// uid index
	if err := w.Delete(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.Uid, key.HashWithString(old.Uid), old.Id), ms.noSync); err != nil {
		return err
	}

	// deviceFlag index
	if err := w.Delete(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.DeviceFlag, old.DeviceFlag, old.Id), ms.noSync); err != nil {
		return err
	}

	// createdAt index
	if old.CreatedAt != nil {
		if err := w.Delete(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.CreatedAt, uint64(old.CreatedAt.UnixNano()), old.Id), ms.noSync); err != nil {
			return err
		}
	}

	// updatedAt index
	if old.UpdatedAt != nil {
		if err := w.Delete(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.UpdatedAt, uint64(old.UpdatedAt.UnixNano()), old.Id), ms.noSync); err != nil {
			return err
		}
	}

	return nil
}

// 解析出设备信息
// id !=0 时，解析出id对应的用户信息
func (ms *mushanDB) iterDevice(iter *pebble.Iterator, iterFnc func(d Device) bool) error {
	var (
		preId          uint64
		preDevice      Device
		lastNeedAppend bool = true
		hasData        bool = false
	)

	for iter.First(); iter.Valid(); iter.Next() {
		primaryKey, columnName, err := key.ParseDeviceColumnKey(iter.Key())
		if err != nil {
			return err
		}

		if preId != primaryKey {
			if preId != 0 {
				if !iterFnc(preDevice) {
					lastNeedAppend = false
					break
				}
			}
			preId = primaryKey
			preDevice = Device{Id: primaryKey}
		}

		switch columnName {
		case key.TableDevice.Column.Uid:
			preDevice.Uid = string(iter.Value())
		case key.TableDevice.Column.Token:
			preDevice.Token = string(iter.Value())
		case key.TableDevice.Column.DeviceFlag:
			preDevice.DeviceFlag = ms.endian.Uint64(iter.Value())
		case key.TableDevice.Column.DeviceLevel:
			preDevice.DeviceLevel = iter.Value()[0]
		case key.TableDevice.Column.CreatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preDevice.CreatedAt = &t
			}

		case key.TableDevice.Column.UpdatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preDevice.UpdatedAt = &t
			}

		}
		lastNeedAppend = true
		hasData = true
	}

	if lastNeedAppend && hasData {
		_ = iterFnc(preDevice)
	}
	return nil
}
