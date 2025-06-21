package msdb

import (
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"go.uber.org/zap"
)

func (ms *mushanDB) AddChannel(channelInfo ChannelInfo) (uint64, error) {
	ms.metrics.AddChannelAdd(1)

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			cost := time.Since(start)
			if cost.Milliseconds() > 200 {
				ms.Info("AddChannel done", zap.Duration("cost", cost), zap.String("channelId", channelInfo.ChannelId), zap.Uint8("channelType", channelInfo.ChannelType))
			}
		}()
	}

	primaryKey, err := ms.getChannelPrimaryKey(channelInfo.ChannelId, channelInfo.ChannelType)
	if err != nil {
		return 0, err
	}

	w := ms.channelDb(channelInfo.ChannelId, channelInfo.ChannelType).NewBatch()
	defer w.Close()

	if err := ms.writeChannelInfo(primaryKey, channelInfo, w); err != nil {
		return 0, err
	}

	return primaryKey, w.Commit(ms.sync)
}

func (ms *mushanDB) UpdateChannel(channelInfo ChannelInfo) error {
	ms.metrics.UpdateChannelAdd(1)

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			cost := time.Since(start)
			if cost.Milliseconds() > 200 {
				ms.Info("UpdateChannel done", zap.Duration("cost", cost), zap.String("channelId", channelInfo.ChannelId), zap.Uint8("channelType", channelInfo.ChannelType))
			}
		}()
	}

	primaryKey, err := ms.getChannelPrimaryKey(channelInfo.ChannelId, channelInfo.ChannelType)
	if err != nil {
		return err
	}

	w := ms.channelDb(channelInfo.ChannelId, channelInfo.ChannelType).NewBatch()
	defer w.Close()

	oldChannelInfo, err := ms.GetChannel(channelInfo.ChannelId, channelInfo.ChannelType)
	if err != nil && err != ErrNotFound {
		return err
	}

	if !IsEmptyChannelInfo(oldChannelInfo) {
		// 更新操作不需要更新创建时间
		if oldChannelInfo.CreatedAt != nil {
			oldChannelInfo.CreatedAt = nil
		}
		// 删除旧索引
		if err := ms.deleteChannelInfoBaseIndex(oldChannelInfo, w); err != nil {
			return err
		}
	}

	if channelInfo.CreatedAt != nil {
		channelInfo.CreatedAt = nil
	}
	if err := ms.writeChannelInfo(primaryKey, channelInfo, w); err != nil {
		return err
	}

	return w.Commit(ms.sync)
}

func (ms *mushanDB) GetChannel(channelId string, channelType uint8) (ChannelInfo, error) {
	ms.metrics.GetChannelAdd(1)

	id, err := ms.getChannelPrimaryKey(channelId, channelType)
	if err != nil {
		return EmptyChannelInfo, err
	}
	if id == 0 {
		return EmptyChannelInfo, nil
	}

	iter := ms.channelDb(channelId, channelType).NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelInfoColumnKey(id, key.MinColumnKey),
		UpperBound: key.NewChannelInfoColumnKey(id, key.MaxColumnKey),
	})
	defer iter.Close()

	var channelInfos []ChannelInfo
	err = ms.iterChannelInfo(iter, func(channelInfo ChannelInfo) bool {
		channelInfos = append(channelInfos, channelInfo)
		return true
	})
	if err != nil {
		return EmptyChannelInfo, err
	}
	if len(channelInfos) == 0 {
		return EmptyChannelInfo, nil

	}
	return channelInfos[0], nil
}

func (ms *mushanDB) SearchChannels(req ChannelSearchReq) ([]ChannelInfo, error) {
	ms.metrics.SearchChannelsAdd(1)

	var channelInfos []ChannelInfo
	if req.ChannelId != "" && req.ChannelType != 0 {
		channelInfo, err := ms.GetChannel(req.ChannelId, req.ChannelType)
		if err != nil {
			return nil, err
		}
		channelInfos = append(channelInfos, channelInfo)
		return channelInfos, nil
	}

	iterFnc := func(channelInfos *[]ChannelInfo) func(channelInfo ChannelInfo) bool {
		currentSize := 0
		return func(channelInfo ChannelInfo) bool {
			if req.ChannelId != "" && req.ChannelId != channelInfo.ChannelId {
				return true
			}
			if req.ChannelType != 0 && req.ChannelType != channelInfo.ChannelType {
				return true
			}
			if req.Ban != nil && *req.Ban != channelInfo.Ban {
				return true
			}
			if req.Disband != nil && *req.Disband != channelInfo.Disband {
				return true
			}
			if req.Pre {
				if req.OffsetCreatedAt > 0 && channelInfo.CreatedAt != nil && channelInfo.CreatedAt.UnixNano() <= req.OffsetCreatedAt {
					return false
				}
			} else {
				if req.OffsetCreatedAt > 0 && channelInfo.CreatedAt != nil && channelInfo.CreatedAt.UnixNano() >= req.OffsetCreatedAt {
					return false
				}
			}

			if currentSize > req.Limit {
				return false
			}
			currentSize++
			*channelInfos = append(*channelInfos, channelInfo)

			return true
		}
	}

	allChannelInfos := make([]ChannelInfo, 0, req.Limit*len(ms.dbs))
	for _, db := range ms.dbs {
		channelInfos := make([]ChannelInfo, 0, req.Limit)
		fnc := iterFnc(&channelInfos)
		// 通过索引查询
		has, err := ms.searchChannelsByIndex(req, db, fnc)
		if err != nil {
			return nil, err
		}
		if has { // 如果有触发索引，则无需全局查询
			allChannelInfos = append(allChannelInfos, channelInfos...)
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
			LowerBound: key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.CreatedAt, start, 0),
			UpperBound: key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.CreatedAt, end, 0),
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
			_, id, err := key.ParseChannelInfoSecondIndexKey(iter.Key())
			if err != nil {
				return nil, err
			}

			dataIter := db.NewIter(&pebble.IterOptions{
				LowerBound: key.NewChannelInfoColumnKey(id, key.MinColumnKey),
				UpperBound: key.NewChannelInfoColumnKey(id, key.MaxColumnKey),
			})
			defer dataIter.Close()

			var ch ChannelInfo
			err = ms.iterChannelInfo(dataIter, func(channelInfo ChannelInfo) bool {
				ch = channelInfo
				return false
			})
			if err != nil {
				return nil, err
			}
			if !fnc(ch) {
				break
			}
		}

		allChannelInfos = append(allChannelInfos, channelInfos...)
	}

	// 降序排序
	sort.Slice(allChannelInfos, func(i, j int) bool {
		return allChannelInfos[i].CreatedAt.UnixNano() > allChannelInfos[j].CreatedAt.UnixNano()
	})

	if req.Limit > 0 && len(allChannelInfos) > req.Limit {
		if req.Pre {
			allChannelInfos = allChannelInfos[len(allChannelInfos)-req.Limit:]
		} else {
			allChannelInfos = allChannelInfos[:req.Limit]
		}
	}

	var (
		count int
	)
	for i, result := range allChannelInfos {

		// 获取频道的最新消息序号
		lastMsgSeq, lastTime, err := ms.GetChannelLastMessageSeq(result.ChannelId, result.ChannelType)
		if err != nil {
			return nil, err
		}
		allChannelInfos[i].LastMsgSeq = lastMsgSeq
		allChannelInfos[i].LastMsgTime = lastTime

		// 订阅数量
		count, err = ms.GetSubscriberCount(result.ChannelId, result.ChannelType)
		if err != nil {
			return nil, err
		}
		allChannelInfos[i].SubscriberCount = count

	}

	return allChannelInfos, nil
}

func (ms *mushanDB) searchChannelsByIndex(req ChannelSearchReq, db *pebble.DB, iterFnc func(ch ChannelInfo) bool) (bool, error) {
	var lomsey []byte
	var highKey []byte

	var existKey = false

	if req.Ban != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.Ban, uint64(msutil.BoolToInt(*req.Ban)), 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.Ban, uint64(msutil.BoolToInt(*req.Ban)), math.MaxUint64)
		existKey = true
	}

	if !existKey && req.Disband != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.Disband, uint64(msutil.BoolToInt(*req.Disband)), 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.Disband, uint64(msutil.BoolToInt(*req.Disband)), math.MaxUint64)
		existKey = true
	}

	if !existKey && req.SubscriberCountGte != nil && req.SubscriberCountLte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.SubscriberCount, uint64(*req.SubscriberCountGte), 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.SubscriberCount, uint64(*req.SubscriberCountLte), math.MaxUint64)
		existKey = true
	}

	if !existKey && req.SubscriberCountLte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.SubscriberCount, 0, 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.SubscriberCount, uint64(*req.SubscriberCountLte), math.MaxUint64)
		existKey = true
	}

	if !existKey && req.SubscriberCountGte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.SubscriberCount, uint64(*req.SubscriberCountGte), 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.SubscriberCount, math.MaxUint64, math.MaxUint64)
		existKey = true
	}

	if !existKey && req.AllowlistCountGte != nil && req.AllowlistCountLte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.AllowlistCount, uint64(*req.AllowlistCountGte), 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.AllowlistCount, uint64(*req.AllowlistCountLte), math.MaxUint64)
		existKey = true
	}

	if !existKey && req.AllowlistCountLte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.AllowlistCount, 0, 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.AllowlistCount, uint64(*req.AllowlistCountLte), math.MaxUint64)
		existKey = true
	}

	if !existKey && req.AllowlistCountGte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.AllowlistCount, uint64(*req.AllowlistCountGte), 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.AllowlistCount, math.MaxUint64, math.MaxUint64)
		existKey = true
	}

	if !existKey && req.DenylistCountGte != nil && req.DenylistCountLte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.DenylistCount, uint64(*req.DenylistCountGte), 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.DenylistCount, uint64(*req.DenylistCountLte), math.MaxUint64)
		existKey = true
	}

	if !existKey && req.DenylistCountLte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.DenylistCount, 0, 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.DenylistCount, uint64(*req.DenylistCountLte), math.MaxUint64)
		existKey = true
	}
	if !existKey && req.DenylistCountGte != nil {
		lomsey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.DenylistCount, uint64(*req.DenylistCountGte), 0)
		highKey = key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.DenylistCount, math.MaxUint64, math.MaxUint64)
		existKey = true
	}

	if !existKey {
		return false, nil
	}

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: lomsey,
		UpperBound: highKey,
	})

	for iter.Last(); iter.Valid(); iter.Prev() {
		_, id, err := key.ParseChannelInfoSecondIndexKey(iter.Key())
		if err != nil {
			return false, err
		}

		dataIter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewChannelInfoColumnKey(id, key.MinColumnKey),
			UpperBound: key.NewChannelInfoColumnKey(id, key.MaxColumnKey),
		})
		defer dataIter.Close()

		var ch ChannelInfo
		err = ms.iterChannelInfo(dataIter, func(channelInfo ChannelInfo) bool {
			ch = channelInfo
			return false
		})
		if err != nil {
			return false, err
		}
		if iterFnc != nil {
			if !iterFnc(ch) {
				break
			}
		}

	}
	return true, nil
}

func (ms *mushanDB) ExistChannel(channelId string, channelType uint8) (bool, error) {

	ms.metrics.ExistChannelAdd(1)

	channel, err := ms.GetChannel(channelId, channelType)
	if err != nil {
		if err == ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return !IsEmptyChannelInfo(channel), nil
}

func (ms *mushanDB) DeleteChannel(channelId string, channelType uint8) error {

	ms.metrics.DeleteChannelAdd(1)

	id, err := ms.getChannelPrimaryKey(channelId, channelType)
	if err != nil {
		return err
	}
	if id == 0 {
		return nil
	}
	batch := ms.channelDb(channelId, channelType).NewBatch()
	defer batch.Close()

	// 删除数据
	err = batch.DeleteRange(key.NewChannelInfoColumnKey(id, key.MinColumnKey), key.NewChannelInfoColumnKey(id, key.MaxColumnKey), ms.noSync)
	if err != nil {
		return err
	}

	err = batch.DeleteRange(key.NewChannelInfoSecondIndexKey(key.MinColumnKey, 0, id), key.NewChannelInfoSecondIndexKey(key.MaxColumnKey, math.MaxUint64, id), ms.noSync)
	if err != nil {
		return err
	}

	err = ms.IncChannelCount(-1)
	if err != nil {
		return err
	}

	return batch.Commit(ms.sync)
}

func (ms *mushanDB) UpdateChannelAppliedIndex(channelId string, channelType uint8, index uint64) error {

	ms.metrics.UpdateChannelAppliedIndexAdd(1)

	indexBytes := make([]byte, 8)
	ms.endian.PutUint64(indexBytes, index)
	return ms.channelDb(channelId, channelType).Set(key.NewChannelCommonColumnKey(channelId, channelType, key.TableChannelCommon.Column.AppliedIndex), indexBytes, ms.sync)
}

func (ms *mushanDB) GetChannelAppliedIndex(channelId string, channelType uint8) (uint64, error) {

	ms.metrics.GetChannelAppliedIndexAdd(1)

	data, closer, err := ms.channelDb(channelId, channelType).Get(key.NewChannelCommonColumnKey(channelId, channelType, key.TableChannelCommon.Column.AppliedIndex))
	if closer != nil {
		defer closer.Close()
	}
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	return ms.endian.Uint64(data), nil
}

// 增加频道属性数量 id为频道信息的唯一主键 count为math.MinInt 表示重置为0
func (ms *mushanDB) incChannelInfoColumnCount(id uint64, columnName, indexName [2]byte, count int, batch *pebble.Batch) error {
	countKey := key.NewChannelInfoColumnKey(id, columnName)
	if count == 0 { //
		return batch.Set(countKey, []byte{0x00, 0x00, 0x00, 0x00}, ms.noSync)
	}

	countBytes, closer, err := batch.Get(countKey)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var oldCount uint32
	if len(countBytes) > 0 {
		oldCount = ms.endian.Uint32(countBytes)
	} else {
		countBytes = make([]byte, 4)
	}
	count += int(oldCount)
	ms.endian.PutUint32(countBytes, uint32(count))

	// 设置数量
	err = batch.Set(countKey, countBytes, ms.noSync)
	if err != nil {
		return err
	}
	// 设置数量对应的索引
	secondIndexKey := key.NewChannelInfoSecondIndexKey(indexName, uint64(count), id)
	err = batch.Set(secondIndexKey, nil, ms.noSync)
	if err != nil {
		return err
	}
	return nil
}

func (ms *mushanDB) writeChannelInfo(primaryKey uint64, channelInfo ChannelInfo, w pebble.Writer) error {

	var (
		err error
	)
	// channelId
	if err = w.Set(key.NewChannelInfoColumnKey(primaryKey, key.TableChannelInfo.Column.ChannelId), []byte(channelInfo.ChannelId), ms.noSync); err != nil {
		return err
	}

	// channelType
	channelTypeBytes := make([]byte, 1)
	channelTypeBytes[0] = channelInfo.ChannelType
	if err = w.Set(key.NewChannelInfoColumnKey(primaryKey, key.TableChannelInfo.Column.ChannelType), channelTypeBytes, ms.noSync); err != nil {
		return err
	}

	// ban
	banBytes := make([]byte, 1)
	banBytes[0] = msutil.BoolToUint8(channelInfo.Ban)
	if err = w.Set(key.NewChannelInfoColumnKey(primaryKey, key.TableChannelInfo.Column.Ban), banBytes, ms.noSync); err != nil {
		return err
	}

	// large
	largeBytes := make([]byte, 1)
	largeBytes[0] = msutil.BoolToUint8(channelInfo.Large)
	if err = w.Set(key.NewChannelInfoColumnKey(primaryKey, key.TableChannelInfo.Column.Large), largeBytes, ms.noSync); err != nil {
		return err
	}

	// disband
	disbandBytes := make([]byte, 1)
	disbandBytes[0] = msutil.BoolToUint8(channelInfo.Disband)
	if err = w.Set(key.NewChannelInfoColumnKey(primaryKey, key.TableChannelInfo.Column.Disband), disbandBytes, ms.noSync); err != nil {
		return err
	}

	// createdAt
	if channelInfo.CreatedAt != nil {
		ct := uint64(channelInfo.CreatedAt.UnixNano())
		createdAt := make([]byte, 8)
		ms.endian.PutUint64(createdAt, ct)
		if err = w.Set(key.NewChannelInfoColumnKey(primaryKey, key.TableChannelInfo.Column.CreatedAt), createdAt, ms.noSync); err != nil {
			return err
		}
	}

	if channelInfo.UpdatedAt != nil {
		// updatedAt
		updatedAt := make([]byte, 8)
		ms.endian.PutUint64(updatedAt, uint64(channelInfo.UpdatedAt.UnixNano()))
		if err = w.Set(key.NewChannelInfoColumnKey(primaryKey, key.TableChannelInfo.Column.UpdatedAt), updatedAt, ms.noSync); err != nil {
			return err
		}

	}

	// write index
	if err = ms.writeChannelInfoBaseIndex(channelInfo, w); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) writeChannelInfoBaseIndex(channelInfo ChannelInfo, w pebble.Writer) error {
	primaryKey, err := ms.getChannelPrimaryKey(channelInfo.ChannelId, channelInfo.ChannelType)
	if err != nil {
		return err
	}

	if channelInfo.CreatedAt != nil {
		// createdAt second index
		if err := w.Set(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.CreatedAt, uint64(channelInfo.CreatedAt.UnixNano()), primaryKey), nil, ms.noSync); err != nil {
			return err
		}
	}

	if channelInfo.UpdatedAt != nil {
		// updatedAt second index
		if err := w.Set(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.UpdatedAt, uint64(channelInfo.UpdatedAt.UnixNano()), primaryKey), nil, ms.noSync); err != nil {
			return err
		}
	}

	// channel index
	primaryKeyBytes := make([]byte, 8)
	ms.endian.PutUint64(primaryKeyBytes, primaryKey)
	if err = w.Set(key.NewChannelInfoIndexKey(key.TableChannelInfo.Index.Channel, key.ChannelToNum(channelInfo.ChannelId, channelInfo.ChannelType)), primaryKeyBytes, ms.noSync); err != nil {
		return err
	}

	// ban index
	if err = w.Set(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.Ban, uint64(msutil.BoolToInt(channelInfo.Ban)), primaryKey), nil, ms.noSync); err != nil {
		return err
	}

	// disband index
	if err = w.Set(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.Disband, uint64(msutil.BoolToInt(channelInfo.Disband)), primaryKey), nil, ms.noSync); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) deleteChannelInfoBaseIndex(channelInfo ChannelInfo, w pebble.Writer) error {
	if channelInfo.CreatedAt != nil {
		// createdAt second index
		ct := uint64(channelInfo.CreatedAt.UnixNano())
		if err := w.Delete(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.CreatedAt, ct, channelInfo.Id), ms.noSync); err != nil {
			return err
		}
	}

	if channelInfo.UpdatedAt != nil {
		// updatedAt second index
		if err := w.Delete(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.UpdatedAt, uint64(channelInfo.UpdatedAt.UnixNano()), channelInfo.Id), ms.noSync); err != nil {
			return err
		}
	}

	// channel index
	if err := w.Delete(key.NewChannelInfoIndexKey(key.TableChannelInfo.Index.Channel, key.ChannelToNum(channelInfo.ChannelId, channelInfo.ChannelType)), ms.noSync); err != nil {
		return err
	}

	// ban index
	if err := w.Delete(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.Ban, uint64(msutil.BoolToInt(channelInfo.Ban)), channelInfo.Id), ms.noSync); err != nil {
		return err
	}

	// disband index
	if err := w.Delete(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.Disband, uint64(msutil.BoolToInt(channelInfo.Disband)), channelInfo.Id), ms.noSync); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) iterChannelInfo(iter *pebble.Iterator, iterFnc func(channelInfo ChannelInfo) bool) error {
	var (
		preId          uint64
		preChannelInfo ChannelInfo
		lastNeedAppend bool = true
		hasData        bool = false
	)
	for iter.First(); iter.Valid(); iter.Next() {
		id, columnName, err := key.ParseChannelInfoColumnKey(iter.Key())
		if err != nil {
			return err
		}
		if id != preId {
			if preId != 0 {
				if !iterFnc(preChannelInfo) {
					lastNeedAppend = false
					break
				}
			}
			preId = id
			preChannelInfo = ChannelInfo{
				Id: id,
			}
		}

		switch columnName {
		case key.TableChannelInfo.Column.ChannelId:
			preChannelInfo.ChannelId = string(iter.Value())
		case key.TableChannelInfo.Column.ChannelType:
			preChannelInfo.ChannelType = iter.Value()[0]
		case key.TableChannelInfo.Column.Ban:
			preChannelInfo.Ban = msutil.Uint8ToBool(iter.Value()[0])
		case key.TableChannelInfo.Column.Large:
			preChannelInfo.Large = msutil.Uint8ToBool(iter.Value()[0])
		case key.TableChannelInfo.Column.Disband:
			preChannelInfo.Disband = msutil.Uint8ToBool(iter.Value()[0])
		case key.TableChannelInfo.Column.SubscriberCount:
			preChannelInfo.SubscriberCount = int(ms.endian.Uint32(iter.Value()))
		case key.TableChannelInfo.Column.AllowlistCount:
			preChannelInfo.AllowlistCount = int(ms.endian.Uint32(iter.Value()))
		case key.TableChannelInfo.Column.DenylistCount:
			preChannelInfo.DenylistCount = int(ms.endian.Uint32(iter.Value()))
		case key.TableChannelInfo.Column.CreatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preChannelInfo.CreatedAt = &t
			}

		case key.TableChannelInfo.Column.UpdatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preChannelInfo.UpdatedAt = &t
			}
		}
		hasData = true
	}
	if lastNeedAppend && hasData {
		_ = iterFnc(preChannelInfo)
	}
	return nil
}

func (ms *mushanDB) getChannelPrimaryKey(channelId string, channelType uint8) (uint64, error) {

	return key.ChannelToNum(channelId, channelType), nil

}
