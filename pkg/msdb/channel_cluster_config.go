package msdb

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
	"go.uber.org/zap"
)

func (ms *mushanDB) SaveChannelClusterConfig(channelClusterConfig ChannelClusterConfig) error {

	ms.metrics.SaveChannelClusterConfigAdd(1)

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			end := time.Since(start)
			if end > time.Millisecond*500 {
				ms.Warn("save channel cluster config", zap.Duration("cost", time.Since(start)), zap.String("channelId", channelClusterConfig.ChannelId), zap.Uint8("channelType", channelClusterConfig.ChannelType))
			}
		}()
	}

	ms.dblock.channelClusterConfig.lockByChannel(channelClusterConfig.ChannelId, channelClusterConfig.ChannelType)
	defer ms.dblock.channelClusterConfig.unlockByChannel(channelClusterConfig.ChannelId, channelClusterConfig.ChannelType)

	primaryKey := key.ChannelToNum(channelClusterConfig.ChannelId, channelClusterConfig.ChannelType)

	db := ms.defaultShardBatchDB()

	batch := db.NewBatch()

	channelClusterConfig.Id = primaryKey

	if err := ms.writeChannelClusterConfig(primaryKey, channelClusterConfig, batch); err != nil {
		return err
	}

	return batch.CommitWait()
}

func (ms *mushanDB) SaveChannelClusterConfigs(channelClusterConfigs []ChannelClusterConfig) error {

	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			end := time.Since(start)
			if end > time.Millisecond*500 {
				ms.Warn("SaveChannelClusterConfigs too cost", zap.Duration("cost", time.Since(start)), zap.Int("count", len(channelClusterConfigs)))
			}
		}()
	}

	ms.metrics.SaveChannelClusterConfigsAdd(1)

	db := ms.defaultShardBatchDB()

	batch := db.NewBatch()
	for _, cfg := range channelClusterConfigs {
		primaryKey := key.ChannelToNum(cfg.ChannelId, cfg.ChannelType)
		if err := ms.writeChannelClusterConfig(primaryKey, cfg, batch); err != nil {
			return err
		}
	}
	return batch.CommitWait()
}

func (ms *mushanDB) GetChannelClusterConfig(channelId string, channelType uint8) (ChannelClusterConfig, error) {

	ms.metrics.GetChannelClusterConfigAdd(1)

	start := time.Now()
	defer func() {
		end := time.Since(start)
		if end > time.Millisecond*200 {
			ms.Warn("get channel cluster config cost to long", zap.Duration("cost", end), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		}
	}()

	primaryKey := key.ChannelToNum(channelId, channelType)

	return ms.getChannelClusterConfigById(primaryKey)
}

func (ms *mushanDB) getChannelClusterConfigById(id uint64) (ChannelClusterConfig, error) {
	iter := ms.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelClusterConfigColumnKey(id, key.MinColumnKey),
		UpperBound: key.NewChannelClusterConfigColumnKey(id, key.MaxColumnKey),
	})
	defer iter.Close()

	var resultCfg ChannelClusterConfig = EmptyChannelClusterConfig
	err := ms.iteratorChannelClusterConfig(iter, func(cfg ChannelClusterConfig) bool {
		resultCfg = cfg
		return false
	})

	if err != nil {
		return EmptyChannelClusterConfig, err
	}

	if IsEmptyChannelClusterConfig(resultCfg) {
		return EmptyChannelClusterConfig, ErrNotFound
	}
	resultCfg.Id = id

	return resultCfg, nil
}

func (ms *mushanDB) GetChannelClusterConfigVersion(channelId string, channelType uint8) (uint64, error) {
	primaryKey := key.ChannelToNum(channelId, channelType)
	result, closer, err := ms.defaultShardDB().Get(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ConfVersion))
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(result) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(result), nil
}

func (ms *mushanDB) GetChannelClusterConfigs(offsetId uint64, limit int) ([]ChannelClusterConfig, error) {

	ms.metrics.GetChannelClusterConfigsAdd(1)

	iter := ms.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelClusterConfigColumnKey(offsetId+1, key.MinColumnKey),
		UpperBound: key.NewChannelClusterConfigColumnKey(math.MaxUint64, key.MaxColumnKey),
	})
	defer iter.Close()

	results := make([]ChannelClusterConfig, 0)
	err := ms.iteratorChannelClusterConfig(iter, func(cfg ChannelClusterConfig) bool {
		if len(results) >= limit {
			return false
		}
		if cfg.Id > offsetId {
			results = append(results, cfg)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return results, nil

}

func (ms *mushanDB) SearchChannelClusterConfig(req ChannelClusterConfigSearchReq, filter ...func(cfg ChannelClusterConfig) bool) ([]ChannelClusterConfig, error) {

	ms.metrics.SearchChannelClusterConfigAdd(1)

	if req.ChannelId != "" {
		cfg, err := ms.GetChannelClusterConfig(req.ChannelId, req.ChannelType)
		if err != nil {
			if err == pebble.ErrNotFound {
				return nil, nil
			}
			return nil, err
		}
		if len(filter) > 0 {
			for _, fnc := range filter {
				v := fnc(cfg)
				if !v {
					return nil, nil
				}
			}
		}
		return []ChannelClusterConfig{cfg}, nil
	}

	iterFnc := func(cfgs *[]ChannelClusterConfig) func(cfg ChannelClusterConfig) bool {
		currentSize := 0
		return func(cfg ChannelClusterConfig) bool {
			if req.Pre {
				if req.OffsetCreatedAt > 0 && cfg.CreatedAt != nil && cfg.CreatedAt.UnixNano() <= req.OffsetCreatedAt {
					return false
				}
			} else {
				if req.OffsetCreatedAt > 0 && cfg.CreatedAt != nil && cfg.CreatedAt.UnixNano() >= req.OffsetCreatedAt {
					return false
				}
			}

			if currentSize > req.Limit {
				return false
			}

			if len(filter) > 0 {
				for _, fnc := range filter {
					v := fnc(cfg)
					if !v {
						return true
					}
				}
			}

			currentSize++
			*cfgs = append(*cfgs, cfg)

			return true
		}
	}

	cfgs := make([]ChannelClusterConfig, 0, req.Limit)
	fnc := iterFnc(&cfgs)

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

	db := ms.defaultShardDB()

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.CreatedAt, start, 0),
		UpperBound: key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.CreatedAt, end, 0),
	})
	defer iter.Close()

	var iterStepFnc func() bool
	if req.Pre {
		if !iter.First() {
			return nil, nil
		}
		iterStepFnc = iter.Next
	} else {
		if !iter.Last() {
			return nil, nil
		}
		iterStepFnc = iter.Prev
	}
	for ; iter.Valid(); iterStepFnc() {
		_, id, err := key.ParseChannelClusterConfigSecondIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}

		dataIter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewChannelClusterConfigColumnKey(id, key.MinColumnKey),
			UpperBound: key.NewChannelClusterConfigColumnKey(id, key.MaxColumnKey),
		})
		defer dataIter.Close()

		var c ChannelClusterConfig
		err = ms.iteratorChannelClusterConfig(dataIter, func(cfg ChannelClusterConfig) bool {
			c = cfg
			return false
		})
		if err != nil {
			return nil, err
		}
		if !fnc(c) {
			break
		}
	}

	return cfgs, nil
}

func (ms *mushanDB) GetChannelClusterConfigCountWithSlotId(slotId uint32) (int, error) {

	ms.metrics.GetChannelClusterConfigCountWithSlotIdAdd(1)

	cfgs, err := ms.GetChannelClusterConfigWithSlotId(slotId)
	if err != nil {
		return 0, err
	}
	return len(cfgs), nil
}

func (ms *mushanDB) GetChannelClusterConfigWithSlotId(slotId uint32) ([]ChannelClusterConfig, error) {

	ms.metrics.GetChannelClusterConfigWithSlotIdAdd(1)

	iter := ms.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelClusterConfigColumnKey(0, key.MinColumnKey),
		UpperBound: key.NewChannelClusterConfigColumnKey(math.MaxUint64, key.MaxColumnKey),
	})
	defer iter.Close()

	results := make([]ChannelClusterConfig, 0)
	err := ms.iteratorChannelClusterConfig(iter, func(cfg ChannelClusterConfig) bool {
		resultSlotId := ms.channelSlotId(cfg.ChannelId)
		if slotId == resultSlotId {
			results = append(results, cfg)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (ms *mushanDB) writeChannelClusterConfig(primaryKey uint64, channelClusterConfig ChannelClusterConfig, w *Batch) error {

	// channelId
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ChannelId), []byte(channelClusterConfig.ChannelId))

	// channelType
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ChannelType), []byte{channelClusterConfig.ChannelType})

	// replicaMaxCount
	var replicaMaxCountBytes = make([]byte, 2)
	binary.BigEndian.PutUint16(replicaMaxCountBytes, channelClusterConfig.ReplicaMaxCount)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ReplicaMaxCount), replicaMaxCountBytes)

	// replicas
	var replicasBytes = make([]byte, 8*len(channelClusterConfig.Replicas))
	for i, replica := range channelClusterConfig.Replicas {
		binary.BigEndian.PutUint64(replicasBytes[i*8:], replica)
	}
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Replicas), replicasBytes)

	// learners
	var learnersBytes = make([]byte, 8*len(channelClusterConfig.Learners))
	for i, learner := range channelClusterConfig.Learners {
		binary.BigEndian.PutUint64(learnersBytes[i*8:], learner)
	}
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Learners), learnersBytes)

	// leaderId
	leaderIdBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(leaderIdBytes, channelClusterConfig.LeaderId)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.LeaderId), leaderIdBytes)

	// term
	termBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(termBytes, channelClusterConfig.Term)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Term), termBytes)

	// migrateFrom
	migrateFromBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(migrateFromBytes, channelClusterConfig.MigrateFrom)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.MigrateFrom), migrateFromBytes)

	// migrateTo
	migrateToBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(migrateToBytes, channelClusterConfig.MigrateTo)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.MigrateTo), migrateToBytes)

	// status
	statusBytes := make([]byte, 1)
	statusBytes[0] = uint8(channelClusterConfig.Status)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Status), statusBytes)

	// config version
	configVersionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(configVersionBytes, channelClusterConfig.ConfVersion)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ConfVersion), configVersionBytes)

	//version
	versionBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(versionBytes, channelClusterConfig.version)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Version), versionBytes)

	if channelClusterConfig.CreatedAt != nil {
		// createdAt
		ct := uint64(channelClusterConfig.CreatedAt.UnixNano())
		var createdAtBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(createdAtBytes, ct)
		w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.CreatedAt), createdAtBytes)
	}

	if channelClusterConfig.UpdatedAt != nil {
		// updatedAt
		up := uint64(channelClusterConfig.UpdatedAt.UnixNano())
		var updatedAtBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(updatedAtBytes, up)
		w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.UpdatedAt), updatedAtBytes)
	}

	// write index
	if err := ms.writeChannelClusterConfigIndex(primaryKey, channelClusterConfig, w); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) writeChannelClusterConfigIndex(primaryKey uint64, channelClusterConfig ChannelClusterConfig, w *Batch) error {

	// channel index
	primaryKeyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(primaryKeyBytes, primaryKey)
	w.Set(key.NewChannelClusterConfigIndexKey(channelClusterConfig.ChannelId, channelClusterConfig.ChannelType), primaryKeyBytes)

	// leader second index
	w.Set(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.LeaderId, channelClusterConfig.LeaderId, primaryKey), nil)
	if channelClusterConfig.CreatedAt != nil {
		ct := uint64(channelClusterConfig.CreatedAt.UnixNano())
		// createdAt second index
		w.Set(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.CreatedAt, ct, primaryKey), nil)
	}

	if channelClusterConfig.UpdatedAt != nil {
		up := uint64(channelClusterConfig.UpdatedAt.UnixNano())
		// updatedAt second index
		w.Set(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.UpdatedAt, up, primaryKey), nil)
	}
	return nil
}

func (ms *mushanDB) deleteChannelClusterConfigIndex(primaryKey uint64, channelClusterConfig ChannelClusterConfig, w *Batch) error {

	// channel index
	w.Delete(key.NewChannelClusterConfigIndexKey(channelClusterConfig.ChannelId, channelClusterConfig.ChannelType))

	// leader second index
	w.Delete(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.LeaderId, channelClusterConfig.LeaderId, primaryKey))

	if channelClusterConfig.CreatedAt != nil {
		ct := uint64(channelClusterConfig.CreatedAt.UnixNano())
		// createdAt second index
		w.Delete(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.CreatedAt, ct, primaryKey))
	}

	if channelClusterConfig.UpdatedAt != nil {
		up := uint64(channelClusterConfig.UpdatedAt.UnixNano())
		// updatedAt second index
		w.Delete(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.UpdatedAt, up, primaryKey))
	}

	return nil
}

func (ms *mushanDB) iteratorChannelClusterConfig(iter *pebble.Iterator, iterFnc func(cfg ChannelClusterConfig) bool) error {

	var (
		preId                   uint64
		preChannelClusterConfig ChannelClusterConfig
		lastNeedAppend          bool = true
		hasData                 bool = false
	)
	for iter.First(); iter.Valid(); iter.Next() {

		id, columnName, err := key.ParseChannelClusterConfigColumnKey(iter.Key())
		if err != nil {
			return err
		}

		if id != preId {
			if preId != 0 {
				if !iterFnc(preChannelClusterConfig) {
					lastNeedAppend = false
					break
				}

			}
			preId = id
			preChannelClusterConfig = ChannelClusterConfig{
				Id: id,
			}
		}

		switch columnName {
		case key.TableChannelClusterConfig.Column.ChannelId:
			preChannelClusterConfig.ChannelId = string(iter.Value())
		case key.TableChannelClusterConfig.Column.ChannelType:
			preChannelClusterConfig.ChannelType = iter.Value()[0]
		case key.TableChannelClusterConfig.Column.ReplicaMaxCount:
			preChannelClusterConfig.ReplicaMaxCount = ms.endian.Uint16(iter.Value())
		case key.TableChannelClusterConfig.Column.Replicas:
			replicas := make([]uint64, len(iter.Value())/8)
			for i := 0; i < len(replicas); i++ {
				replicas[i] = ms.endian.Uint64(iter.Value()[i*8:])
			}
			preChannelClusterConfig.Replicas = replicas
		case key.TableChannelClusterConfig.Column.Learners:
			learners := make([]uint64, len(iter.Value())/8)
			for i := 0; i < len(learners); i++ {
				learners[i] = ms.endian.Uint64(iter.Value()[i*8:])
			}
			preChannelClusterConfig.Learners = learners
		case key.TableChannelClusterConfig.Column.LeaderId:
			preChannelClusterConfig.LeaderId = ms.endian.Uint64(iter.Value())
		case key.TableChannelClusterConfig.Column.Term:
			preChannelClusterConfig.Term = ms.endian.Uint32(iter.Value())
		case key.TableChannelClusterConfig.Column.MigrateFrom:
			preChannelClusterConfig.MigrateFrom = ms.endian.Uint64(iter.Value())
		case key.TableChannelClusterConfig.Column.MigrateTo:
			preChannelClusterConfig.MigrateTo = ms.endian.Uint64(iter.Value())
		case key.TableChannelClusterConfig.Column.Status:
			preChannelClusterConfig.Status = ChannelClusterStatus(iter.Value()[0])
		case key.TableChannelClusterConfig.Column.ConfVersion:
			preChannelClusterConfig.ConfVersion = ms.endian.Uint64(iter.Value())
		case key.TableChannelClusterConfig.Column.Version:
			preChannelClusterConfig.version = ms.endian.Uint16(iter.Value())
		case key.TableChannelClusterConfig.Column.CreatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preChannelClusterConfig.CreatedAt = &t
			}
		case key.TableChannelClusterConfig.Column.UpdatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preChannelClusterConfig.UpdatedAt = &t
			}
		}
		hasData = true
	}
	if lastNeedAppend && hasData {
		_ = iterFnc(preChannelClusterConfig)
	}
	return nil
}
