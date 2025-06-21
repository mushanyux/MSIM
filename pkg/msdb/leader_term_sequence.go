package msdb

import (
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
)

func (ms *mushanDB) SetLeaderTermStartIndex(shardNo string, term uint32, index uint64) error {

	ms.metrics.SetLeaderTermStartIndexAdd(1)

	indexBytes := make([]byte, 8)
	ms.endian.PutUint64(indexBytes, index)
	batch := ms.sharedBatchDB(shardNo).NewBatch()

	batch.Set(key.NewLeaderTermSequenceTermKey(shardNo, term), indexBytes)

	return batch.CommitWait()
}

func (ms *mushanDB) LeaderLastTerm(shardNo string) (uint32, error) {

	ms.metrics.LeaderLastTermAdd(1)

	iter := ms.shardDB(shardNo).NewIter(&pebble.IterOptions{
		LowerBound: key.NewLeaderTermSequenceTermKey(shardNo, 0),
		UpperBound: key.NewLeaderTermSequenceTermKey(shardNo, math.MaxUint32),
	})
	defer iter.Close()

	if iter.Last() && iter.Valid() {
		term, err := key.ParseLeaderTermSequenceTermKey(iter.Key())
		if err != nil {
			return 0, err
		}
		return term, nil
	}
	return 0, nil
}

func (ms *mushanDB) LeaderTermStartIndex(shardNo string, term uint32) (uint64, error) {

	ms.metrics.LeaderTermStartIndexAdd(1)

	indexBytes, closer, err := ms.shardDB(shardNo).Get(key.NewLeaderTermSequenceTermKey(shardNo, term))
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	return ms.endian.Uint64(indexBytes), nil
}

func (ms *mushanDB) LeaderLastTermGreaterEqThan(shardNo string, term uint32) (uint32, error) {

	ms.metrics.LeaderLastTermGreaterThanAdd(1)

	iter := ms.shardDB(shardNo).NewIter(&pebble.IterOptions{
		LowerBound: key.NewLeaderTermSequenceTermKey(shardNo, term),
		UpperBound: key.NewLeaderTermSequenceTermKey(shardNo, math.MaxUint32),
	})
	defer iter.Close()

	var maxTerm uint32 = term
	for iter.First(); iter.Valid(); iter.Next() {
		qterm, err := key.ParseLeaderTermSequenceTermKey(iter.Key())
		if err != nil {
			return 0, err
		}
		if qterm >= maxTerm {
			maxTerm = qterm
			break
		}
	}
	return maxTerm, nil
}

func (ms *mushanDB) DeleteLeaderTermStartIndexGreaterThanTerm(shardNo string, term uint32) error {

	ms.metrics.DeleteLeaderTermStartIndexGreaterThanTermAdd(1)

	return ms.shardDB(shardNo).DeleteRange(key.NewLeaderTermSequenceTermKey(shardNo, term+1), key.NewLeaderTermSequenceTermKey(shardNo, math.MaxUint32), ms.sync)
}
