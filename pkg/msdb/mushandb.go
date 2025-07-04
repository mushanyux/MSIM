package msdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"path/filepath"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/cockroachdb/pebble"
	"github.com/lni/goutils/syncutil"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/mushanyux/MSIM/pkg/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ DB = (*mushanDB)(nil)

type mushanDB struct {
	dbs      []*pebble.DB
	msdbs    []*BatchDB
	shardNum uint32 // 分区数量，这个一但设置就不能修改
	opts     *Options
	sync     *pebble.WriteOptions
	endian   binary.ByteOrder
	mslog.Log
	prmaryKeyGen *snowflake.Node // 消息ID生成器
	noSync       *pebble.WriteOptions
	dblock       *dblock
	cancelCtx    context.Context
	cancelFunc   context.CancelFunc

	metrics trace.IDBMetrics

	channelSeqCache *channelSeqCache

	h hash.Hash32
}

func NewMushanDB(opts *Options) DB {
	prmaryKeyGen, err := snowflake.NewNode(int64(opts.NodeId))
	if err != nil {
		panic(err)
	}

	var metrics trace.IDBMetrics
	if trace.GlobalTrace != nil {
		metrics = trace.GlobalTrace.Metrics.DB()
	} else {
		metrics = trace.NewDBMetrics()
	}

	endian := binary.BigEndian

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	return &mushanDB{
		opts:            opts,
		shardNum:        uint32(opts.ShardNum),
		prmaryKeyGen:    prmaryKeyGen,
		endian:          endian,
		cancelCtx:       cancelCtx,
		cancelFunc:      cancelFunc,
		metrics:         metrics,
		channelSeqCache: newChannelSeqCache(10000, endian),
		h:               fnv.New32(),
		sync: &pebble.WriteOptions{
			Sync: true,
		},
		noSync: &pebble.WriteOptions{
			Sync: false,
		},
		Log:    mslog.NewMSLog("mushanDB"),
		dblock: newDBLock(),
	}
}

func (ms *mushanDB) defaultPebbleOptions() *pebble.Options {
	blockSize := 32 * 1024
	sz := 16 * 1024 * 1024
	levelSizeMultiplier := 2

	lopts := make([]pebble.LevelOptions, 0)
	var numOfLevels int64 = 7
	for l := int64(0); l < numOfLevels; l++ {
		opt := pebble.LevelOptions{
			// Compression:    pebble.NoCompression,
			BlockSize:      blockSize,
			TargetFileSize: 16 * 1024 * 1024,
		}
		sz = sz * levelSizeMultiplier
		lopts = append(lopts, opt)
	}
	return &pebble.Options{
		Levels:             lopts,
		FormatMajorVersion: pebble.FormatNewest,
		// 控制写缓冲区的大小。较大的写缓冲区可以减少磁盘写入次数，但会占用更多内存。
		MemTableSize: ms.opts.MemTableSize,
		// 当队列中的MemTables的大小超过 MemTableStopWritesThreshold*MemTableSize 时，将停止写入，
		// 直到被刷到磁盘，这个值不能小于2
		MemTableStopWritesThreshold: 4,
		// MANIFEST 文件的大小
		MaxManifestFileSize:       128 * 1024 * 1024,
		LBaseMaxBytes:             4 * 1024 * 1024 * 1024,
		L0CompactionFileThreshold: 8,
		L0StopWritesThreshold:     24,
	}
}

func (ms *mushanDB) Open() error {

	ms.dblock.start()

	opts := ms.defaultPebbleOptions()
	for i := 0; i < int(ms.shardNum); i++ {

		db, err := pebble.Open(filepath.Join(ms.opts.DataDir, "mushanimdb", fmt.Sprintf("shard%03d", i)), opts)
		if err != nil {
			return err
		}
		ms.dbs = append(ms.dbs, db)

		msdb := NewBatchDB(i, db)
		msdb.Start()
		ms.msdbs = append(ms.msdbs, msdb)
	}

	go ms.collectMetricsLoop()

	return nil
}

func (ms *mushanDB) Close() error {
	ms.cancelFunc()
	for _, db := range ms.dbs {
		if err := db.Close(); err != nil {
			ms.Error("close db error", zap.Error(err))
		}
	}

	for _, msd := range ms.msdbs {
		msd.Stop()
	}
	ms.dblock.stop()
	return nil
}

func (ms *mushanDB) shardDB(v string) *pebble.DB {
	shardId := ms.shardId(v)
	return ms.dbs[shardId]
}

func (ms *mushanDB) sharedBatchDB(v string) *BatchDB {
	shardId := ms.shardId(v)
	return ms.msdbs[shardId]
}

func (ms *mushanDB) shardId(v string) uint32 {
	if v == "" {
		ms.Panic("shardId key is empty")
	}
	if ms.opts.ShardNum == 1 {
		return 0
	}
	h := fnv.New32()
	h.Write([]byte(v))

	return h.Sum32() % ms.shardNum
}

func (ms *mushanDB) shardDBById(id uint32) *pebble.DB {
	return ms.dbs[id]
}

func (ms *mushanDB) shardBatchDBById(id uint32) *BatchDB {
	return ms.msdbs[id]
}

func (ms *mushanDB) defaultShardDB() *pebble.DB {
	return ms.dbs[0]
}

func (ms *mushanDB) defaultShardBatchDB() *BatchDB {
	return ms.msdbs[0]
}

func (ms *mushanDB) channelSlotId(channelId string) uint32 {
	return msutil.GetSlotNum(int(ms.opts.SlotCount), channelId)
}

func (ms *mushanDB) collectMetricsLoop() {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			ms.collectMetrics()
		case <-ms.cancelCtx.Done():
			return
		}
	}
}

func (ms *mushanDB) collectMetrics() {

	for i := uint32(0); i < uint32(ms.shardNum); i++ {
		ms := ms.dbs[i].Metrics()

		// ========== compact 压缩相关 ==========
		trace.GlobalTrace.Metrics.DB().CompactTotalCountSet(i, ms.Compact.Count)
		trace.GlobalTrace.Metrics.DB().CompactDefaultCountSet(i, ms.Compact.DefaultCount)
		trace.GlobalTrace.Metrics.DB().CompactDeleteOnlyCountSet(i, ms.Compact.DeleteOnlyCount)
		trace.GlobalTrace.Metrics.DB().CompactElisionOnlyCountSet(i, ms.Compact.ElisionOnlyCount)
		trace.GlobalTrace.Metrics.DB().CompactEstimatedDebtSet(i, int64(ms.Compact.EstimatedDebt))
		trace.GlobalTrace.Metrics.DB().CompactInProgressBytesSet(i, ms.Compact.InProgressBytes)
		trace.GlobalTrace.Metrics.DB().CompactMarkedFilesSet(i, int64(ms.Compact.MarkedFiles))
		trace.GlobalTrace.Metrics.DB().CompactMoveCountSet(i, ms.Compact.MoveCount)
		trace.GlobalTrace.Metrics.DB().CompactMultiLevelCount(i, ms.Compact.MultiLevelCount)
		trace.GlobalTrace.Metrics.DB().CompactNumInProgressSet(i, ms.Compact.NumInProgress)
		trace.GlobalTrace.Metrics.DB().CompactReadCountSet(i, ms.Compact.ReadCount)
		trace.GlobalTrace.Metrics.DB().CompactRewriteCountSet(i, ms.Compact.RewriteCount)

		// ========== flush 相关 ==========
		trace.GlobalTrace.Metrics.DB().FlushCountAdd(i, int64(ms.Flush.Count))
		trace.GlobalTrace.Metrics.DB().FlushBytesAdd(i, ms.Flush.WriteThroughput.Bytes)
		trace.GlobalTrace.Metrics.DB().FlushNumInProgressAdd(i, ms.Flush.NumInProgress)
		trace.GlobalTrace.Metrics.DB().FlushAsIngestCountAdd(i, int64(ms.Flush.AsIngestCount))
		trace.GlobalTrace.Metrics.DB().FlushAsIngestTableCountAdd(i, int64(ms.Flush.AsIngestTableCount))
		trace.GlobalTrace.Metrics.DB().FlushAsIngestBytesAdd(i, int64(ms.Flush.AsIngestBytes))

		// ========== memtable 内存表相关 ==========
		trace.GlobalTrace.Metrics.DB().MemTableCountSet(i, int64(ms.MemTable.Count))
		trace.GlobalTrace.Metrics.DB().MemTableSizeSet(i, int64(ms.MemTable.Size))
		trace.GlobalTrace.Metrics.DB().MemTableZombieSizeSet(i, int64(ms.MemTable.ZombieSize))
		trace.GlobalTrace.Metrics.DB().MemTableZombieCountSet(i, ms.MemTable.ZombieCount)

		// ========== Snapshots 镜像相关 ==========
		trace.GlobalTrace.Metrics.DB().SnapshotsCountSet(i, int64(ms.Snapshots.Count))

		// ========== TableCache 相关 ==========
		trace.GlobalTrace.Metrics.DB().TableCacheSizeSet(i, ms.TableCache.Size)
		trace.GlobalTrace.Metrics.DB().TableCacheCountSet(i, ms.TableCache.Count)
		trace.GlobalTrace.Metrics.DB().TableItersCountSet(i, ms.TableIters)

		// ========== WAL 相关 ==========
		trace.GlobalTrace.Metrics.DB().WALFilesCountSet(i, ms.WAL.Files)
		trace.GlobalTrace.Metrics.DB().WALSizeSet(i, int64(ms.WAL.Size))
		trace.GlobalTrace.Metrics.DB().WALPhysicalSizeSet(i, int64(ms.WAL.PhysicalSize))
		trace.GlobalTrace.Metrics.DB().WALObsoleteFilesCountSet(i, ms.WAL.ObsoleteFiles)
		trace.GlobalTrace.Metrics.DB().WALObsoletePhysicalSizeSet(i, int64(ms.WAL.ObsoletePhysicalSize))
		trace.GlobalTrace.Metrics.DB().WALBytesInSet(i, int64(ms.WAL.BytesIn))
		trace.GlobalTrace.Metrics.DB().WALBytesWrittenSet(i, int64(ms.WAL.BytesWritten))

		// ========== Write 相关 ==========
		trace.GlobalTrace.Metrics.DB().LogWriterBytesSet(i, ms.LogWriter.WriteThroughput.Bytes)

		trace.GlobalTrace.Metrics.DB().DiskSpaceUsageSet(i, int64(ms.DiskSpaceUsage()))

		// ========== level 相关 ==========

		trace.GlobalTrace.Metrics.DB().LevelNumFilesSet(i, ms.Total().NumFiles)
		trace.GlobalTrace.Metrics.DB().LevelFileSizeSet(i, int64(ms.Total().Size))
		trace.GlobalTrace.Metrics.DB().LevelCompactScoreSet(i, int64(ms.Total().Score))
		trace.GlobalTrace.Metrics.DB().LevelBytesInSet(i, int64(ms.Total().BytesIn))
		trace.GlobalTrace.Metrics.DB().LevelBytesIngestedSet(i, int64(ms.Total().BytesIngested))
		trace.GlobalTrace.Metrics.DB().LevelBytesMovedSet(i, int64(ms.Total().BytesMoved))
		trace.GlobalTrace.Metrics.DB().LevelBytesReadSet(i, int64(ms.Total().BytesRead))
		trace.GlobalTrace.Metrics.DB().LevelBytesCompactedSet(i, int64(ms.Total().BytesCompacted))
		trace.GlobalTrace.Metrics.DB().LevelBytesFlushedSet(i, int64(ms.Total().BytesFlushed))
		trace.GlobalTrace.Metrics.DB().LevelTablesCompactedSet(i, int64(ms.Total().TablesCompacted))
		trace.GlobalTrace.Metrics.DB().LevelTablesFlushedSet(i, int64(ms.Total().TablesFlushed))
		trace.GlobalTrace.Metrics.DB().LevelTablesIngestedSet(i, int64(ms.Total().TablesIngested))
		trace.GlobalTrace.Metrics.DB().LevelTablesMovedSet(i, int64(ms.Total().TablesMoved))

	}
}

func (ms *mushanDB) NextPrimaryKey() uint64 {
	return uint64(ms.prmaryKeyGen.Generate().Int64())
}

// 批量提交
func Commits(bs []*Batch) error {
	if len(bs) == 0 {
		return nil
	}
	newBatchs := groupBatch(bs)
	if len(newBatchs) == 1 {
		return newBatchs[0].CommitWait()
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	g, _ := errgroup.WithContext(timeoutCtx)
	g.SetLimit(200)
	for _, b := range newBatchs {
		b1 := b
		g.Go(func() error {
			return b1.CommitWait()
		})
	}
	return g.Wait()
}

// 将batch集合操作按照db进行聚合到一起

func groupBatch(bs []*Batch) []*Batch {
	newBatchs := make([]*Batch, 0, len(bs))
	for _, b := range bs {
		exist := false
		for _, nb := range newBatchs {
			if nb.db == b.db {
				exist = true
				nb.setKvs = append(nb.setKvs, b.setKvs...)
				nb.delKvs = append(nb.delKvs, b.delKvs...)
				nb.delRangeKvs = append(nb.delRangeKvs, b.delRangeKvs...)
				break
			}
		}
		if !exist {
			newBatchs = append(newBatchs, b)
		}
	}
	return newBatchs
}

type BatchDB struct {
	db *pebble.DB

	batchChan chan *Batch

	stopper *syncutil.Stopper
	Index   int
}

func NewBatchDB(index int, db *pebble.DB) *BatchDB {
	return &BatchDB{
		batchChan: make(chan *Batch, 4000),
		stopper:   syncutil.NewStopper(),
		db:        db,
		Index:     index,
	}
}

func (ms *BatchDB) NewBatch() *Batch {

	return &Batch{
		db: ms,
	}
}

func (ms *BatchDB) Start() {
	for i := 0; i < 1; i++ {
		ms.stopper.RunWorker(ms.loop)
	}
}

func (ms *BatchDB) Stop() {
	ms.stopper.Stop()
}

func (ms *BatchDB) loop() {
	batchSize := 100
	done := false
	batches := make([]*Batch, 0, batchSize)
	for {
		select {
		case bt := <-ms.batchChan:
			// 获取所有的batch
			batches = append(batches, bt)
			for !done {
				select {
				case b := <-ms.batchChan:
					batches = append(batches, b)
					if len(batches) >= batchSize {
						done = true
					}
				default:
					done = true
				}
			}
			ms.executeBatch(batches) // 批量执行
			batches = batches[:0]
			done = false

		case <-ms.stopper.ShouldStop():
			return
		}
	}
}

func (ms *BatchDB) executeBatch(bs []*Batch) {

	bt := ms.db.NewBatch()
	defer bt.Close()

	// start := time.Now()

	for _, b := range bs {

		// fmt.Println("batch-->:", b.String())

		// trace.GlobalTrace.Metrics.DB().SetAdd(int64(len(b.setKvs)))
		// trace.GlobalTrace.Metrics.DB().DeleteAdd(int64(len(b.delKvs)))
		// trace.GlobalTrace.Metrics.DB().DeleteRangeAdd(int64(len(b.delRangeKvs)))

		for _, kv := range b.delKvs {
			if err := bt.Delete(kv.key, pebble.NoSync); err != nil {
				b.err = err
				break
			}
		}

		for _, kv := range b.delRangeKvs {
			if err := bt.DeleteRange(kv.key, kv.val, pebble.NoSync); err != nil {
				b.err = err
				break
			}
		}

		for _, kv := range b.setKvs {
			if err := bt.Set(kv.key, kv.val, pebble.NoSync); err != nil {
				b.err = err
				break
			}
		}

	}
	// trace.GlobalTrace.Metrics.DB().CommitAdd(1)
	err := bt.Commit(pebble.Sync)
	if err != nil {
		for _, b := range bs {
			b.err = err
			if b.waitC != nil {
				b.waitC <- err
			}
		}
		return
	}

	// end := time.Since(start)
	// fmt.Println("executeBatch耗时--->", end, len(bs))

	for _, b := range bs {
		if b.waitC != nil {
			b.waitC <- b.err
		}
		// 释放资源
		b.release()
	}

}

type Batch struct {
	db          *BatchDB
	setKvs      []kv
	delKvs      []kv
	delRangeKvs []kv
	waitC       chan error
	err         error
}

func (b *Batch) Set(key, value []byte) {
	// 预分配切片容量
	if cap(b.setKvs) == 0 {
		b.setKvs = make([]kv, 0, 100) // 假设预估大小为100
	}
	b.setKvs = append(b.setKvs, kv{
		key: key,
		val: value,
	})
}

func (b *Batch) Delete(key []byte) {
	b.delKvs = append(b.delKvs, kv{
		key: key,
		val: nil,
	})
}

func (b *Batch) DeleteRange(start, end []byte) {
	b.delRangeKvs = append(b.delRangeKvs, kv{
		key: start,
		val: end,
	})
}

func (b *Batch) Commit() error {
	b.db.batchChan <- b
	return nil
}

func (b *Batch) CommitWait() error {
	b.waitC = make(chan error, 1)
	b.db.batchChan <- b
	return <-b.waitC
}

func (b *Batch) release() {
	b.setKvs = nil
	b.delKvs = nil
	b.delRangeKvs = nil
	b.waitC = nil
	b.err = nil
}

func (b *Batch) String() string {
	return fmt.Sprintf("setKvs:%d, delKvs:%d, delRangeKvs:%d", len(b.setKvs), len(b.delKvs), len(b.delRangeKvs))
}

func (b *Batch) DbIndex() int {
	return b.db.Index
}

type kv struct {
	key []byte
	val []byte
}
