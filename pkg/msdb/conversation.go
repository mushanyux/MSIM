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

func (ms *mushanDB) AddOrUpdateConversations(conversations []Conversation) error {
	ms.metrics.AddOrUpdateConversationsAdd(1)

	if len(conversations) == 0 {
		return nil
	}

	userBatchMap := make(map[uint32]*Batch)

	for _, conversation := range conversations {
		shardId := ms.shardId(conversation.Uid)
		batch := userBatchMap[shardId]
		if batch == nil {
			batch = ms.shardBatchDBById(shardId).NewBatch()
			userBatchMap[shardId] = batch
		}

		oldConversation, err := ms.GetConversation(conversation.Uid, conversation.ChannelId, conversation.ChannelType)
		if err != nil && err != ErrNotFound {
			return err
		}
		exist := !IsEmptyConversation(oldConversation)

		// 如果会话存在 则删除旧的索引
		if exist {
			oldConversation.CreatedAt = nil
			err = ms.deleteConversationIndex(oldConversation, batch)
			if err != nil {
				return err
			}
			conversation.Id = oldConversation.Id
		}

		if exist {
			conversation.CreatedAt = nil // 更新时不更新创建时间
		}

		if err := ms.writeConversation(conversation, batch); err != nil {
			return err
		}
	}

	err := ms.setConversationLocalUserRelation(conversations, false)
	if err != nil {
		return err
	}

	batchs := make([]*Batch, 0, len(userBatchMap))
	for _, batch := range userBatchMap {
		batchs = append(batchs, batch)
	}

	err = Commits(batchs)
	if err != nil {
		ms.Error("commits failed", zap.Error(err))
		return nil
	}

	return nil

}

func (ms *mushanDB) AddOrUpdateConversationsBatchIfNotExist(conversations []Conversation) error {
	if len(conversations) == 0 {
		return nil
	}

	userBatchMap := make(map[uint32]*Batch) // 用户uid分区对应的db

	for _, conversation := range conversations {

		shardId := ms.shardId(conversation.Uid)
		batch := userBatchMap[shardId]
		if batch == nil {
			batch = ms.shardBatchDBById(shardId).NewBatch()
			userBatchMap[shardId] = batch
		}

		exist, err := ms.ExistConversation(conversation.Uid, conversation.ChannelId, conversation.ChannelType)
		if err != nil {
			return err
		}
		if exist {
			continue
		}

		// 如果会话不存在 则写入
		if err := ms.writeConversation(conversation, batch); err != nil {
			return err
		}
	}

	if len(userBatchMap) == 0 {
		return nil
	}

	batchs := make([]*Batch, 0, len(userBatchMap))
	for _, batch := range userBatchMap {
		batchs = append(batchs, batch)
	}

	return Commits(batchs)
}

func (ms *mushanDB) AddOrUpdateConversationsWithUser(uid string, conversations []Conversation) error {
	ms.metrics.AddOrUpdateConversationsAdd(1)
	// ms.dblock.conversationLock.lock(uid)
	// defer ms.dblock.conversationLock.unlock(uid)
	if ms.opts.EnableCost {
		start := time.Now()
		defer func() {
			end := time.Since(start)
			if end > time.Millisecond*500 {
				ms.Info("AddOrUpdateConversations cost too long", zap.Duration("cost", end), zap.String("uid", uid), zap.Int("conversations", len(conversations)))
			}
		}()
	}

	batch := ms.sharedBatchDB(uid).NewBatch()

	for _, cn := range conversations {
		oldConversation, err := ms.GetConversation(uid, cn.ChannelId, cn.ChannelType)
		if err != nil && err != ErrNotFound {
			return err
		}

		exist := !IsEmptyConversation(oldConversation)

		// 如果会话存在 则删除旧的索引
		if exist {
			oldConversation.CreatedAt = nil
			err = ms.deleteConversationIndex(oldConversation, batch)
			if err != nil {
				return err
			}
			cn.Id = oldConversation.Id
		}

		if exist {
			cn.CreatedAt = nil // 更新时不更新创建时间
		}

		if err := ms.writeConversation(cn, batch); err != nil {
			return err
		}
	}

	err := ms.setConversationLocalUserRelation(conversations, false)
	if err != nil {
		return err
	}

	return batch.CommitWait()
}

// UpdateConversationDeletedAtMsgSeq 更新最近会话的已删除的消息序号位置
func (ms *mushanDB) UpdateConversationDeletedAtMsgSeq(uid string, channelId string, channelType uint8, deletedAtMsgSeq uint64) error {
	id, err := ms.getConversationIdByChannel(uid, channelId, channelType)
	if err != nil {
		return err
	}

	if id == 0 {
		return nil
	}
	w := ms.shardDB(uid).NewBatch()
	var deletedAtMsgSeqBytes = make([]byte, 8)
	ms.endian.PutUint64(deletedAtMsgSeqBytes, deletedAtMsgSeq)
	err = w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.DeletedAtMsgSeq), deletedAtMsgSeqBytes, ms.noSync)
	if err != nil {
		return err
	}
	return w.Commit(ms.sync)
}

func (ms *mushanDB) UpdateConversationIfSeqGreaterAsync(uid, channelId string, channelType uint8, readToMsgSeq uint64) error {

	existConversation, err := ms.GetConversation(uid, channelId, channelType)
	if err != nil {
		return err
	}
	if IsEmptyConversation(existConversation) {
		return nil
	}

	if existConversation.ReadToMsgSeq >= readToMsgSeq { // 如果当前readToMsgSeq大于或等于传过来的，则不需要更新
		return nil
	}

	w := ms.sharedBatchDB(uid).NewBatch()
	// readedToMsgSeq
	var msgSeqBytes = make([]byte, 8)
	ms.endian.PutUint64(msgSeqBytes, readToMsgSeq)
	w.Set(key.NewConversationColumnKey(uid, existConversation.Id, key.TableConversation.Column.ReadedToMsgSeq), msgSeqBytes)
	return w.Commit()
}

// GetConversations 获取指定用户的最近会话
func (ms *mushanDB) GetConversations(uid string) ([]Conversation, error) {

	ms.metrics.GetConversationsAdd(1)

	db := ms.shardDB(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewConversationPrimaryKey(uid, 0),
		UpperBound: key.NewConversationPrimaryKey(uid, math.MaxUint64),
	})
	defer iter.Close()

	var conversations []Conversation
	err := ms.iterateConversation(iter, func(conversation Conversation) bool {
		conversations = append(conversations, conversation)
		return true
	})
	if err != nil {
		return nil, err
	}
	return conversations, nil
}

func (ms *mushanDB) GetConversationsByType(uid string, tp ConversationType) ([]Conversation, error) {

	ms.metrics.GetConversationsByTypeAdd(1)

	db := ms.shardDB(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewConversationPrimaryKey(uid, 0),
		UpperBound: key.NewConversationPrimaryKey(uid, math.MaxUint64),
	})
	defer iter.Close()

	var conversations []Conversation
	err := ms.iterateConversation(iter, func(conversation Conversation) bool {
		if conversation.Type == tp {
			conversations = append(conversations, conversation)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	// 移除重复
	oldCount := len(conversations)
	conversations = removeDupliConversationByChannel(conversations)
	if oldCount != len(conversations) {
		ms.Warn("GetConversationsByType remove duplicate", zap.Int("oldCount", oldCount), zap.Int("newCount", len(conversations)))
	}
	return conversations, nil
}

func (ms *mushanDB) GetLastConversations(uid string, tp ConversationType, updatedAt uint64, excludeChannelTypes []uint8, limit int) ([]Conversation, error) {

	ms.metrics.GetLastConversationsAdd(1)

	ids, err := ms.getLastConversationIds(uid, updatedAt, limit)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	conversations := make([]Conversation, 0, len(ids))

	for _, id := range ids {
		conversation, err := ms.getConversation(uid, id)
		if err != nil && err != ErrNotFound {
			return nil, err
		}
		if err == ErrNotFound {
			continue
		}
		if conversation.Type != tp {
			continue
		}
		exclude := false

		if len(excludeChannelTypes) > 0 {
			for _, excludeChannelType := range excludeChannelTypes {
				if conversation.ChannelType == excludeChannelType {
					exclude = true
					break
				}
			}
		}
		if exclude {
			continue
		}

		conversations = append(conversations, conversation)
	}
	// conversations 根据id去重复
	conversations = uniqueConversation(conversations)

	// 按照更新时间排序
	sort.Slice(conversations, func(i, j int) bool {
		c1 := conversations[i]
		c2 := conversations[j]
		if c1.UpdatedAt == nil {
			return false
		}
		if c2.UpdatedAt == nil {
			return true
		}
		return c1.UpdatedAt.After(*c2.UpdatedAt)
	})

	return conversations, nil
}

func (ms *mushanDB) GetChannelConversationLocalUsers(channelId string, channelType uint8) ([]string, error) {

	db := ms.channelDb(channelId, channelType)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewConversationLocalUserLomsey(channelId, channelType),
		UpperBound: key.NewConversationLocalUserHighKey(channelId, channelType),
	})
	defer iter.Close()

	var users []string
	for iter.First(); iter.Valid(); iter.Next() {
		uid, err := key.ParseConversationLocalUserKey(iter.Key())
		if err != nil {
			return nil, err
		}
		users = append(users, uid)
	}
	return users, nil
}

func uniqueConversation(conversations []Conversation) []Conversation {
	if len(conversations) == 0 {
		return conversations
	}

	uniqueMap := make(map[uint64]Conversation)
	for _, conversation := range conversations {
		uniqueMap[conversation.Id] = conversation
	}

	var uniqueConversations = make([]Conversation, 0, len(uniqueMap))
	for _, conversation := range uniqueMap {
		uniqueConversations = append(uniqueConversations, conversation)
	}
	return uniqueConversations
}

func removeDupliConversationByChannel(conversations []Conversation) []Conversation {
	if len(conversations) == 0 {
		return conversations
	}

	uniqueMap := make(map[string]Conversation)
	for _, conversation := range conversations {
		uniqueMap[msutil.ChannelToKey(conversation.ChannelId, conversation.ChannelType)] = conversation
	}

	var uniqueConversations = make([]Conversation, 0, len(uniqueMap))
	for _, conversation := range uniqueMap {
		uniqueConversations = append(uniqueConversations, conversation)
	}
	return uniqueConversations
}

func (ms *mushanDB) getLastConversationIds(uid string, updatedAt uint64, limit int) ([]uint64, error) {
	db := ms.shardDB(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewConversationSecondIndexKey(uid, key.TableConversation.SecondIndex.UpdatedAt, updatedAt, 0),
		UpperBound: key.NewConversationSecondIndexKey(uid, key.TableConversation.SecondIndex.UpdatedAt, math.MaxUint64, math.MaxUint64),
	})
	defer iter.Close()

	var (
		ids = make([]uint64, 0)
	)

	for iter.Last(); iter.Valid(); iter.Prev() {
		id, _, _, err := key.ParseConversationSecondIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
		if limit > 0 && len(ids) >= limit {
			break
		}
	}

	// ids去重,并保留原来ids的顺序
	uniqueIds := make(map[uint64]struct{})
	uniqueIdsMap := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if _, ok := uniqueIds[id]; !ok {
			uniqueIds[id] = struct{}{}
			uniqueIdsMap = append(uniqueIdsMap, id)
		}
	}

	if len(ids) != len(uniqueIdsMap) {
		ms.Warn("getLastConversationIds duplicate ids", zap.Int("oldCount", len(ids)), zap.Int("newCount", len(uniqueIdsMap)))
	}

	return uniqueIdsMap, nil
}

// DeleteConversation 删除最近会话
func (ms *mushanDB) DeleteConversation(uid string, channelId string, channelType uint8) error {

	ms.metrics.DeleteConversationAdd(1)

	batch := ms.sharedBatchDB(uid).NewBatch()

	err := ms.deleteConversation(uid, channelId, channelType, batch)
	if err != nil {
		return err
	}

	if err := ms.deleteConversationLocalUserRelation(channelId, channelType, uid); err != nil {
		return err
	}

	return batch.CommitWait()

}

// DeleteConversations 批量删除最近会话
func (ms *mushanDB) DeleteConversations(uid string, channels []Channel) error {

	ms.metrics.DeleteConversationsAdd(1)

	batch := ms.sharedBatchDB(uid).NewBatch()

	for _, channel := range channels {
		err := ms.deleteConversation(uid, channel.ChannelId, channel.ChannelType, batch)
		if err != nil {
			return err
		}
	}

	err := ms.deleteConversationLocalUserRelationWithChannels(uid, channels)
	if err != nil {
		return err
	}

	return batch.CommitWait()
}

func (ms *mushanDB) SearchConversation(req ConversationSearchReq) ([]Conversation, error) {

	ms.metrics.SearchConversationAdd(1)

	if req.Uid != "" {
		return ms.GetConversations(req.Uid)
	}

	var conversations []Conversation
	currentSize := 0
	for _, db := range ms.dbs {
		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewConversationUidHashKey(0),
			UpperBound: key.NewConversationUidHashKey(math.MaxUint64),
		})
		defer iter.Close()

		err := ms.iterateConversation(iter, func(conversation Conversation) bool {
			if currentSize > req.Limit*req.CurrentPage { // 大于当前页的消息终止遍历
				return false
			}
			currentSize++
			if currentSize > (req.CurrentPage-1)*req.Limit && currentSize <= req.CurrentPage*req.Limit {
				conversations = append(conversations, conversation)
				return true
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}
	return conversations, nil
}

func (ms *mushanDB) deleteConversation(uid string, channelId string, channelType uint8, w *Batch) error {
	oldConversations, err := ms.getConversations(uid, channelId, channelType)
	if err != nil && err != ErrNotFound {
		return err
	}

	if len(oldConversations) == 0 {
		return nil
	}

	for _, oldConversation := range oldConversations {
		// 删除索引
		err = ms.deleteConversationIndex(oldConversation, w)
		if err != nil {
			return err
		}
		// 删除数据
		w.DeleteRange(key.NewConversationColumnKey(uid, oldConversation.Id, key.MinColumnKey), key.NewConversationColumnKey(uid, oldConversation.Id, key.MaxColumnKey))
	}
	return nil
}

// GetConversation 获取指定用户的指定会话
func (ms *mushanDB) GetConversation(uid string, channelId string, channelType uint8) (Conversation, error) {

	ms.metrics.GetConversationAdd(1)

	id, err := ms.getConversationIdByChannel(uid, channelId, channelType)
	if err != nil {
		return EmptyConversation, err
	}

	if id == 0 {
		return EmptyConversation, ErrNotFound
	}

	iter := ms.shardDB(uid).NewIter(&pebble.IterOptions{
		LowerBound: key.NewConversationColumnKey(uid, id, key.MinColumnKey),
		UpperBound: key.NewConversationColumnKey(uid, id, key.MaxColumnKey),
	})
	defer iter.Close()

	var conversation = EmptyConversation
	err = ms.iterateConversation(iter, func(cn Conversation) bool {
		conversation = cn
		return false
	})
	if err != nil {
		return EmptyConversation, err
	}

	if conversation == EmptyConversation {
		return EmptyConversation, ErrNotFound
	}

	return conversation, nil
}

// getConversations 获取指定用户的指定会话(有可能一个用户一个频道存在多条数据，这个应该是bug导致的，所以这里一起返回，给上层删除)
func (ms *mushanDB) getConversations(uid string, channelId string, channelType uint8) ([]Conversation, error) {

	db := ms.shardDB(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewConversationPrimaryKey(uid, 0),
		UpperBound: key.NewConversationPrimaryKey(uid, math.MaxUint64),
	})
	defer iter.Close()

	var conversations []Conversation
	err := ms.iterateConversation(iter, func(conversation Conversation) bool {
		if conversation.ChannelId == channelId && conversation.ChannelType == channelType {
			conversations = append(conversations, conversation)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return conversations, nil
}

func (ms *mushanDB) ExistConversation(uid string, channelId string, channelType uint8) (bool, error) {

	ms.metrics.ExistConversationAdd(1)

	idBytes, closer, err := ms.shardDB(uid).Get(key.NewConversationIndexChannelKey(uid, channelId, channelType))
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	defer closer.Close()

	if len(idBytes) == 0 {
		return false, nil
	}
	return true, nil
}

func (ms *mushanDB) getConversation(uid string, id uint64) (Conversation, error) {
	iter := ms.shardDB(uid).NewIter(&pebble.IterOptions{
		LowerBound: key.NewConversationColumnKey(uid, id, key.MinColumnKey),
		UpperBound: key.NewConversationColumnKey(uid, id, key.MaxColumnKey),
	})
	defer iter.Close()

	var conversation = EmptyConversation
	err := ms.iterateConversation(iter, func(cn Conversation) bool {
		conversation = cn
		return false
	})
	if err != nil {
		return EmptyConversation, err
	}

	if conversation == EmptyConversation {
		return EmptyConversation, ErrNotFound
	}

	return conversation, nil
}

func (ms *mushanDB) getConversationIdByChannel(uid string, channelId string, channelType uint8) (uint64, error) {
	idBytes, closer, err := ms.shardDB(uid).Get(key.NewConversationIndexChannelKey(uid, channelId, channelType))
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	return ms.endian.Uint64(idBytes), nil
}

func (ms *mushanDB) writeConversation(conversation Conversation, w *Batch) error {
	var (
		err error
	)

	id := conversation.Id
	uid := conversation.Uid
	// uid
	w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.Uid), []byte(uid))

	// channelId
	w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.ChannelId), []byte(conversation.ChannelId))

	// channelType
	w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.ChannelType), []byte{conversation.ChannelType})

	// type
	w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.Type), []byte{byte(conversation.Type)})

	// unreadCount
	var unreadCountBytes = make([]byte, 4)
	ms.endian.PutUint32(unreadCountBytes, conversation.UnreadCount)
	w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.UnreadCount), unreadCountBytes)

	// readedToMsgSeq
	var msgSeqBytes = make([]byte, 8)
	ms.endian.PutUint64(msgSeqBytes, conversation.ReadToMsgSeq)
	w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.ReadedToMsgSeq), msgSeqBytes)
	// createdAt
	if conversation.CreatedAt != nil {
		createdAtBytes := make([]byte, 8)
		createdAt := uint64(conversation.CreatedAt.UnixNano())
		ms.endian.PutUint64(createdAtBytes, createdAt)
		w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.CreatedAt), createdAtBytes)
	}

	if conversation.UpdatedAt != nil {
		// updatedAt
		updatedAtBytes := make([]byte, 8)
		updatedAt := uint64(conversation.UpdatedAt.UnixNano())
		ms.endian.PutUint64(updatedAtBytes, updatedAt)
		w.Set(key.NewConversationColumnKey(uid, id, key.TableConversation.Column.UpdatedAt), updatedAtBytes)
	}

	// write index
	if err = ms.writeConversationIndex(conversation, w); err != nil {
		return err
	}

	return nil
}

func (ms *mushanDB) writeConversationIndex(conversation Conversation, w *Batch) error {

	idBytes := make([]byte, 8)
	ms.endian.PutUint64(idBytes, conversation.Id)

	// channel index
	w.Set(key.NewConversationIndexChannelKey(conversation.Uid, conversation.ChannelId, conversation.ChannelType), idBytes)

	//  type second index
	w.Set(key.NewConversationSecondIndexKey(conversation.Uid, key.TableConversation.SecondIndex.Type, uint64(conversation.Type), conversation.Id), nil)

	if conversation.CreatedAt != nil {
		// createdAt second index
		w.Set(key.NewConversationSecondIndexKey(conversation.Uid, key.TableConversation.SecondIndex.CreatedAt, uint64(conversation.CreatedAt.UnixNano()), conversation.Id), nil)
	}

	if conversation.UpdatedAt != nil {
		// updatedAt second index
		w.Set(key.NewConversationSecondIndexKey(conversation.Uid, key.TableConversation.SecondIndex.UpdatedAt, uint64(conversation.UpdatedAt.UnixNano()), conversation.Id), nil)
	}

	return nil
}

func (ms *mushanDB) deleteConversationIndex(conversation Conversation, w *Batch) error {
	// channel index
	w.Delete(key.NewConversationIndexChannelKey(conversation.Uid, conversation.ChannelId, conversation.ChannelType))

	// type second index
	w.Delete(key.NewConversationSecondIndexKey(conversation.Uid, key.TableConversation.SecondIndex.Type, uint64(conversation.Type), conversation.Id))

	if conversation.CreatedAt != nil {
		// createdAt second index
		w.Delete(key.NewConversationSecondIndexKey(conversation.Uid, key.TableConversation.SecondIndex.CreatedAt, uint64(conversation.CreatedAt.UnixNano()), conversation.Id))
	}

	if conversation.UpdatedAt != nil {
		// updatedAt second index
		w.Delete(key.NewConversationSecondIndexKey(conversation.Uid, key.TableConversation.SecondIndex.UpdatedAt, uint64(conversation.UpdatedAt.UnixNano()), conversation.Id))
	}

	return nil
}

func (ms *mushanDB) iterateConversation(iter *pebble.Iterator, iterFnc func(conversation Conversation) bool) error {
	var (
		preId           uint64
		preConversation Conversation
		lastNeedAppend  bool = true
		hasData         bool = false
	)

	for iter.First(); iter.Valid(); iter.Next() {

		id, columnName, err := key.ParseConversationColumnKey(iter.Key())
		if err != nil {
			return err
		}
		if preId != id {
			if preId != 0 {
				if !iterFnc(preConversation) {
					lastNeedAppend = false
					break
				}
			}

			preId = id
			preConversation = Conversation{
				Id: id,
			}
		}
		switch columnName {
		case key.TableConversation.Column.Uid:
			preConversation.Uid = string(iter.Value())
		case key.TableConversation.Column.Type:
			preConversation.Type = ConversationType(iter.Value()[0])
		case key.TableConversation.Column.ChannelId:
			preConversation.ChannelId = string(iter.Value())
		case key.TableConversation.Column.ChannelType:
			preConversation.ChannelType = iter.Value()[0]
		case key.TableConversation.Column.UnreadCount:
			preConversation.UnreadCount = ms.endian.Uint32(iter.Value())
		case key.TableConversation.Column.ReadedToMsgSeq:
			preConversation.ReadToMsgSeq = ms.endian.Uint64(iter.Value())
		case key.TableConversation.Column.CreatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preConversation.CreatedAt = &t
			}

		case key.TableConversation.Column.UpdatedAt:
			tm := int64(ms.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preConversation.UpdatedAt = &t
			}

		case key.TableConversation.Column.DeletedAtMsgSeq:
			preConversation.DeletedAtMsgSeq = ms.endian.Uint64(iter.Value())

		}
		hasData = true
	}
	if lastNeedAppend && hasData {
		_ = iterFnc(preConversation)
	}

	return nil
}

// 设置最近会话用户关系
func (ms *mushanDB) setConversationLocalUserRelation(conversations []Conversation, commitWait bool) error {

	// 按照频道分组
	batchMap := make(map[string]*Batch)
	for _, conversation := range conversations {
		batch := batchMap[conversation.Uid]
		if batch == nil {
			batch = ms.channelBatchDb(conversation.ChannelId, conversation.ChannelType).NewBatch()
			batchMap[conversation.Uid] = batch
		}
		batch.Set(key.NewConversationLocalUserKey(conversation.ChannelId, conversation.ChannelType, conversation.Uid), nil)
	}

	batchs := make([]*Batch, 0, len(batchMap))
	for _, batch := range batchMap {
		batchs = append(batchs, batch)
	}

	if commitWait {
		return Commits(batchs)
	} else {
		for _, batch := range batchs {
			err := batch.Commit()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ms *mushanDB) deleteConversationLocalUserRelation(channelId string, channelType uint8, uid string) error {
	batch := ms.channelBatchDb(channelId, channelType).NewBatch()
	batch.Delete(key.NewConversationLocalUserKey(channelId, channelType, uid))

	return batch.CommitWait()
}

func (ms *mushanDB) deleteConversationLocalUserRelationWithChannels(uid string, channels []Channel) error {
	batch := ms.sharedBatchDB(uid).NewBatch()
	for _, channel := range channels {
		batch.Delete(key.NewConversationLocalUserKey(channel.ChannelId, channel.ChannelType, uid))
	}
	return batch.CommitWait()
}
