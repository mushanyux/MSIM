package manager

import (
	"sync"
	"time"

	"github.com/lni/goutils/syncutil"
	"github.com/mushanyux/MSIM/internal/types"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/valyala/fastrand"
	"go.uber.org/zap"
)

type tagBucket struct {
	tag struct {
		m map[string]*types.Tag
		sync.RWMutex
	}
	index   int
	expire  time.Duration
	channel struct {
		sync.RWMutex
		m map[string]string
	}
	stopper *syncutil.Stopper
	mslog.Log

	existTagFnc func(tagKey string) bool
}

func newtagBucket(index int, expire time.Duration, existTagFnc func(tagKey string) bool) *tagBucket {
	b := &tagBucket{
		index:       index,
		expire:      expire,
		stopper:     syncutil.NewStopper(),
		Log:         mslog.NewMSLog("tagBucket"),
		existTagFnc: existTagFnc,
	}
	b.channel.m = make(map[string]string)
	b.tag.m = make(map[string]*types.Tag)
	return b
}

func (b *tagBucket) start() error {
	b.stopper.RunWorker(b.loop)
	return nil
}

func (b *tagBucket) stop() {
	b.stopper.Stop()
}

func (b *tagBucket) loop() {
	scanInterval := b.expire / 2

	p := float64(fastrand.Uint32()) / (1 << 32)
	// 以避免系统中因定时器、周期性任务或请求间隔完全一致而导致的同步问题（例如拥堵或资源竞争）。
	jitter := time.Duration(p * float64(scanInterval))
	tk := time.NewTicker(scanInterval + jitter)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			b.checkExpireTags()
		case <-b.stopper.ShouldStop():
			return
		}
	}
}

func (b *tagBucket) checkExpireTags() {
	b.tag.Lock()
	// tag过期检查
	var removeTags []string // 需要移除的tagKey
	for _, tag := range b.tag.m {
		if time.Since(tag.LastGetTime) > b.expire {
			if removeTags == nil {
				removeTags = make([]string, 0, 20)
			}
			removeTags = append(removeTags, tag.Key)
		}
	}

	if len(removeTags) > 0 {
		for _, removeTagKey := range removeTags {
			delete(b.tag.m, removeTagKey)
		}
		b.Info("checkExpireTags: remove tags", zap.Int("count", len(removeTags)), zap.String("removeTag", removeTags[0]))

	}
	b.tag.Unlock()

	// channel tag过期检查
	b.channel.Lock()
	var removeChannels []string
	for channelKey, tagKey := range b.channel.m {
		if !b.existTagFnc(tagKey) {
			if removeChannels == nil {
				removeChannels = make([]string, 0, 20)
			}
			removeChannels = append(removeChannels, channelKey)
		}
	}

	if len(removeChannels) > 0 {
		for _, removeChannelKey := range removeChannels {
			delete(b.channel.m, removeChannelKey)
		}
		b.Info("checkExpireTags: remove channels", zap.Int("count", len(removeChannels)), zap.String("removeChannel", removeChannels[0]))
	}
	b.channel.Unlock()
}

func (b *tagBucket) setTag(tag *types.Tag) {
	b.tag.Lock()
	b.tag.m[tag.Key] = tag
	b.tag.Unlock()
}

func (b *tagBucket) getTag(tagKey string) *types.Tag {
	b.tag.RLock()
	defer b.tag.RUnlock()
	return b.tag.m[tagKey]
}

func (b *tagBucket) removeTag(tagKey string) {
	b.tag.Lock()
	defer b.tag.Unlock()
	delete(b.tag.m, tagKey)
}

func (b *tagBucket) existTag(tagKey string) bool {
	b.tag.RLock()
	defer b.tag.RUnlock()
	_, ok := b.tag.m[tagKey]
	return ok
}

func (b *tagBucket) setChannelTag(channelId string, channelType uint8, tagKey string) {
	b.channel.Lock()
	b.channel.m[msutil.ChannelToKey(channelId, channelType)] = tagKey
	b.channel.Unlock()
}

func (b *tagBucket) removeChannelTag(channelId string, channelType uint8) {
	b.channel.Lock()
	delete(b.channel.m, msutil.ChannelToKey(channelId, channelType))
	b.channel.Unlock()
}

func (b *tagBucket) getChannelTag(channelId string, channelType uint8) string {
	b.channel.RLock()
	defer b.channel.RUnlock()
	return b.channel.m[msutil.ChannelToKey(channelId, channelType)]
}

func (b *tagBucket) getAllTags() []*types.Tag {
	b.tag.RLock()
	defer b.tag.RUnlock()
	tags := make([]*types.Tag, 0, len(b.tag.m))
	for _, tag := range b.tag.m {
		tags = append(tags, tag)
	}
	return tags
}

func (b *tagBucket) getAllChannelTags() map[string]string {
	b.channel.RLock()
	defer b.channel.RUnlock()
	channelTags := make(map[string]string)
	for k, v := range b.channel.m {
		channelTags[k] = v
	}
	return channelTags
}
