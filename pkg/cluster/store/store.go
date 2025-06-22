package store

import (
	"github.com/lni/goutils/syncutil"
	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/mslog"
)

type Store struct {
	opts *Options
	mslog.Log

	wdb msdb.DB

	channelCfgCh chan *channelCfgReq
	stopper      *syncutil.Stopper
}

func New(opts *Options) *Store {
	s := &Store{
		opts:         opts,
		Log:          mslog.NewMSLog("store"),
		wdb:          opts.DB,
		channelCfgCh: make(chan *channelCfgReq, 2048),
		stopper:      syncutil.NewStopper(),
	}

	return s
}

func (s *Store) NextPrimaryKey() uint64 {
	return s.wdb.NextPrimaryKey()
}

func (s *Store) DB() msdb.DB {
	return s.wdb
}

func (s *Store) Start() error {
	for i := 0; i < 50; i++ {
		go s.loopSaveChannelClusterConfig()
	}
	// s.stopper.RunWorker(s.loopSaveChannelClusterConfig)
	return nil
}

func (s *Store) Stop() {
	s.stopper.Stop()
}

type channelCfgReq struct {
	cfg   msdb.ChannelClusterConfig
	errCh chan error
}
