package msdb

import (
	"github.com/cockroachdb/pebble"
	"github.com/lni/goutils/syncutil"
)

type shardb struct {
	*pebble.DB
	stopper        *syncutil.Stopper
	appendMessageC chan AppendMessagesReq

	msdb DB
}

//func newShardb(db *pebble.DB, msdb DB) *shardb {
//	return &shardb{
//		DB:             db,
//		appendMessageC: make(chan AppendMessagesReq, 1024),
//		stopper:        syncutil.NewStopper(),
//		msdb:           msdb,
//	}
//}
//
//// 异步增加消息
//func (s *shardb) appendMessageAsync(req AppendMessagesReq) {
//	select {
//	case s.appendMessageC <- req:
//	case <-s.stopper.ShouldStop():
//		return
//	}
//}
//
//func (s *shardb) writeLoop() {
//	for {
//		select {
//		case req := <-s.appendMessageC:
//
//		case <-s.stopper.ShouldStop():
//			return
//		}
//	}
//}
