package raftgroup

import "github.com/mushanyux/MSIM/pkg/raft/types"

type Event struct {
	RaftKey string
	types.Event
	WaitC chan error
}
