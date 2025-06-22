package raft

import "github.com/mushanyux/MSIM/pkg/raft/types"

type stepReq struct {
	event types.Event
	resp  chan error
}
