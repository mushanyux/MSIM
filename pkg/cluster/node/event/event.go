package event

import "github.com/mushanyux/MSIM/pkg/cluster/node/types"

type IEvent interface {

	// OnSlotElection 槽选举
	OnSlotElection(slots []*types.Slot) error
}
