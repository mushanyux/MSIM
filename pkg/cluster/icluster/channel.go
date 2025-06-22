package icluster

import (
	"context"

	"github.com/mushanyux/MSIM/pkg/raft/types"
)

type Channel interface {
	// ProposeBatchUntilAppliedTimeout 批量提按等待应用完成
	ProposeBatchUntilAppliedTimeout(ctx context.Context, channelId string, channelType uint8, reqs types.ProposeReqSet) (types.ProposeRespSet, error)
}
