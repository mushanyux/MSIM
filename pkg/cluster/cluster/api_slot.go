package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mushanyux/MSIM/pkg/auth"
	"github.com/mushanyux/MSIM/pkg/auth/resource"
	"github.com/mushanyux/MSIM/pkg/mshttp"
	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/mushanyux/MSIM/pkg/network"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (s *Server) slotsGet(c *mshttp.Context) {
	slotIdStrs := c.Query("ids")
	slotIds := make([]uint32, 0)
	if slotIdStrs != "" {
		slotIdStrArray := strings.Split(slotIdStrs, ",")
		for _, slotIdStr := range slotIdStrArray {
			slotId, err := strconv.ParseUint(slotIdStr, 10, 32)
			if err != nil {
				s.Error("slotId parse error", zap.Error(err))
				c.ResponseError(err)
				return
			}
			slotIds = append(slotIds, uint32(slotId))
		}
	}
	slotInfos := make([]*SlotResp, 0, len(slotIds))

	for _, slotId := range slotIds {
		slotInfo, err := s.getSlotInfo(slotId)
		if err != nil {
			s.Error("getSlotInfo error", zap.Error(err))
			c.ResponseError(err)
			return
		}
		slotInfos = append(slotInfos, slotInfo)
	}
	c.JSON(http.StatusOK, slotInfos)
}

func (s *Server) slotClusterConfigGet(c *mshttp.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		s.Error("id parse error", zap.Error(err))
		c.ResponseError(err)
		return
	}
	slotId := uint32(id)

	slot := s.cfgServer.Slot(slotId)
	if slot == nil {
		s.Error("slot not found", zap.Uint32("slotId", slotId))
		c.ResponseError(err)
		return
	}

	leaderLogMaxIndex, err := s.getSlotMaxLogIndex(slotId)
	if err != nil {
		s.Error("getSlotMaxLogIndex error", zap.Error(err))
		c.ResponseError(err)
		return
	}
	appliedIdx, err := s.slotServer.AppliedIndex(slotId)
	if err != nil {
		s.Error("getAppliedIndex error", zap.Error(err))
		c.ResponseError(err)
		return
	}
	lastIdx, err := s.slotServer.LastIndex(slotId)
	if err != nil {
		s.Error("LastIndex error", zap.Error(err))
		c.ResponseError(err)
		return
	}

	cfg := NewSlotClusterConfigRespFromClusterConfig(appliedIdx, lastIdx, leaderLogMaxIndex, slot)
	c.JSON(http.StatusOK, cfg)

}

// func (s *Server) slotChannelsGet(c *mshttp.Context) {
// 	idStr := c.Param("id")
// 	id, err := strconv.ParseUint(idStr, 10, 32)
// 	if err != nil {
// 		s.Error("id parse error", zap.Error(err))
// 		c.ResponseError(err)
// 		return
// 	}
// 	slotId := uint32(id)

// 	channels, err := s.opts.ChannelClusterStorage.GetWithSlotId(slotId)
// 	if err != nil {
// 		s.Error("GetChannels error", zap.Error(err))
// 		c.ResponseError(err)
// 		return
// 	}
// 	channelClusterConfigResps := make([]*ChannelClusterConfigResp, 0, len(channels))
// 	for _, cfg := range channels {
// 		if !msutil.ArrayContainsUint64(cfg.Replicas, s.opts.NodeId) {
// 			continue
// 		}
// 		slot := s.clusterEventServer.Slot(slotId)
// 		if slot == nil {
// 			s.Error("slot not found", zap.Uint32("slotId", slotId))
// 			c.ResponseError(err)
// 			return
// 		}
// 		shardNo := msutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
// 		lastMsgSeq, lastAppendTime, err := s.opts.MessageLogStorage.LastIndexAndAppendTime(shardNo)
// 		if err != nil {
// 			s.Error("LastIndexAndAppendTime error", zap.Error(err))
// 			c.ResponseError(err)
// 			return
// 		}

// 		resp := NewChannelClusterConfigRespFromClusterConfig(slot.Leader, slot.Id, cfg)
// 		resp.LastMessageSeq = lastMsgSeq
// 		if lastAppendTime > 0 {
// 			resp.LastAppendTime = msutil.ToyyyyMMddHHmm(time.Unix(int64(lastAppendTime/1e9), 0))
// 		}
// 		channelClusterConfigResps = append(channelClusterConfigResps, resp)
// 	}

// 	c.JSON(http.StatusOK, ChannelClusterConfigRespTotal{
// 		Total: len(channelClusterConfigResps),
// 		Data:  channelClusterConfigResps,
// 	})
// }

func (s *Server) slotMigrate(c *mshttp.Context) {
	var req struct {
		MigrateFrom uint64 `json:"migrate_from"` // 迁移的原节点
		MigrateTo   uint64 `json:"migrate_to"`   // 迁移的目标节点
	}

	if !s.opts.Auth.HasPermissionWithContext(c, resource.Slot.Migrate, auth.ActionWrite) {
		c.ResponseErrorWithStatus(http.StatusForbidden, errors.New("没有权限"))
		return
	}

	bodyBytes, err := BindJSON(&req, c)
	if err != nil {
		s.Error("bind json error", zap.Error(err))
		c.ResponseError(err)
		return
	}
	idStr := c.Param("id")
	id := msutil.ParseUint32(idStr)

	slot := s.cfgServer.Slot(id)
	if slot == nil {
		s.Error("slot not found", zap.Uint32("slotId", id))
		c.ResponseError(errors.New("slot not found"))
		return
	}

	node := s.cfgServer.Node(slot.Leader)
	if node == nil {
		s.Error("leader not found", zap.Uint64("leaderId", slot.Leader))
		c.ResponseError(errors.New("leader not found"))
		return
	}

	if slot.Leader != s.opts.ConfigOptions.NodeId {
		c.ForwardWithBody(fmt.Sprintf("%s%s", node.ApiServerAddr, c.Request.URL.Path), bodyBytes)
		return
	}

	if !msutil.ArrayContainsUint64(slot.Replicas, req.MigrateFrom) {
		c.ResponseError(errors.New("MigrateFrom not in replicas"))
		return
	}
	if msutil.ArrayContainsUint64(slot.Replicas, req.MigrateTo) && req.MigrateFrom != slot.Leader {
		c.ResponseError(errors.New("transition between followers is not supported"))
		return
	}

	if req.MigrateFrom == 0 || req.MigrateTo == 0 {
		c.ResponseError(errors.New("migrateFrom or migrateTo is 0"))
		return
	}

	if req.MigrateFrom == req.MigrateTo {
		c.ResponseError(errors.New("migrateFrom is equal to migrateTo"))
		return
	}

	// err = s.clusterEventServer.ProposeMigrateSlot(id, req.MigrateFrom, req.MigrateTo)
	// if err != nil {
	// 	s.Error("slotMigrate: ProposeMigrateSlot error", zap.Error(err))
	// 	c.ResponseError(err)
	// 	return
	// }

	c.ResponseOK()

}

func (s *Server) allSlotsGet(c *mshttp.Context) {
	leaderId := s.cfgServer.LeaderId()
	if leaderId == 0 {
		c.ResponseError(errors.New("leader not found"))
		return
	}
	if leaderId != s.opts.ConfigOptions.NodeId {
		leaderNode := s.cfgServer.Node(leaderId)
		c.Forward(fmt.Sprintf("%s%s", leaderNode.ApiServerAddr, c.Request.URL.Path))
		return
	}

	clusterCfg := s.cfgServer.GetClusterConfig()
	resps := make([]*SlotResp, 0, len(clusterCfg.Slots))

	nodeSlotsMap := make(map[uint64][]uint32)
	for _, st := range clusterCfg.Slots {
		if st.Leader == s.opts.ConfigOptions.NodeId {
			slotInfo, err := s.getSlotInfo(st.Id)
			if err != nil {
				s.Error("getSlotInfo error", zap.Error(err))
				c.ResponseError(err)
				return
			}
			resps = append(resps, slotInfo)
			continue
		}
		nodeSlotsMap[st.Leader] = append(nodeSlotsMap[st.Leader], st.Id)
	}

	timeoutCtx, cancel := context.WithTimeout(s.cancelCtx, time.Second*10)
	defer cancel()
	requestGroup, _ := errgroup.WithContext(timeoutCtx)

	for nodeId, slotIds := range nodeSlotsMap {

		if !s.cfgServer.NodeIsOnline(nodeId) {
			slotResps, err := s.getSlotInfoForLeaderOffline(slotIds)
			if err != nil {
				s.Error("getSlotInfoForLeaderOffline error", zap.Error(err))
				c.ResponseError(err)
				return
			}
			resps = append(resps, slotResps...)
			continue
		}

		requestGroup.Go(func(nId uint64, sIds []uint32) func() error {
			return func() error {
				slotResps, err := s.requestSlotInfo(nId, sIds, c.CopyRequestHeader(c.Request))
				if err != nil {
					return err
				}
				resps = append(resps, slotResps...)
				return nil
			}
		}(nodeId, slotIds))
	}

	err := requestGroup.Wait()
	if err != nil {
		s.Error("requestSlotInfo error", zap.Error(err))
		c.ResponseError(err)
		return
	}
	sort.Slice(resps, func(i, j int) bool {
		return resps[i].Id < resps[j].Id
	})
	c.JSON(http.StatusOK, SlotRespTotal{
		Total: len(resps),
		Data:  resps,
	})
}

func (s *Server) getSlotInfo(slotId uint32) (*SlotResp, error) {
	slot := s.cfgServer.Slot(slotId)
	if slot == nil {
		return nil, errors.New("slot not found")
	}
	// count, err := s.opts.ChannelClusterStorage.GetCountWithSlotId(slotId)
	// if err != nil {
	// 	return nil, err
	// }
	lastIdx, err := s.slotServer.LastIndex(slotId)
	if err != nil {
		return nil, err
	}
	resp := NewSlotResp(slot, 0)
	resp.LogIndex = lastIdx
	return resp, nil
}

func (s *Server) getSlotMaxLogIndex(slotId uint32) (uint64, error) {

	slot := s.cfgServer.Slot(slotId)
	if slot == nil {
		return 0, errors.New("slot not found")
	}

	if slot.Leader == s.opts.ConfigOptions.NodeId {
		lastIdx, err := s.slotServer.LastIndex(slotId)
		if err != nil {
			return 0, err
		}
		return lastIdx, nil
	}

	slotLogResp, err := s.rpcClient.RequestSlotLastLogInfo(slot.Leader, &SlotLogInfoReq{
		SlotIds: []uint32{slotId},
	})
	if err != nil {
		s.Error("requestSlotLogInfo error", zap.Error(err))
		return 0, err
	}
	if len(slotLogResp.Slots) > 0 {
		return slotLogResp.Slots[0].LogIndex, nil
	}
	return 0, nil
}

func (s *Server) slotReplicasGet(c *mshttp.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		s.Error("id parse error", zap.Error(err))
		c.ResponseError(err)
		return
	}
	slotId := uint32(id)

	slot := s.cfgServer.Slot(slotId)
	if slot == nil {
		s.Error("slot not found", zap.Uint32("slotId", slotId))
		c.ResponseError(errors.New("slot not found"))
		return
	}
	if slot.Leader == 0 {
		s.Error("slot leader not found", zap.Uint32("slotId", slotId))
		c.ResponseError(errors.New("slot leader not found"))
		return
	}
	slotNodeLeader := s.cfgServer.Node(slot.Leader)
	if slotNodeLeader == nil {
		s.Error("slot leader node not found", zap.Uint64("nodeId", slot.Leader))
		c.ResponseError(errors.New("slot leader node not found"))
		return
	}

	if slotNodeLeader.Id != s.opts.ConfigOptions.NodeId {
		c.Forward(fmt.Sprintf("%s%s", slotNodeLeader.ApiServerAddr, c.Request.URL.Path))
		return
	}
	replicas := make([]*slotReplicaDetailResp, 0, len(slot.Replicas))
	replicasLock := &sync.Mutex{}
	replicaIds := make([]uint64, 0, len(slot.Replicas)+len(slot.Learners))
	replicaIds = append(replicaIds, slot.Replicas...)
	replicaIds = append(replicaIds, slot.Learners...)

	timeoutCtx, cancel := context.WithTimeout(s.cancelCtx, s.opts.ReqTimeout)
	defer cancel()
	requestGroup, _ := errgroup.WithContext(timeoutCtx)

	for _, replicaId := range replicaIds {
		replicaId := replicaId

		if !s.NodeIsOnline(replicaId) {
			continue
		}
		lastLog, err := s.slotServer.LastLog(slotId)
		if err != nil {
			s.Error("LastLog error", zap.Error(err))
			c.ResponseError(err)
			return
		}
		if replicaId == s.opts.ConfigOptions.NodeId {
			slotRaft := s.slotServer.GetSlotRaft(slotId)
			running := 0
			var leaderId uint64
			var term uint32
			var confVersion uint64
			if slotRaft != nil {
				running = 1
				leaderId = slotRaft.LeaderId()
				term = slotRaft.Config().Term
				confVersion = slotRaft.Config().Version
			}

			replicaResp := slotReplicaResp{
				ReplicaId:   s.opts.ConfigOptions.NodeId,
				LeaderId:    leaderId,
				Term:        term,
				ConfVersion: confVersion,
				Running:     running,
				LastLogSeq:  lastLog.Index,
			}
			if lastLog.Time.IsZero() {
				replicaResp.LastLogTime = 0
			} else {
				replicaResp.LastLogTime = uint64(lastLog.Time.UnixNano())
			}
			lastLogTimeFormat := ""
			if replicaResp.LastLogTime != 0 {
				lastLogTimeFormat = myUptime(time.Since(time.Unix(int64(replicaResp.LastLogTime/1e9), 0)))
			}
			replicasLock.Lock()
			replicas = append(replicas, &slotReplicaDetailResp{
				slotReplicaResp:   replicaResp,
				Role:              s.getReplicaRoleWithSlotClusterConfig(slot, replicaId),
				RoleFormat:        s.getReplicaRoleFormatWithSlotClusterConfig(slot, replicaId),
				LastLogTimeFormat: lastLogTimeFormat,
			})
			replicasLock.Unlock()
			continue
		}

		requestGroup.Go(func() error {
			replicaResp, err := s.requestSlotLocalReplica(replicaId, slotId, c.CopyRequestHeader(c.Request))
			if err != nil {
				return err
			}

			lastLogTimeFormat := ""
			if replicaResp.LastLogTime != 0 {
				lastLogTimeFormat = myUptime(time.Since(time.Unix(int64(replicaResp.LastLogTime/1e9), 0)))
			}

			replicasLock.Lock()
			replicas = append(replicas, &slotReplicaDetailResp{
				slotReplicaResp:   *replicaResp,
				Role:              s.getReplicaRoleWithSlotClusterConfig(slot, replicaId),
				RoleFormat:        s.getReplicaRoleFormatWithSlotClusterConfig(slot, replicaId),
				LastLogTimeFormat: lastLogTimeFormat,
			})
			replicasLock.Unlock()
			return nil
		})
	}
	err = requestGroup.Wait()
	if err != nil {
		s.Error("slotReplicas requestGroup error", zap.Error(err))
		c.ResponseError(err)
		return
	}

	c.JSON(http.StatusOK, replicas)
}

func (s *Server) slotLocalReplicaGet(c *mshttp.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		s.Error("id parse error", zap.Error(err))
		c.ResponseError(err)
		return
	}
	slotId := uint32(id)

	slot := s.cfgServer.Slot(slotId)
	if slot == nil {
		s.Error("slot not found", zap.Uint32("slotId", slotId))
		c.ResponseError(errors.New("slot not found"))
		return
	}
	lastLog, err := s.slotServer.LastLog(slotId)
	if err != nil {
		s.Error("LastLog error", zap.Error(err))
		c.ResponseError(err)
		return
	}
	slotRaft := s.slotServer.GetSlotRaft(slotId)
	running := 0
	var leaderId uint64
	var term uint32
	var confVersion uint64
	if slotRaft != nil {
		running = 1
		leaderId = slotRaft.LeaderId()
		term = slotRaft.Config().Term
		confVersion = slotRaft.Config().Version
	}

	replicaResp := slotReplicaResp{
		ReplicaId:   s.opts.ConfigOptions.NodeId,
		LeaderId:    leaderId,
		Term:        term,
		ConfVersion: confVersion,
		Running:     running,
		LastLogSeq:  lastLog.Index,
	}
	if lastLog.Time.IsZero() {
		replicaResp.LastLogTime = 0
	} else {
		replicaResp.LastLogTime = uint64(lastLog.Time.UnixNano())
	}

	c.JSON(http.StatusOK, replicaResp)
}

func (s *Server) requestSlotLocalReplica(nodeId uint64, slotId uint32, headers map[string]string) (*slotReplicaResp, error) {
	node := s.cfgServer.Node(nodeId)
	if node == nil {
		s.Error("requestSlotLocalReplica failed, node not found", zap.Uint64("nodeId", nodeId))
		return nil, errors.New("node not found")
	}
	fullUrl := fmt.Sprintf("%s%s", node.ApiServerAddr, s.formatPath(fmt.Sprintf("/slots/%d/localReplica", slotId)))
	resp, err := network.Get(fullUrl, nil, headers)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("requestSlotLocalReplica failed, status code: %d", resp.StatusCode)
	}

	var replicaResp *slotReplicaResp
	err = msutil.ReadJSONByByte([]byte(resp.Body), &replicaResp)
	if err != nil {
		return nil, err
	}
	return replicaResp, nil
}
