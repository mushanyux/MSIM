package plugin

import (
	"errors"

	"github.com/mushanyux/MSIM/internal/service"
	"github.com/mushanyux/MSIM/internal/types/pluginproto"
	"github.com/mushanyux/msrpc"
	"go.uber.org/zap"
)

func (a *rpc) clusterConfig(c *msrpc.Context) {
	nodes := service.Cluster.Nodes()
	slots := service.Cluster.Slots()

	cfg := pluginproto.ClusterConfig{}

	respNodes := make([]*pluginproto.Node, 0, len(service.Cluster.Nodes()))
	for _, node := range nodes {
		respNodes = append(respNodes, &pluginproto.Node{
			Id:            node.Id,
			ClusterAddr:   node.ClusterAddr,
			ApiServerAddr: node.ApiServerAddr,
			Online:        node.Online,
		})
	}

	respSlots := make([]*pluginproto.Slot, 0, len(slots))
	for _, slot := range slots {
		respSlots = append(respSlots, &pluginproto.Slot{
			Id:       slot.Id,
			Leader:   slot.Leader,
			Term:     slot.Term,
			Replicas: slot.Replicas,
		})
	}

	cfg.Nodes = respNodes
	cfg.Slots = respSlots

	data, err := cfg.Marshal()
	if err != nil {
		a.Error("ClusterConfig marshal failed", zap.Error(err))
		c.WriteErr(err)
		return
	}

	c.Write(data)
}

func (a *rpc) clusterChannelBelongNode(c *msrpc.Context) {
	req := &pluginproto.ClusterChannelBelongNodeReq{}
	err := req.Unmarshal(c.Body())
	if err != nil {
		a.Error("ClusterChannelBelongNodeReq unmarshal failed", zap.Error(err))
		c.WriteErr(err)
		return
	}

	if len(req.Channels) == 0 {
		c.WriteErr(errors.New("channels is empty"))
		return
	}

	nodeChannelsMap := make(map[uint64][]*pluginproto.Channel)
	for _, channel := range req.Channels {
		if channel.ChannelId == "" {
			c.WriteErr(errors.New("channelId is empty"))
			return
		}
		leaderId, err := service.Cluster.LeaderIdOfChannel(channel.ChannelId, uint8(channel.ChannelType))
		if err != nil {
			a.Error("LeaderOfChannel failed", zap.Error(err))
			c.WriteErr(err)
			return
		}
		nodeChannelsMap[leaderId] = append(nodeChannelsMap[leaderId], channel)
	}

	batchResp := &pluginproto.ClusterChannelBelongNodeBatchResp{}
	for nodeId, channels := range nodeChannelsMap {
		resp := &pluginproto.ClusterChannelBelongNodeResp{
			NodeId:   nodeId,
			Channels: channels,
		}
		batchResp.ClusterChannelBelongNodeResps = append(batchResp.ClusterChannelBelongNodeResps, resp)
	}

	data, err := batchResp.Marshal()
	if err != nil {
		a.Error("ClusterChannelBelongNodeResp marshal failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	c.Write(data)
}
