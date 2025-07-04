package eventbus

import (
	"fmt"

	"github.com/mushanyux/MSIM/internal/track"
	msproto "github.com/mushanyux/MSIMGoProto"
)

type EventType uint8

const (
	EventUnknown EventType = iota
	// =================== 用户事件 ===================
	// EventConnect 连接事件
	EventConnect
	// EventConnack 连接ack，连接结果事件
	EventConnack
	// EventOnSend 收到发送消息
	EventOnSend
	// EventConnWriteFrame 写入frame
	EventConnWriteFrame
	// EventConnClose 连接关闭(移除逻辑连接并关闭真实连接)
	EventConnClose

	// EventConnRemove 连接移除, 仅仅只是移除本节点上的逻辑连接，
	EventConnRemove
	//	EventConnLeaderRemove 移除leader节点上的连接
	EventConnLeaderRemove

	// =================== 频道事件 ===================
	// EventChannelOnSend 频道收到发送消息
	EventChannelOnSend
	// EventChannelOnStream 频道收到流消息
	EventChannelOnStream
	// EventChannelWebhook 频道webhook
	EventChannelWebhook
	// EventChannelDistribute 频道消息分发
	EventChannelDistribute

	// =================== Pusher ===================
	// EventPushOnline push在线消息
	EventPushOnline
	// EventPushOffline push离线消息
	EventPushOffline
)

func (e EventType) String() string {
	switch e {
	case EventConnect:
		return "EventConnect"
	case EventConnack:
		return "EventConnack"
	case EventOnSend:
		return "EventOnSend"
	case EventConnWriteFrame:
		return "EventConnWriteFrame"
	case EventConnClose:
		return "EventConnClose"
	case EventConnRemove:
		return "EventConnRemove"
	case EventChannelOnSend:
		return "EventChannelOnSend"
	case EventChannelOnStream:
		return "EventChannelOnStream"
	case EventChannelWebhook:
		return "EventChannelWebhook"
	case EventChannelDistribute:
		return "EventChannelDistribute"
	case EventPushOnline:
		return "EventPushOnline"
	case EventPushOffline:
		return "EventPushOffline"
	}
	return fmt.Sprintf("EventType(%d)", e)
}

func (e EventType) Uint8() uint8 {
	return uint8(e)
}

type Event struct {
	Type         EventType
	Conn         *Conn
	Frame        msproto.Frame
	MessageId    int64
	MessageSeq   uint64
	StreamNo     string             // 流号编号
	StreamFlag   msproto.StreamFlag // 流消息标记
	ReasonCode   msproto.ReasonCode
	TagKey       string // tag的key
	ToUid        string // 发送事件的目标用户
	SourceNodeId uint64 // 事件发起源节点
	// 事件记录
	Track track.Message
	// 不需要编码
	Index        uint64
	OfflineUsers []string // 离线用户集合
	ChannelId    string   // 频道ID
	ChannelType  uint8    // 频道类型
	ReqId        string   // 请求ID(非必填)(jsonrpc)
}

func (e *Event) Clone() *Event {
	return &Event{
		Type:         e.Type,
		Conn:         e.Conn,
		Frame:        e.Frame,
		MessageId:    e.MessageId,
		MessageSeq:   e.MessageSeq,
		StreamNo:     e.StreamNo,
		StreamFlag:   e.StreamFlag,
		ReasonCode:   e.ReasonCode,
		TagKey:       e.TagKey,
		ToUid:        e.ToUid,
		SourceNodeId: e.SourceNodeId,
		Track:        e.Track.Clone(),
		Index:        e.Index,
		OfflineUsers: e.OfflineUsers,
		ChannelId:    e.ChannelId,
		ChannelType:  e.ChannelType,
		ReqId:        e.ReqId,
	}
}

func (e *Event) Size() uint64 {
	size := uint64(0)
	size += 1 // flag
	size += 1 // type
	if e.hasConn() == 1 {
		size += e.Conn.Size()
	}
	if e.hasFrame() == 1 {
		size += 4 + uint64(e.Frame.GetFrameSize())
	}
	size += 8                           // message id
	size += 8                           // message seq
	size += uint64(2 + len(e.StreamNo)) // stream no
	size += 1                           // stream flag
	size += 1                           // reason code
	size += uint64(2 + len(e.TagKey))   // tag key
	size += uint64(2 + len(e.ToUid))    // to uid
	size += 8                           // source node id

	if e.hasTrack() == 1 {
		size += e.Track.Size()
	}

	if e.hasChannel() == 1 {
		size += uint64(2 + len(e.ChannelId)) // channel id
		size += 1                            // channel type
	}

	if e.hasReqId() == 1 {
		size += uint64(2 + len(e.ReqId)) // req id
	}

	return size
}

func (e Event) encodeWithEcoder(enc *msproto.Encoder) error {
	var flag uint8 = e.hasConn()<<7 | e.hasFrame()<<6 | e.hasTrack()<<5 | e.hasChannel()<<4 | e.hasReqId()<<3
	enc.WriteUint8(flag)

	enc.WriteUint8(e.Type.Uint8())
	if e.hasConn() == 1 {
		data, err := e.Conn.Encode()
		if err != nil {
			return err
		}
		enc.WriteBinary(data)
	}
	if e.hasFrame() == 1 {
		data, err := Proto.EncodeFrame(e.Frame, msproto.LatestVersion)
		if err != nil {
			return err
		}
		enc.WriteUint32(uint32(len(data)))
		enc.WriteBytes(data)
	}

	enc.WriteInt64(e.MessageId)
	enc.WriteUint64(e.MessageSeq)
	enc.WriteString(e.StreamNo)
	enc.WriteUint8(uint8(e.StreamFlag))
	enc.WriteUint8(uint8(e.ReasonCode))
	enc.WriteString(e.TagKey)
	enc.WriteString(e.ToUid)
	enc.WriteUint64(e.SourceNodeId)

	if e.hasTrack() == 1 {
		enc.WriteBinary(e.Track.Encode())
	}

	if e.hasChannel() == 1 {
		enc.WriteString(e.ChannelId)
		enc.WriteUint8(e.ChannelType)
	}

	if e.hasReqId() == 1 {
		enc.WriteString(e.ReqId)
	}

	return nil
}

func (e *Event) decodeWithDecoder(dec *msproto.Decoder) error {
	flag, err := dec.Uint8()
	if err != nil {
		return err
	}
	hasConn := (flag >> 7) & 0x01
	hasFrame := (flag >> 6) & 0x01
	hasTrack := (flag >> 5) & 0x01
	hasChannel := (flag >> 4) & 0x01
	hasReqId := (flag >> 3) & 0x01
	typeUint8, err := dec.Uint8()
	if err != nil {
		return err
	}
	e.Type = EventType(typeUint8)

	if hasConn == 1 {
		data, err := dec.Binary()
		if err != nil {
			return err
		}

		conn := &Conn{}
		err = conn.Decode(data)
		if err != nil {
			return err
		}
		e.Conn = conn
	}

	if hasFrame == 1 {
		frameLen, err := dec.Uint32()
		if err != nil {
			return err
		}
		frameData, err := dec.Bytes(int(frameLen))
		if err != nil {
			return err
		}
		frame, _, err := Proto.DecodeFrame(frameData, msproto.LatestVersion)
		if err != nil {
			return err
		}
		e.Frame = frame

	}

	e.MessageId, err = dec.Int64()
	if err != nil {
		return err
	}
	if e.MessageSeq, err = dec.Uint64(); err != nil {
		return err
	}

	if e.StreamNo, err = dec.String(); err != nil {
		return err
	}

	var streamFlag uint8
	if streamFlag, err = dec.Uint8(); err != nil {
		return err
	}
	e.StreamFlag = msproto.StreamFlag(streamFlag)

	var reasonCode uint8
	if reasonCode, err = dec.Uint8(); err != nil {
		return err
	}
	e.ReasonCode = msproto.ReasonCode(reasonCode)

	if e.TagKey, err = dec.String(); err != nil {
		return err
	}
	if e.ToUid, err = dec.String(); err != nil {
		return err
	}
	if e.SourceNodeId, err = dec.Uint64(); err != nil {
		return err
	}

	if hasTrack == 1 {
		trackData, err := dec.Binary()
		if err != nil {
			return err
		}
		err = e.Track.Decode(trackData)
		if err != nil {
			return err
		}
	}
	if hasChannel == 1 {
		if e.ChannelId, err = dec.String(); err != nil {
			return err
		}
		var channelType uint8
		if channelType, err = dec.Uint8(); err != nil {
			return err
		}
		e.ChannelType = channelType
	}

	if hasReqId == 1 {
		if e.ReqId, err = dec.String(); err != nil {
			return err
		}
	}

	return nil
}

func (e Event) hasConn() uint8 {
	if e.Conn != nil {
		return 1
	}
	return 0
}

func (e Event) hasFrame() uint8 {
	if e.Frame != nil {
		return 1
	}
	return 0
}
func (e *Event) hasTrack() uint8 {
	if e.Track.HasData() {
		return 1
	}
	return 0
}

func (e *Event) hasChannel() uint8 {
	if len(e.ChannelId) > 0 {
		return 1
	}
	return 0
}

func (e *Event) hasReqId() uint8 {
	if e.ReqId != "" {
		return 1
	}
	return 0
}

type EventBatch []*Event

func (e EventBatch) Encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	count := len(e)
	enc.WriteUint32(uint32(count))

	for _, ev := range e {
		err := ev.encodeWithEcoder(enc)
		if err != nil {
			return nil, err
		}
	}
	return enc.Bytes(), nil
}

func (e *EventBatch) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	count, err := dec.Uint32()
	if err != nil {
		return err
	}
	for i := 0; i < int(count); i++ {
		ev := &Event{}
		err := ev.decodeWithDecoder(dec)
		if err != nil {
			return err
		}
		*e = append(*e, ev)
	}
	return nil
}

// 用户事件处理者
type UserEventHandler interface {
	// OnEvent 事件处理
	OnEvent(ctx *UserContext)
}

// 频道事件
type ChannelEventHandler interface {
	// OnEvent 事件处理
	OnEvent(ctx *ChannelContext)
}

// push事件
type PushEventHandler interface {
	// OnEvent 事件处理
	OnEvent(ctx *PushContext)
}

type EventMsgRange uint32

const (
	//  user的最小消息类型
	// [min,max)
	UserEventMsgMin EventMsgRange = 2000
	//  user的最大消息类型, 不包含max
	UserEventMsgMax EventMsgRange = 3000

	// channel的最小消息类型
	// [min,max)
	ChannelEventMsgMin EventMsgRange = 3001
	//  channel的最大消息类型, 不包含max
	ChannelEventMsgMax EventMsgRange = 4000

	// push的最小消息类型
	// [min,max)
	PushEventMsgMin EventMsgRange = 4001
	//  push的最大消息类型, 不包含max
	PushEventMsgMax EventMsgRange = 5000
)
