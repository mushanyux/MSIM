package ingress

import (
	"github.com/mushanyux/MSIM/pkg/msutil"
	msproto "github.com/mushanyux/MSIMGoProto"
)

type TagReq struct {
	TagKey      string
	ChannelId   string
	ChannelType uint8
	NodeId      uint64 // 获取属于指定节点的uids
}

func (t *TagReq) encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	enc.WriteString(t.TagKey)
	enc.WriteString(t.ChannelId)
	enc.WriteUint8(t.ChannelType)
	enc.WriteUint64(t.NodeId)
	return enc.Bytes(), nil
}

func (t *TagReq) decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	var err error
	if t.TagKey, err = dec.String(); err != nil {
		return err
	}
	if t.ChannelId, err = dec.String(); err != nil {
		return err
	}
	if t.ChannelType, err = dec.Uint8(); err != nil {
		return err
	}
	if t.NodeId, err = dec.Uint64(); err != nil {
		return err
	}
	return nil
}

type TagResp struct {
	TagKey string
	Uids   []string
}

func (t *TagResp) encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteString(t.TagKey)
	enc.WriteUint32(uint32(len(t.Uids)))
	for _, uid := range t.Uids {
		enc.WriteString(uid)
	}
	return enc.Bytes(), nil
}

func (t *TagResp) decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	tagKey, err := dec.String()
	if err != nil {
		return err
	}
	t.TagKey = tagKey
	count, err := dec.Uint32()
	if err != nil {
		return err
	}
	t.Uids = make([]string, 0, count)
	for i := 0; i < int(count); i++ {
		var uid string
		if uid, err = dec.String(); err != nil {
			return err
		}
		t.Uids = append(t.Uids, uid)
	}
	return nil
}

type AllowSendReq struct {
	From string // 发送者
	To   string // 接收者
}

func (a *AllowSendReq) decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	var err error
	if a.From, err = dec.String(); err != nil {
		return err
	}
	if a.To, err = dec.String(); err != nil {
		return err
	}
	return nil
}

func (a *AllowSendReq) encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteString(a.From)
	enc.WriteString(a.To)
	return enc.Bytes(), nil
}

type TagUpdateReq struct {
	TagKey      string
	ChannelId   string
	ChannelType uint8
	Uids        []string
	Remove      bool // 是否是移除uids
	ChannelTag  bool // 是否是频道tag
}

func (t *TagUpdateReq) Encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	enc.WriteString(t.TagKey)
	enc.WriteString(t.ChannelId)
	enc.WriteUint8(t.ChannelType)
	enc.WriteUint32(uint32(len(t.Uids)))
	for _, uid := range t.Uids {
		enc.WriteString(uid)
	}
	enc.WriteUint8(msutil.BoolToUint8(t.Remove))
	enc.WriteUint8(msutil.BoolToUint8(t.ChannelTag))
	return enc.Bytes(), nil
}

func (t *TagUpdateReq) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	var err error
	if t.TagKey, err = dec.String(); err != nil {
		return err
	}
	if t.ChannelId, err = dec.String(); err != nil {
		return err
	}
	if t.ChannelType, err = dec.Uint8(); err != nil {
		return err
	}
	count, err := dec.Uint32()
	if err != nil {
		return err
	}
	for i := 0; i < int(count); i++ {
		uid, err := dec.String()
		if err != nil {
			return err
		}
		t.Uids = append(t.Uids, uid)
	}
	remove, err := dec.Uint8()
	if err != nil {
		return err
	}
	t.Remove = msutil.Uint8ToBool(remove)
	channelTag, err := dec.Uint8()
	if err != nil {
		return err
	}
	t.ChannelTag = msutil.Uint8ToBool(channelTag)
	return nil
}

type TagAddReq struct {
	TagKey string
	Uids   []string
}

func (t *TagAddReq) Encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteString(t.TagKey)
	enc.WriteUint32(uint32(len(t.Uids)))
	for _, uid := range t.Uids {
		enc.WriteString(uid)
	}
	return enc.Bytes(), nil
}

func (t *TagAddReq) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	var err error
	if t.TagKey, err = dec.String(); err != nil {
		return err
	}
	count, err := dec.Uint32()
	if err != nil {
		return err
	}
	for i := 0; i < int(count); i++ {
		uid, err := dec.String()
		if err != nil {
			return err
		}
		t.Uids = append(t.Uids, uid)
	}
	return nil
}

type ChannelReq struct {
	ChannelId   string
	ChannelType uint8
}

func (c *ChannelReq) Encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	enc.WriteString(c.ChannelId)
	enc.WriteUint8(c.ChannelType)
	return enc.Bytes(), nil
}

func (c *ChannelReq) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	var err error
	if c.ChannelId, err = dec.String(); err != nil {
		return err
	}
	if c.ChannelType, err = dec.Uint8(); err != nil {
		return err
	}
	return nil
}

type SubscribersResp struct {
	Subscribers []string
}

func (s *SubscribersResp) Encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteUint32(uint32(len(s.Subscribers)))
	for _, uid := range s.Subscribers {
		enc.WriteString(uid)
	}
	return enc.Bytes(), nil
}

func (s *SubscribersResp) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	count, err := dec.Uint32()
	if err != nil {
		return err
	}
	s.Subscribers = make([]string, 0, count)
	for i := 0; i < int(count); i++ {
		uid, err := dec.String()
		if err != nil {
			return err
		}
		s.Subscribers = append(s.Subscribers, uid)
	}
	return nil
}

type StreamReq struct {
	StreamNos []string
}

func (s *StreamReq) Encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	defer enc.End()

	enc.WriteUint32(uint32(len(s.StreamNos)))
	for _, streamNo := range s.StreamNos {
		enc.WriteString(streamNo)
	}
	return enc.Bytes(), nil
}

func (s *StreamReq) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	count, err := dec.Uint32()
	if err != nil {
		return err
	}
	s.StreamNos = make([]string, 0, count)
	for i := 0; i < int(count); i++ {
		streamNo, err := dec.String()
		if err != nil {
			return err
		}
		s.StreamNos = append(s.StreamNos, streamNo)
	}
	return nil
}

type StreamResp struct {
	Streams []*Stream
}

type Stream struct {
	StreamNo string
	StreamId uint64
	Payload  []byte
}

func (s *StreamResp) Encode() ([]byte, error) {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteUint32(uint32(len(s.Streams)))
	for _, stream := range s.Streams {
		enc.WriteString(stream.StreamNo)
		enc.WriteUint64(stream.StreamId)
		enc.WriteUint32(uint32(len(stream.Payload)))
		enc.WriteBytes(stream.Payload)
	}
	return enc.Bytes(), nil
}

func (s *StreamResp) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	count, err := dec.Uint32()
	if err != nil {
		return err
	}
	s.Streams = make([]*Stream, 0, count)
	for i := 0; i < int(count); i++ {
		stream := &Stream{}
		if stream.StreamNo, err = dec.String(); err != nil {
			return err
		}
		if stream.StreamId, err = dec.Uint64(); err != nil {
			return err
		}
		payloadLen, err := dec.Uint32()
		if err != nil {
			return err
		}
		if stream.Payload, err = dec.Bytes(int(payloadLen)); err != nil {
			return err
		}
		s.Streams = append(s.Streams, stream)
	}
	return nil
}
