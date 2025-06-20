package msdb

import (
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/mushanyux/MSIM/pkg/msdb/key"
	msproto "github.com/mushanyux/MSIMGoProto"
)

func (ms *mushanDB) AddStreamMeta(streamMeta *StreamMeta) error {
	db := ms.shardDB(streamMeta.StreamNo)
	batch := db.NewBatch()
	defer batch.Close()

	keyBytes := key.NewStreamMetaKey(streamMeta.StreamNo)
	valueBytes := streamMeta.Encode()

	return db.Set(keyBytes, valueBytes, ms.sync)
}

func (ms *mushanDB) GetStreamMeta(streamNo string) (*StreamMeta, error) {
	db := ms.shardDB(streamNo)
	keyBytes := key.NewStreamMetaKey(streamNo)
	valueBytes, closer, err := db.Get(keyBytes)
	defer closer.Close()
	if err != nil {
		return nil, err
	}

	if len(valueBytes) == 0 {
		return nil, nil
	}

	streamMeta := &StreamMeta{}
	if err := streamMeta.Decode(valueBytes); err != nil {
		return nil, err
	}
	return streamMeta, nil
}

func (ms *mushanDB) AddStream(stream *Stream) error {
	db := ms.shardDB(stream.StreamNo)
	batch := db.NewBatch()
	defer batch.Close()

	keyBytes := key.NewStreamIndexKey(stream.StreamNo, stream.StreamId)
	valueBytes := stream.Encode()

	return db.Set(keyBytes, valueBytes, ms.sync)
}

func (ms *mushanDB) AddStreams(streams []*Stream) error {
	for _, stream := range streams {
		if err := ms.AddStream(stream); err != nil {
			return err
		}
	}
	return nil
}

func (ms *mushanDB) GetStreams(streamNo string) ([]*Stream, error) {
	db := ms.shardDB(streamNo)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewStreamIndexKey(streamNo, 0),
		UpperBound: key.NewStreamIndexKey(streamNo, math.MaxUint64),
	})
	defer iter.Close()

	var streams []*Stream
	for iter.First(); iter.Valid(); iter.Next() {
		stream := &Stream{}
		if err := stream.Decode(iter.Value()); err != nil {
			return nil, err
		}
		streams = append(streams, stream)
	}
	return streams, nil
}

type StreamMeta struct {
	version     int16 // 数据版本
	StreamNo    string
	ChannelId   string
	ChannelType uint8
	FromUid     string
	ClientMsgNo string
	MessageId   int64
	MessageSeq  int64
}

func (s *StreamMeta) Encode() []byte {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteInt16(int(s.version))
	enc.WriteString(s.StreamNo)
	enc.WriteString(s.ChannelId)
	enc.WriteUint8(s.ChannelType)
	enc.WriteString(s.FromUid)
	enc.WriteString(s.ClientMsgNo)
	enc.WriteInt64(s.MessageId)
	enc.WriteInt64(s.MessageSeq)
	return enc.Bytes()
}

func (s *StreamMeta) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	var err error
	if s.version, err = dec.Int16(); err != nil {
		return err
	}
	if s.StreamNo, err = dec.String(); err != nil {
		return err
	}
	if s.ChannelId, err = dec.String(); err != nil {
		return err
	}
	if s.ChannelType, err = dec.Uint8(); err != nil {
		return err
	}
	if s.FromUid, err = dec.String(); err != nil {
		return err
	}
	if s.ClientMsgNo, err = dec.String(); err != nil {
		return err
	}
	if s.MessageId, err = dec.Int64(); err != nil {
		return err
	}
	if s.MessageSeq, err = dec.Int64(); err != nil {
		return err
	}
	return nil
}

type Stream struct {
	version  int16 // 数据版本
	StreamNo string
	StreamId uint64
	Payload  []byte
}

func (s *Stream) Encode() []byte {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteInt16(int(s.version))
	enc.WriteUint64(s.StreamId)
	enc.WriteString(s.StreamNo)
	enc.WriteBytes(s.Payload)
	return enc.Bytes()
}

func (s *Stream) Decode(data []byte) error {
	dec := msproto.NewDecoder(data)
	var err error
	if s.version, err = dec.Int16(); err != nil {
		return err
	}

	if s.StreamId, err = dec.Uint64(); err != nil {
		return err
	}

	if s.StreamNo, err = dec.String(); err != nil {
		return err
	}
	if s.Payload, err = dec.BinaryAll(); err != nil {
		return err
	}
	return nil
}
