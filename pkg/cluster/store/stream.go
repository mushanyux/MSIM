package store

import "github.com/mushanyux/MSIM/pkg/msdb"

func (s *Store) AddStreamMeta(streamMeta *msdb.StreamMeta) error {
	data := EncodeCMDAddStreamMeta(streamMeta)
	cmd := NewCMD(CMDAddStreamMeta, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return err
	}
	slotId := s.opts.Slot.GetSlotId(streamMeta.ChannelId)
	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	return err
}

func (s *Store) GetStreamMeta(streamNo string) (*msdb.StreamMeta, error) {

	return s.wdb.GetStreamMeta(streamNo)
}

func (s *Store) AddStreams(channelId string, channelType uint8, streams []*msdb.Stream) error {
	data := EncodeCMDAddStreams(streams)
	cmd := NewCMD(CMDAddStreams, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return err
	}
	slotId := s.opts.Slot.GetSlotId(channelId)
	_, err = s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	return err
}

func (s *Store) GetStreams(streamNo string) ([]*msdb.Stream, error) {
	return s.wdb.GetStreams(streamNo)
}
