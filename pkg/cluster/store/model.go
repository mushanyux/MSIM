package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/msutil"
	msproto "github.com/mushanyux/MSIMGoProto"
)

var ErrStoreStopped = fmt.Errorf("store stopped")

type CMDType uint16

const (
	CMDUnknown CMDType = iota
	// 添加设备
	CMDAddDevice
	// 更新设备
	CMDUpdateDevice
	// 添加用户
	CMDAddUser
	// 更新用户
	CMDUpdateUser
	// 添加频道
	CMDAddChannelInfo
	// 更新频道
	CMDUpdateChannelInfo
	// 添加订阅者
	CMDAddSubscribers
	// 移除订阅者
	CMDRemoveSubscribers
	// 移除所有订阅者
	CMDRemoveAllSubscriber
	// 删除频道
	CMDDeleteChannel
	// 添加黑名单
	CMDAddDenylist
	// 移除黑名单
	CMDRemoveDenylist
	// 移除所有黑名单
	CMDRemoveAllDenylist
	// 添加白名单
	CMDAddAllowlist
	// 移除白名单
	CMDRemoveAllowlist
	// 移除所有白名单
	CMDRemoveAllAllowlist
	// 追加消息
	// CMDAppendMessages
	// 追加用户消息
	// CMDAppendMessagesOfUser
	// 追加通知队列消息
	CMDAppendMessagesOfNotifyQueue
	// 移除通知队列消息
	CMDRemoveMessagesOfNotifyQueue
	// 删除频道并清空消息
	CMDDeleteChannelAndClearMessages
	// 添加或更新指定用户的最近会话
	CMDAddOrUpdateUserConversations
	// 删除会话
	CMDDeleteConversation
	// 批量删除会话
	CMDDeleteConversations
	// 添加系统UID
	CMDSystemUIDsAdd
	// 移除系统UID
	CMDSystemUIDsRemove
	// 保存流元数据
	CMDSaveStreamMeta
	// 流结束
	CMDStreamEnd
	// 追加流元素
	CMDAppendStreamItem
	// 频道分布式配置保存
	CMDChannelClusterConfigSave
	// 频道分布式配置删除
	CMDChannelClusterConfigDelete

	// 批量更新最近会话
	CMDBatchUpdateConversation
	// 添加流元数据
	CMDAddStreamMeta
	// 添加流元数据
	CMDAddStreams
	// 批量添加最近会话
	CMDAddOrUpdateConversations
	// 添加或更新测试机
	CMDAddOrUpdateTester
	// 移除测试机
	CMDRemoveTester
	// 更新用户插件号
	CMDUpdateUserPluginNo
	// 添加或更新插件
	CMDAddOrUpdatePlugin
	// 更新插件配置
	CMDUpdatePluginConfig
	// 移除插件用户
	CMDRemovePluginUser
	// 批量添加最近会话如果存在则不添加
	CMDAddOrUpdateConversationsBatchIfNotExist
	// 更新最近会话的已删除的消息序号位置
	CMDUpdateConversationDeletedAtMsgSeq
)

func (c CMDType) Uint16() uint16 {
	return uint16(c)
}

func (c CMDType) String() string {
	switch c {
	case CMDAddDevice:
		return "CMDAddDevice"
	case CMDUpdateDevice:
		return "CMDUpdateDevice"
	case CMDAddUser:
		return "CMDAddUser"
	case CMDUpdateUser:
		return "CMDUpdateUser"
	case CMDAddChannelInfo:
		return "CMDAddChannelInfo"
	case CMDUpdateChannelInfo:
		return "CMDUpdateChannelInfo"
	case CMDAddSubscribers:
		return "CMDAddSubscribers"
	case CMDRemoveSubscribers:
		return "CMDRemoveSubscribers"
	case CMDRemoveAllSubscriber:
		return "CMDRemoveAllSubscriber"
	case CMDDeleteChannel:
		return "CMDDeleteChannel"
	case CMDAddDenylist:
		return "CMDAddDenylist"
	case CMDRemoveDenylist:
		return "CMDRemoveDenylist"
	case CMDRemoveAllDenylist:
		return "CMDRemoveAllDenylist"
	case CMDAddAllowlist:
		return "CMDAddAllowlist"
	case CMDRemoveAllowlist:
		return "CMDRemoveAllowlist"
	case CMDRemoveAllAllowlist:
		return "CMDRemoveAllAllowlist"
	// case CMDAppendMessages:
	// return "CMDAppendMessages"
	// case CMDAppendMessagesOfUser:
	// return "CMDAppendMessagesOfUser"
	case CMDAppendMessagesOfNotifyQueue:
		return "CMDAppendMessagesOfNotifyQueue"
	case CMDRemoveMessagesOfNotifyQueue:
		return "CMDRemoveMessagesOfNotifyQueue"
	case CMDDeleteChannelAndClearMessages:
		return "CMDDeleteChannelAndClearMessages"
	case CMDAddOrUpdateUserConversations:
		return "CMDAddOrUpdateUserConversations"
	case CMDDeleteConversation:
		return "CMDDeleteConversation"
	case CMDSystemUIDsAdd:
		return "CMDSystemUIDsAdd"
	case CMDSystemUIDsRemove:
		return "CMDSystemUIDsRemove"
	case CMDSaveStreamMeta:
		return "CMDSaveStreamMeta"
	case CMDStreamEnd:
		return "CMDStreamEnd"
	case CMDAppendStreamItem:
		return "CMDAppendStreamItem"
	case CMDChannelClusterConfigSave:
		return "CMDChannelClusterConfigSave"
	case CMDChannelClusterConfigDelete:
		return "CMDChannelClusterConfigDelete"
	case CMDBatchUpdateConversation:
		return "CMDBatchUpdateConversation"
	case CMDDeleteConversations:
		return "CMDDeleteConversations"
	case CMDAddStreamMeta:
		return "CMDAddStreamMeta"
	case CMDAddStreams:
		return "CMDAddStreams"
	case CMDAddOrUpdateConversations:
		return "CMDAddOrUpdateConversations"
	case CMDAddOrUpdateTester:
		return "CMDAddOrUpdateTester"
	case CMDRemoveTester:
		return "CMDRemoveTester"
	case CMDUpdateUserPluginNo:
		return "CMDUpdateUserPluginNo"
	case CMDAddOrUpdatePlugin:
		return "CMDAddOrUpdatePlugin"
	case CMDUpdatePluginConfig:
		return "CMDUpdatePluginConfig"
	case CMDRemovePluginUser:
		return "CMDRemovePluginUser"
	case CMDAddOrUpdateConversationsBatchIfNotExist:
		return "CMDAddOrUpdateConversationsBatchIfNotExist"
	case CMDUpdateConversationDeletedAtMsgSeq:
		return "CMDUpdateConversationDeletedAtMsgSeq"
	default:
		return fmt.Sprintf("CMDUnknown[%d]", c)
	}
}

type CMD struct {
	CmdType CMDType
	Data    []byte
	version CmdVersion // 数据协议版本

}

func NewCMD(cmdType CMDType, data []byte) *CMD {
	return &CMD{
		CmdType: cmdType,
		Data:    data,
	}
}

func NewCMDWithVersion(cmdType CMDType, data []byte, version CmdVersion) *CMD {
	return &CMD{
		CmdType: cmdType,
		Data:    data,
		version: version,
	}
}

func (c *CMD) Marshal() ([]byte, error) {
	c.version = 1
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteUint16(c.version.Uint16())
	enc.WriteUint16(c.CmdType.Uint16())
	enc.WriteBytes(c.Data)
	return enc.Bytes(), nil

}

func (c *CMD) Unmarshal(data []byte) error {
	dec := msproto.NewDecoder(data)
	var err error
	var version uint16
	if version, err = dec.Uint16(); err != nil {
		return err
	}
	c.version = CmdVersion(version)
	var cmdType uint16
	if cmdType, err = dec.Uint16(); err != nil {
		return err
	}
	c.CmdType = CMDType(cmdType)
	if c.Data, err = dec.BinaryAll(); err != nil {
		return err
	}
	return nil
}

func (c *CMD) CMDContent() (string, error) {
	switch c.CmdType {
	case CMDAddDevice:
		device, err := c.DecodeCMDDevice()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(device), nil
	case CMDUpdateDevice:
		device, err := c.DecodeCMDDevice()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(device), nil
	case CMDAddUser:
		user, err := c.DecodeCMDUser()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(user), nil
	case CMDUpdateUser:
		user, err := c.DecodeCMDUser()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(user), nil
	case CMDAddChannelInfo:
		channel, err := c.DecodeChannelInfo()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(channel), nil
	case CMDUpdateChannelInfo:
		channel, err := c.DecodeChannelInfo()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(channel), nil
	case CMDAddSubscribers:
		channelId, channelType, members, err := c.DecodeMembers()
		if err != nil {
			return "", err
		}
		uids := make([]string, 0, len(members))
		for _, member := range members {
			uids = append(uids, member.Uid)
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
			"uids":        uids,
		}), nil
	case CMDRemoveSubscribers:
		channelId, channelType, uids, err := c.DecodeChannelUids()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
			"uids":        uids,
		}), nil
	case CMDRemoveAllSubscriber:
		channelId, channelType, err := c.DecodeChannel()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
		}), nil

	case CMDDeleteChannel:
		channelId, channelType, err := c.DecodeChannel()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
		}), nil
	case CMDAddDenylist:
		channelId, channelType, members, err := c.DecodeMembers()
		if err != nil {
			return "", err
		}
		uids := make([]string, 0, len(members))
		for _, member := range members {
			uids = append(uids, member.Uid)
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
			"uids":        uids,
		}), nil

	case CMDRemoveDenylist:
		channelId, channelType, uids, err := c.DecodeChannelUids()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
			"uids":        uids,
		}), nil

	case CMDRemoveAllDenylist:
		channelId, channelType, err := c.DecodeChannel()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
		}), nil

	case CMDAddAllowlist:
		channelId, channelType, members, err := c.DecodeMembers()
		if err != nil {
			return "", err
		}
		uids := make([]string, 0, len(members))
		for _, member := range members {
			uids = append(uids, member.Uid)
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
			"uids":        uids,
		}), nil

	case CMDRemoveAllowlist:
		channelId, channelType, uids, err := c.DecodeChannelUids()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
			"uids":        uids,
		}), nil

	case CMDRemoveAllAllowlist:
		channelId, channelType, err := c.DecodeChannel()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
		}), nil

	case CMDDeleteChannelAndClearMessages:
		channelId, channelType, err := c.DecodeChannel()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"channelId":   channelId,
			"channelType": channelType,
		}), nil

	case CMDAddOrUpdateUserConversations:
		uid, conversations, err := c.DecodeCMDAddOrUpdateUserConversations()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"uid":           uid,
			"conversations": conversations,
		}), nil

	case CMDDeleteConversation:
		uid, channelId, channelType, err := c.DecodeCMDDeleteConversation()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"uid":         uid,
			"channelId":   channelId,
			"channelType": channelType,
		}), nil

	case CMDDeleteConversations:
		uid, channels, err := c.DecodeCMDDeleteConversations()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"uid":      uid,
			"channels": channels,
		}), nil

	case CMDSystemUIDsAdd:
		uids, err := c.DecodeCMDSystemUIDs()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(uids), nil

	case CMDSystemUIDsRemove:
		uids, err := c.DecodeCMDSystemUIDs()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(uids), nil

	case CMDBatchUpdateConversation:
		models, err := c.DecodeCMDBatchUpdateConversation()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(models), nil

	case CMDChannelClusterConfigSave:
		_, _, data, err := c.DecodeCMDChannelClusterConfigSave()
		if err != nil {
			return "", err
		}
		channelClusterConfig := msdb.ChannelClusterConfig{}
		err = channelClusterConfig.Unmarshal(data)
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(channelClusterConfig), nil
	case CMDAddOrUpdateConversations:
		conversations, err := c.DecodeCMDAddOrUpdateConversations()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(conversations), nil
	case CMDAddOrUpdatePlugin:
		plugin, err := c.DecodeCMDPlugin()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(plugin), nil
	case CMDUpdatePluginConfig:
		pluginNo, config, err := c.DecodeCMDPluginConfig()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"pluginNo": pluginNo,
			"config":   config,
		}), nil
	case CMDAddOrUpdateConversationsBatchIfNotExist:
		conversations, err := c.DecodeCMDAddOrUpdateConversations()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(conversations), nil
	case CMDUpdateConversationDeletedAtMsgSeq:
		uid, channelId, channelType, deletedAtMsgSeq, err := c.DecodeCMDUpdateConversationDeletedAtMsgSeq()
		if err != nil {
			return "", err
		}
		return msutil.ToJSON(map[string]interface{}{
			"uid":             uid,
			"channelId":       channelId,
			"channelType":     channelType,
			"deletedAtMsgSeq": deletedAtMsgSeq,
		}), nil
	}

	return "", nil
}

func EncodeMembers(channelId string, channelType uint8, members []msdb.Member) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(channelId)
	encoder.WriteUint8(channelType)
	encoder.WriteUint32(uint32(len(members)))
	if len(members) > 0 {
		for _, member := range members {
			memberData, err := member.Marshal()
			if err != nil {
				return nil
			}
			encoder.WriteBinary(memberData)
		}
	}
	return encoder.Bytes()
}

func (c *CMD) DecodeChannelUids() (channelId string, channelType uint8, uids []string, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if channelId, err = decoder.String(); err != nil {
		return
	}
	if channelType, err = decoder.Uint8(); err != nil {
		return
	}
	var count uint32
	if count, err = decoder.Uint32(); err != nil {
		return
	}
	for i := uint32(0); i < count; i++ {
		var uid string
		if uid, err = decoder.String(); err != nil {
			return
		}
		uids = append(uids, uid)
	}
	return
}

func EncodeChannelUids(channelId string, channelType uint8, uids []string) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(channelId)
	encoder.WriteUint8(channelType)
	encoder.WriteUint32(uint32(len(uids)))
	for _, uid := range uids {
		encoder.WriteString(uid)
	}
	return encoder.Bytes()
}

// DecodeMembers DecodeMembers
func (c *CMD) DecodeMembers() (channelId string, channelType uint8, members []msdb.Member, err error) {
	decoder := msproto.NewDecoder(c.Data)

	if channelId, err = decoder.String(); err != nil {
		return
	}

	if channelType, err = decoder.Uint8(); err != nil {
		return
	}

	var count uint32
	if count, err = decoder.Uint32(); err != nil {
		return
	}
	if count > 0 {
		for i := uint32(0); i < count; i++ {
			var memberBytes []byte
			if memberBytes, err = decoder.Binary(); err != nil {
				return
			}
			var member = &msdb.Member{}
			err = member.Unmarshal(memberBytes)
			if err != nil {
				return
			}
			members = append(members, *member)
		}
	}
	return
}

func (c *CMD) DecodeChannel() (channelId string, channelType uint8, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if channelId, err = decoder.String(); err != nil {
		return
	}
	if channelType, err = decoder.Uint8(); err != nil {
		return
	}
	return
}

func EncodeChannel(channelId string, channelType uint8) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(channelId)
	encoder.WriteUint8(channelType)
	return encoder.Bytes()
}

func EncodeCMDUser(u msdb.User) []byte {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteUint64(u.Id)
	enc.WriteString(u.Uid)
	if u.CreatedAt != nil {
		enc.WriteUint64(uint64(u.CreatedAt.UnixNano()))
	} else {
		enc.WriteUint64(0)
	}

	if u.UpdatedAt != nil {
		enc.WriteUint64(uint64(u.UpdatedAt.UnixNano()))
	} else {
		enc.WriteUint64(0)
	}
	return enc.Bytes()
}

func (c *CMD) DecodeCMDUser() (u msdb.User, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if u.Id, err = decoder.Uint64(); err != nil {
		return
	}
	if u.Uid, err = decoder.String(); err != nil {
		return
	}

	var createdAtUnixNano uint64
	if createdAtUnixNano, err = decoder.Uint64(); err != nil {
		return
	}
	if createdAtUnixNano > 0 {
		ct := time.Unix(int64(createdAtUnixNano/1e9), int64(createdAtUnixNano%1e9))
		u.CreatedAt = &ct
	}

	var updatedAtUnixNano uint64
	if updatedAtUnixNano, err = decoder.Uint64(); err != nil {
		return
	}
	if updatedAtUnixNano > 0 {
		ct := time.Unix(int64(updatedAtUnixNano/1e9), int64(updatedAtUnixNano%1e9))
		u.UpdatedAt = &ct
	}

	return
}

// EncodeCMDDevice EncodeCMDDevice
func EncodeCMDDevice(d msdb.Device) []byte {
	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteUint64(d.Id)
	enc.WriteString(d.Uid)
	enc.WriteUint64(d.DeviceFlag)
	enc.WriteUint8(d.DeviceLevel)
	enc.WriteString(d.Token)
	if d.CreatedAt != nil {
		enc.WriteUint64(uint64(d.CreatedAt.UnixNano()))
	} else {
		enc.WriteUint64(0)
	}

	if d.UpdatedAt != nil {
		enc.WriteUint64(uint64(d.UpdatedAt.UnixNano()))
	} else {
		enc.WriteUint64(0)
	}

	return enc.Bytes()
}

func (c *CMD) DecodeCMDDevice() (d msdb.Device, err error) {
	decoder := msproto.NewDecoder(c.Data)

	if d.Id, err = decoder.Uint64(); err != nil {
		return
	}

	if d.Uid, err = decoder.String(); err != nil {
		return
	}
	if d.DeviceFlag, err = decoder.Uint64(); err != nil {
		return
	}

	if d.DeviceLevel, err = decoder.Uint8(); err != nil {
		return
	}
	if d.Token, err = decoder.String(); err != nil {
		return
	}

	var createdAtUnixNano uint64
	if createdAtUnixNano, err = decoder.Uint64(); err != nil {
		return
	}
	if createdAtUnixNano > 0 {
		ct := time.Unix(int64(createdAtUnixNano/1e9), int64(createdAtUnixNano%1e9))
		d.CreatedAt = &ct
	}

	var updatedAtUnixNano uint64
	if updatedAtUnixNano, err = decoder.Uint64(); err != nil {
		return
	}
	if updatedAtUnixNano > 0 {
		ct := time.Unix(int64(updatedAtUnixNano/1e9), int64(updatedAtUnixNano%1e9))
		d.UpdatedAt = &ct
	}

	return
}

func EncodeCMDUserAndDevice(id uint64, uid string, deviceFlag msproto.DeviceFlag, deviceLevel msproto.DeviceLevel, token string, createdAt *time.Time, updatedAt *time.Time) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteUint64(id)
	encoder.WriteString(uid)
	encoder.WriteUint64(uint64(deviceFlag))
	encoder.WriteUint8(uint8(deviceLevel))
	encoder.WriteString(token)
	if createdAt != nil {
		encoder.WriteUint64(uint64(createdAt.UnixNano()))
	} else {
		encoder.WriteUint64(0)
	}

	if updatedAt != nil {
		encoder.WriteUint64(uint64(updatedAt.UnixNano()))
	} else {
		encoder.WriteUint64(0)
	}

	return encoder.Bytes()
}

func (c *CMD) DecodeCMDUserAndDevice() (id uint64, uid string, deviceFlag uint64, deviceLevel msproto.DeviceLevel, token string, createdAt *time.Time, updatedAt *time.Time, err error) {
	decoder := msproto.NewDecoder(c.Data)

	if id, err = decoder.Uint64(); err != nil {
		return
	}

	if uid, err = decoder.String(); err != nil {
		return
	}
	if deviceFlag, err = decoder.Uint64(); err != nil {
		return
	}
	var deviceLevelUint8 uint8
	if deviceLevelUint8, err = decoder.Uint8(); err != nil {
		return
	}
	deviceLevel = msproto.DeviceLevel(deviceLevelUint8)

	if token, err = decoder.String(); err != nil {
		return
	}

	var createdAtUnixNano uint64
	if createdAtUnixNano, err = decoder.Uint64(); err != nil {
		return
	}
	if createdAtUnixNano > 0 {
		ct := time.Unix(int64(createdAtUnixNano/1e9), int64(createdAtUnixNano%1e9))
		createdAt = &ct
	}

	var updatedAtUnixNano uint64
	if updatedAtUnixNano, err = decoder.Uint64(); err != nil {
		return
	}
	if updatedAtUnixNano > 0 {
		ct := time.Unix(int64(updatedAtUnixNano/1e9), int64(updatedAtUnixNano%1e9))
		updatedAt = &ct
	}

	return
}

// EncodeCMDUpdateMessageOfUserCursorIfNeed EncodeCMDUpdateMessageOfUserCursorIfNeed
func EncodeCMDUpdateMessageOfUserCursorIfNeed(uid string, messageSeq uint64) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(uid)
	encoder.WriteUint64(messageSeq)
	return encoder.Bytes()
}

// DecodeCMDUpdateMessageOfUserCursorIfNeed DecodeCMDUpdateMessageOfUserCursorIfNeed
func (c *CMD) DecodeCMDUpdateMessageOfUserCursorIfNeed() (uid string, messageSeq uint64, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if uid, err = decoder.String(); err != nil {
		return
	}
	if messageSeq, err = decoder.Uint64(); err != nil {
		return
	}
	return
}

// EncodeChannelInfo EncodeChannelInfo
func EncodeChannelInfo(c msdb.ChannelInfo, version CmdVersion) ([]byte, error) {

	enc := msproto.NewEncoder()
	defer enc.End()
	enc.WriteString(c.ChannelId)
	enc.WriteUint8(c.ChannelType)
	enc.WriteUint8(msutil.BoolToUint8(c.Ban))
	enc.WriteUint8(msutil.BoolToUint8(c.Large))
	enc.WriteUint8(msutil.BoolToUint8(c.Disband))
	if c.CreatedAt != nil {
		enc.WriteUint64(uint64(c.CreatedAt.UnixNano()))
	} else {
		enc.WriteUint64(0)
	}
	if c.UpdatedAt != nil {
		enc.WriteUint64(uint64(c.UpdatedAt.UnixNano()))
	} else {
		enc.WriteUint64(0)
	}
	if version > 0 {
		enc.WriteString(c.Webhook)
	}
	return enc.Bytes(), nil
}

// DecodeChannelInfo DecodeChannelInfo
func (c *CMD) DecodeChannelInfo() (msdb.ChannelInfo, error) {
	channelInfo := msdb.ChannelInfo{}
	dec := msproto.NewDecoder(c.Data)

	var err error
	if channelInfo.ChannelId, err = dec.String(); err != nil {
		return channelInfo, err
	}
	if channelInfo.ChannelType, err = dec.Uint8(); err != nil {
		return channelInfo, err
	}
	var ban uint8
	if ban, err = dec.Uint8(); err != nil {
		return channelInfo, err
	}
	var large uint8
	if large, err = dec.Uint8(); err != nil {
		return channelInfo, err
	}
	var disband uint8
	if disband, err = dec.Uint8(); err != nil {
		return channelInfo, err
	}

	channelInfo.Ban = msutil.Uint8ToBool(ban)
	channelInfo.Large = msutil.Uint8ToBool(large)
	channelInfo.Disband = msutil.Uint8ToBool(disband)

	var createdAt uint64
	if createdAt, err = dec.Uint64(); err != nil {
		return channelInfo, err
	}
	if createdAt > 0 {
		ct := time.Unix(int64(createdAt/1e9), int64(createdAt%1e9))
		channelInfo.CreatedAt = &ct
	}
	var updatedAt uint64
	if updatedAt, err = dec.Uint64(); err != nil {
		return channelInfo, err
	}
	if updatedAt > 0 {
		ct := time.Unix(int64(updatedAt/1e9), int64(updatedAt%1e9))
		channelInfo.UpdatedAt = &ct
	}

	if c.version > 0 {
		if channelInfo.Webhook, err = dec.String(); err != nil {
			return channelInfo, err
		}
	}

	return channelInfo, err
}

// EncodeCMDAddOrUpdateUserConversations EncodeCMDAddOrUpdateConversations
func EncodeCMDAddOrUpdateUserConversations(uid string, conversations []msdb.Conversation) ([]byte, error) {

	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(uid)
	encoder.WriteUint32(uint32(len(conversations)))
	for _, conversation := range conversations {
		data, err := conversation.Marshal()
		if err != nil {
			return nil, err
		}
		encoder.WriteBinary(data)
	}
	return encoder.Bytes(), nil
}

// DecodeCMDAddOrUpdateUserConversations
func (c *CMD) DecodeCMDAddOrUpdateUserConversations() (uid string, conversations []msdb.Conversation, err error) {
	if len(c.Data) == 0 {
		return
	}
	decoder := msproto.NewDecoder(c.Data)
	if uid, err = decoder.String(); err != nil {
		return
	}

	var count uint32
	if count, err = decoder.Uint32(); err != nil {
		return
	}
	for i := uint32(0); i < count; i++ {
		var conversationBytes []byte
		if conversationBytes, err = decoder.Binary(); err != nil {
			return
		}
		var conversation = &msdb.Conversation{}
		err = conversation.Unmarshal(conversationBytes)
		if err != nil {
			return
		}
		conversations = append(conversations, *conversation)

	}

	return
}

func EncodeCMDDeleteConversation(uid string, channelId string, channelType uint8) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(uid)
	encoder.WriteString(channelId)
	encoder.WriteUint8(channelType)
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDDeleteConversation() (uid string, channelId string, channelType uint8, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if uid, err = decoder.String(); err != nil {
		return
	}
	if channelId, err = decoder.String(); err != nil {
		return
	}
	if channelType, err = decoder.Uint8(); err != nil {
		return
	}
	return
}

func EncodeCMDDeleteConversations(uid string, channels []msdb.Channel) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(uid)
	encoder.WriteInt32(int32(len(channels)))
	for _, channel := range channels {
		encoder.WriteString(channel.ChannelId)
		encoder.WriteUint8(channel.ChannelType)
	}
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDDeleteConversations() (uid string, channels []msdb.Channel, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if uid, err = decoder.String(); err != nil {
		return
	}
	var count int32
	count, err = decoder.Int32()
	if err != nil {
		return
	}

	var channelId string
	var channelType uint8
	for i := 0; i < int(count); i++ {
		channelId, err = decoder.String()
		if err != nil {
			return
		}
		channelType, err = decoder.Uint8()
		if err != nil {
			return
		}
		channels = append(channels, msdb.Channel{
			ChannelId:   channelId,
			ChannelType: channelType,
		})

	}
	return

}

func EncodeCMDStreamEnd(channelID string, channelType uint8, streamNo string) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(channelID)
	encoder.WriteUint8(channelType)
	encoder.WriteString(streamNo)
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDStreamEnd() (channelID string, channelType uint8, streamNo string, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if channelID, err = decoder.String(); err != nil {
		return
	}
	if channelType, err = decoder.Uint8(); err != nil {
		return
	}
	if streamNo, err = decoder.String(); err != nil {
		return
	}
	return
}

// func EncodeCMDAppendStreamItem(channelID string, channelType uint8, streamNo string, item *msstore.StreamItem) []byte {
// 	encoder := msproto.NewEncoder()
// 	defer encoder.End()

// 	encoder.WriteString(channelID)
// 	encoder.WriteUint8(channelType)
// 	encoder.WriteString(streamNo)
// 	encoder.WriteBinary(msstore.EncodeStreamItem(item))

// 	return encoder.Bytes()
// }
// func (c *CMD) DecodeCMDAppendStreamItem() (channelID string, channelType uint8, streamNo string, item *msstore.StreamItem, err error) {
// 	decoder := msproto.NewDecoder(c.Data)
// 	if channelID, err = decoder.String(); err != nil {
// 		return
// 	}
// 	if channelType, err = decoder.Uint8(); err != nil {
// 		return
// 	}
// 	if streamNo, err = decoder.String(); err != nil {
// 		return
// 	}
// 	var itemBytes []byte
// 	itemBytes, err = decoder.Binary()
// 	if err != nil {
// 		return
// 	}
// 	item, err = msstore.DecodeStreamItem(itemBytes)
// 	return
// }

func EncodeCMDChannelClusterConfigSave(channelID string, channelType uint8, data []byte) ([]byte, error) {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(channelID)
	encoder.WriteUint8(channelType)
	encoder.WriteBytes(data)
	return encoder.Bytes(), nil
}

func (c *CMD) DecodeCMDChannelClusterConfigSave() (channelID string, channelType uint8, data []byte, err error) {
	decoder := msproto.NewDecoder(c.Data)

	if channelID, err = decoder.String(); err != nil {
		return
	}
	if channelType, err = decoder.Uint8(); err != nil {
		return
	}
	data, err = decoder.BinaryAll()
	return
}

func EncodeCMDBatchUpdateConversation(models []*msdb.BatchUpdateConversationModel) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()

	encoder.WriteUint32(uint32(len(models)))
	for _, model := range models {
		encoder.WriteUint16(uint16(len(model.Uids)))
		for uid, seq := range model.Uids {
			encoder.WriteString(uid)
			encoder.WriteUint64(seq)
		}
		encoder.WriteString(model.ChannelId)
		encoder.WriteUint8(model.ChannelType)

	}

	return encoder.Bytes()
}

func (c *CMD) DecodeCMDBatchUpdateConversation() (models []*msdb.BatchUpdateConversationModel, err error) {
	decoder := msproto.NewDecoder(c.Data)

	var count uint32
	if count, err = decoder.Uint32(); err != nil {
		return
	}

	for i := uint32(0); i < count; i++ {
		var model = &msdb.BatchUpdateConversationModel{
			Uids: map[string]uint64{},
		}
		var uidCount uint16
		if uidCount, err = decoder.Uint16(); err != nil {
			return
		}
		for j := uint16(0); j < uidCount; j++ {
			var uid string
			if uid, err = decoder.String(); err != nil {
				return
			}

			var seq uint64
			if seq, err = decoder.Uint64(); err != nil {
				return
			}
			model.Uids[uid] = seq
		}
		if model.ChannelId, err = decoder.String(); err != nil {
			return
		}
		if model.ChannelType, err = decoder.Uint8(); err != nil {
			return
		}
		models = append(models, model)
	}

	return
}

func EncodeCMDSystemUIDs(uids []string) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteUint32(uint32(len(uids)))
	for _, uid := range uids {
		encoder.WriteString(uid)
	}
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDSystemUIDs() (uids []string, err error) {
	decoder := msproto.NewDecoder(c.Data)
	var count uint32
	if count, err = decoder.Uint32(); err != nil {
		return
	}
	for i := uint32(0); i < count; i++ {
		var uid string
		if uid, err = decoder.String(); err != nil {
			return
		}
		uids = append(uids, uid)
	}
	return
}

func EncodeCMDAddStreamMeta(streamMeta *msdb.StreamMeta) []byte {
	return streamMeta.Encode()
}

func (c *CMD) DecodeCMDAddStreamMeta() (streamMeta *msdb.StreamMeta, err error) {
	streamMeta = &msdb.StreamMeta{}
	err = streamMeta.Decode(c.Data)
	return
}

func EncodeCMDAddStreams(streams []*msdb.Stream) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteUint32(uint32(len(streams)))
	for _, stream := range streams {
		data := stream.Encode()
		encoder.WriteUint32(uint32(len(data)))
		encoder.WriteBytes(data)
	}
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDAddStreams() ([]*msdb.Stream, error) {
	decoder := msproto.NewDecoder(c.Data)
	var count uint32
	var err error
	if count, err = decoder.Uint32(); err != nil {
		return nil, err
	}
	streams := make([]*msdb.Stream, 0, count)
	for i := uint32(0); i < count; i++ {
		dataLen, err := decoder.Uint32()
		if err != nil {
			return nil, err
		}

		data, err := decoder.Bytes(int(dataLen))
		if err != nil {
			return nil, err
		}

		stream := &msdb.Stream{}
		if err = stream.Decode(data); err != nil {
			return nil, err
		}
		streams = append(streams, stream)
	}
	return streams, nil
}

func EncodeCMDAddOrUpdateConversationsWithChannel(channelId string, channelType uint8, subscribers []string, readToMsgSeq uint64, conversationType msdb.ConversationType, unreadCount int, createdAt, updatedAt int64) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(channelId)
	encoder.WriteUint8(channelType)
	encoder.WriteUint32(uint32(len(subscribers)))
	for _, subscriber := range subscribers {
		encoder.WriteString(subscriber)
	}
	encoder.WriteUint64(readToMsgSeq)
	encoder.WriteUint8(uint8(conversationType))
	encoder.WriteInt32(int32(unreadCount))
	encoder.WriteInt64(createdAt)
	encoder.WriteInt64(updatedAt)
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDAddOrUpdateConversationsWithChannel() (channelId string, channelType uint8, subscribers []string, readToMsgSeq uint64, conversationType msdb.ConversationType, unreadCount int, createdAt, updatedAt int64, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if channelId, err = decoder.String(); err != nil {
		return
	}
	if channelType, err = decoder.Uint8(); err != nil {
		return
	}
	var count uint32
	if count, err = decoder.Uint32(); err != nil {
		return
	}
	for i := uint32(0); i < count; i++ {
		var subscriber string
		if subscriber, err = decoder.String(); err != nil {
			return
		}
		subscribers = append(subscribers, subscriber)
	}
	if readToMsgSeq, err = decoder.Uint64(); err != nil {
		return
	}
	var conversationTypeUint8 uint8
	if conversationTypeUint8, err = decoder.Uint8(); err != nil {
		return
	}
	conversationType = msdb.ConversationType(conversationTypeUint8)
	var unreadCountI32 int32
	if unreadCountI32, err = decoder.Int32(); err != nil {
		return
	}
	unreadCount = int(unreadCountI32)
	if createdAt, err = decoder.Int64(); err != nil {
		return
	}
	if updatedAt, err = decoder.Int64(); err != nil {
		return
	}
	return
}

func EncodeCMDAddOrUpdateConversations(conversations msdb.ConversationSet) ([]byte, error) {

	return conversations.Marshal()
}

func (c *CMD) DecodeCMDAddOrUpdateConversations() (msdb.ConversationSet, error) {
	conversations := msdb.ConversationSet{}
	err := conversations.Unmarshal(c.Data)
	return conversations, err
}

func EncodeCMDAddOrUpdateTester(tester msdb.Tester) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(tester.No)
	encoder.WriteString(tester.Addr)
	encoder.WriteUint64(uint64(tester.CreatedAt.UnixNano()))
	encoder.WriteUint64(uint64(tester.UpdatedAt.UnixNano()))
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDAddOrUpdateTester() (tester msdb.Tester, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if tester.No, err = decoder.String(); err != nil {
		return
	}
	if tester.Addr, err = decoder.String(); err != nil {
		return
	}
	var createdAtUnixNano uint64
	if createdAtUnixNano, err = decoder.Uint64(); err != nil {
		return
	}
	ct := time.Unix(int64(createdAtUnixNano/1e9), int64(createdAtUnixNano%1e9))
	tester.CreatedAt = &ct
	var updatedAtUnixNano uint64
	if updatedAtUnixNano, err = decoder.Uint64(); err != nil {
		return
	}
	ut := time.Unix(int64(updatedAtUnixNano/1e9), int64(updatedAtUnixNano%1e9))
	tester.UpdatedAt = &ut
	return
}

func EncodeCMDRemoveTester(no string) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(no)
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDRemoveTester() (no string, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if no, err = decoder.String(); err != nil {
		return
	}
	return
}

func EncodeCMDUserPluginNo(p msdb.PluginUser) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(p.Uid)
	encoder.WriteString(p.PluginNo)

	var createdAt uint64 = 0
	var updatedAt uint64 = 0
	if p.CreatedAt != nil {
		createdAt = uint64(p.CreatedAt.UnixNano())
	}
	if p.UpdatedAt != nil {
		updatedAt = uint64(p.UpdatedAt.UnixNano())
	}
	encoder.WriteUint64(createdAt)
	encoder.WriteUint64(updatedAt)

	return encoder.Bytes()
}

func (c *CMD) DecodeCMDUserPluginNo() (msdb.PluginUser, error) {
	decoder := msproto.NewDecoder(c.Data)
	var pluginUser = msdb.PluginUser{}
	var err error
	if pluginUser.Uid, err = decoder.String(); err != nil {
		return pluginUser, err
	}
	if pluginUser.PluginNo, err = decoder.String(); err != nil {
		return pluginUser, err
	}

	var createdAt uint64
	if createdAt, err = decoder.Uint64(); err != nil {
		return pluginUser, err
	}
	if createdAt > 0 {
		ct := time.Unix(int64(createdAt/1e9), int64(createdAt%1e9))
		pluginUser.CreatedAt = &ct
	}
	var updatedAt uint64
	if updatedAt, err = decoder.Uint64(); err != nil {
		return pluginUser, err
	}
	if updatedAt > 0 {
		ct := time.Unix(int64(updatedAt/1e9), int64(updatedAt%1e9))
		pluginUser.UpdatedAt = &ct
	}
	return pluginUser, nil
}

func EncodeCMDPluginUser(pluginNo string, uid string) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(pluginNo)
	encoder.WriteString(uid)
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDPluginUser() (pluginNo string, uid string, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if pluginNo, err = decoder.String(); err != nil {
		return
	}
	if uid, err = decoder.String(); err != nil {
		return
	}
	return
}

func EncodeCMDPlugin(p msdb.Plugin) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(p.No)
	encoder.WriteString(p.Name)
	encoder.WriteUint32(uint32(len(p.ConfigTemplate)))
	encoder.WriteBytes(p.ConfigTemplate)

	var createdAt uint64 = 0
	var updatedAt uint64 = 0
	if p.CreatedAt != nil {
		createdAt = uint64(p.CreatedAt.UnixNano())
	}
	if p.UpdatedAt != nil {
		updatedAt = uint64(p.UpdatedAt.UnixNano())
	}
	encoder.WriteUint64(createdAt)
	encoder.WriteUint64(updatedAt)
	encoder.WriteUint32(uint32(p.Status))
	encoder.WriteString(p.Version)
	// methods
	encoder.WriteUint16(uint16(len(p.Methods)))
	for _, method := range p.Methods {
		encoder.WriteString(method)
	}
	// priority
	encoder.WriteUint32(p.Priority)

	return encoder.Bytes()
}

func (c *CMD) DecodeCMDPlugin() (p msdb.Plugin, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if p.No, err = decoder.String(); err != nil {
		return
	}
	if p.Name, err = decoder.String(); err != nil {
		return
	}
	var configLen uint32
	if configLen, err = decoder.Uint32(); err != nil {
		return
	}
	if p.ConfigTemplate, err = decoder.Bytes(int(configLen)); err != nil {
		return
	}

	var createdAt uint64
	if createdAt, err = decoder.Uint64(); err != nil {
		return
	}
	if createdAt > 0 {
		ct := time.Unix(int64(createdAt/1e9), int64(createdAt%1e9))
		p.CreatedAt = &ct
	}
	var updatedAt uint64
	if updatedAt, err = decoder.Uint64(); err != nil {
		return
	}
	if updatedAt > 0 {
		ct := time.Unix(int64(updatedAt/1e9), int64(updatedAt%1e9))
		p.UpdatedAt = &ct
	}
	var status uint32
	if status, err = decoder.Uint32(); err != nil {
		return
	}
	p.Status = msdb.PluginStatus(status)
	if p.Version, err = decoder.String(); err != nil {
		return
	}
	var methodLen uint16
	if methodLen, err = decoder.Uint16(); err != nil {
		return
	}
	p.Methods = make([]string, methodLen)
	for i := uint16(0); i < methodLen; i++ {
		if p.Methods[i], err = decoder.String(); err != nil {
			return
		}
	}
	if p.Priority, err = decoder.Uint32(); err != nil {
		return
	}
	return
}

func EncodeCMDPluginConfig(pluginNo string, config map[string]interface{}) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(pluginNo)

	cfgData, _ := json.Marshal(config)
	encoder.WriteUint32(uint32(len(cfgData)))
	encoder.WriteBytes(cfgData)

	return encoder.Bytes()
}

func (c *CMD) DecodeCMDPluginConfig() (pluginNo string, config map[string]interface{}, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if pluginNo, err = decoder.String(); err != nil {
		return
	}
	var cfgLen uint32
	if cfgLen, err = decoder.Uint32(); err != nil {
		return
	}
	cfgData, err := decoder.Bytes(int(cfgLen))
	if err != nil {
		return
	}
	err = json.Unmarshal(cfgData, &config)
	return
}

func EncodeCMDUpdateConversationDeletedAtMsgSeq(uid string, channelId string, channelType uint8, deletedAtMsgSeq uint64) []byte {
	encoder := msproto.NewEncoder()
	defer encoder.End()
	encoder.WriteString(uid)
	encoder.WriteString(channelId)
	encoder.WriteUint8(channelType)
	encoder.WriteUint64(deletedAtMsgSeq)
	return encoder.Bytes()
}

func (c *CMD) DecodeCMDUpdateConversationDeletedAtMsgSeq() (uid string, channelId string, channelType uint8, deletedAtMsgSeq uint64, err error) {
	decoder := msproto.NewDecoder(c.Data)
	if uid, err = decoder.String(); err != nil {
		return
	}
	if channelId, err = decoder.String(); err != nil {
		return
	}
	if channelType, err = decoder.Uint8(); err != nil {
		return
	}
	if deletedAtMsgSeq, err = decoder.Uint64(); err != nil {
		return
	}
	return
}
