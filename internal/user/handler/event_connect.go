package handler

import (
	"encoding/base64"
	"errors"
	"time"

	"github.com/mushanyux/MSIM/internal/eventbus"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/internal/service"
	"github.com/mushanyux/MSIM/pkg/fasttime"
	"github.com/mushanyux/MSIM/pkg/msdb"
	"github.com/mushanyux/MSIM/pkg/msnet"
	"github.com/mushanyux/MSIM/pkg/msutil"
	msproto "github.com/mushanyux/MSIMGoProto"
	"go.uber.org/zap"
)

func (h *Handler) connect(ctx *eventbus.UserContext) {
	for _, event := range ctx.Events {
		conn := event.Conn
		uid := event.Conn.Uid
		reasonCode, packet, err := h.handleConnect(event)
		if err != nil {
			h.Error("handle connect err", zap.Error(err))
			return
		}
		if reasonCode == msproto.ReasonSuccess {
			if conn.LastActive <= 0 {
				conn.LastActive = fasttime.UnixTimestamp()
			}
			ctx.AddConn(conn)

			// -------------------- user online --------------------
			// 在线webhook
			deviceOnlineCount := eventbus.User.ConnCountByDeviceFlag(uid, conn.DeviceFlag)
			totalOnlineCount := eventbus.User.ConnCountByUid(uid)
			service.Webhook.Online(uid, conn.DeviceFlag, conn.ConnId, deviceOnlineCount, totalOnlineCount)
		}
		connackEvent := &eventbus.Event{
			Type:         eventbus.EventConnack,
			Conn:         conn,
			Frame:        packet,
			SourceNodeId: options.G.Cluster.NodeId,
			ReqId:        event.ReqId,
		}
		if options.G.IsLocalNode(conn.NodeId) {
			eventbus.User.AddEvent(uid, connackEvent)
			eventbus.User.Advance(uid)
		} else {
			h.forwardToNode(conn.NodeId, uid, &eventbus.Event{
				Type:         eventbus.EventConnack,
				Conn:         conn,
				Frame:        packet,
				SourceNodeId: options.G.Cluster.NodeId,
				ReqId:        event.ReqId,
			})
		}

	}
}

func (h *Handler) handleConnect(event *eventbus.Event) (msproto.ReasonCode, *msproto.ConnackPacket, error) {
	var (
		conn          = event.Conn
		connectPacket = event.Frame.(*msproto.ConnectPacket)
		devceLevel    msproto.DeviceLevel
		uid           = connectPacket.UID
	)
	// -------------------- token verify --------------------
	if connectPacket.UID == options.G.ManagerUID {
		if options.G.ManagerTokenOn && connectPacket.Token != options.G.ManagerToken {
			h.Error("manager token verify fail", zap.String("uid", uid), zap.String("token", connectPacket.Token))
			return msproto.ReasonAuthFail, nil, nil
		}
		devceLevel = msproto.DeviceLevelSlave // 默认都是slave设备
	} else if options.G.TokenAuthOn {
		if connectPacket.Token == "" {
			h.Error("token is empty")
			return msproto.ReasonAuthFail, nil, errors.New("token is empty")
		}
		device, err := service.Store.GetDevice(uid, connectPacket.DeviceFlag)
		if err != nil {
			h.Error("get device token err", zap.Error(err))
			return msproto.ReasonAuthFail, nil, err
		}
		if device.Token != connectPacket.Token {
			h.Error("token verify fail", zap.String("uid", uid), zap.Uint64("sourceNodeId", event.SourceNodeId), zap.String("expectToken", device.Token), zap.String("actToken", connectPacket.Token))
			return msproto.ReasonAuthFail, nil, errors.New("token verify fail")
		}
		devceLevel = msproto.DeviceLevel(device.DeviceLevel)
	} else {
		devceLevel = msproto.DeviceLevelSlave // 默认都是slave设备
	}

	// -------------------- ban  --------------------
	userChannelInfo, err := service.Store.GetChannel(uid, msproto.ChannelTypePerson)
	if err != nil {
		h.Error("get device channel info err", zap.Error(err))
		return msproto.ReasonAuthFail, nil, err
	}
	ban := false
	if !msdb.IsEmptyChannelInfo(userChannelInfo) {
		ban = userChannelInfo.Ban
	}
	if ban {
		h.Error("device is ban", zap.String("uid", uid))
		return msproto.ReasonBan, nil, errors.New("device is ban")
	}

	var aesKey, aesIV []byte
	var dhServerPublicKeyEnc string

	// -------------------- get message encrypt key (if enabled) --------------------
	if !options.G.DisableEncryption && !conn.IsJsonRpc { // 如果连接是jsonrpc连接，则不进行加密
		dhServerPrivKey, dhServerPublicKey := msutil.GetCurve25519KeypPair() // 生成服务器的DH密钥对
		var err error
		aesKey, aesIV, err = h.getClientAesKeyAndIV(connectPacket.ClientKey, dhServerPrivKey)
		if err != nil {
			h.Error("get client aes key and iv err", zap.Error(err))
			return msproto.ReasonAuthFail, nil, err
		}
		dhServerPublicKeyEnc = base64.StdEncoding.EncodeToString(dhServerPublicKey[:])
	}

	// -------------------- same master kicks each other --------------------
	oldConns := eventbus.User.ConnsByDeviceFlag(uid, connectPacket.DeviceFlag)
	if len(oldConns) > 0 {
		if devceLevel == msproto.DeviceLevelMaster { // 如果设备是master级别，则把旧连接都踢掉
			for _, oldConn := range oldConns {
				if oldConn.Equal(conn) { // 不能把自己踢了
					continue
				}
				// 在master级别下，同一个用户，不同设备Id，踢掉
				if oldConn.DeviceId != connectPacket.DeviceID {
					h.Info("auth: same master kicks each other",
						zap.String("devceLevel", devceLevel.String()),
						zap.String("uid", uid),
						zap.String("deviceID", connectPacket.DeviceID),
						zap.String("oldDeviceId", oldConn.DeviceId),
					)
					eventbus.User.ConnWrite(event.ReqId, oldConn, &msproto.DisconnectPacket{
						ReasonCode: msproto.ReasonConnectKick,
						Reason:     "login in other device",
					})
					service.CommonService.AfterFunc(time.Second*2, func(od *eventbus.Conn) func() {
						return func() {
							eventbus.User.CloseConn(od)
						}
					}(oldConn))
				} else {
					// 相同设备Id，只关闭连接，不进行踢操作
					service.CommonService.AfterFunc(time.Second*2, func(od *eventbus.Conn) func() {
						return func() {
							eventbus.User.CloseConn(od)
						}

					}(oldConn))
				}
				h.Info("auth: close old conn for master", zap.Any("oldConn", oldConn))
			}
		} else if devceLevel == msproto.DeviceLevelSlave { // 如果设备是slave级别，则把相同的deviceId关闭
			for _, oldConn := range oldConns {
				if oldConn.ConnId != conn.ConnId && oldConn.DeviceId == connectPacket.DeviceID {
					service.CommonService.AfterFunc(time.Second*2, func(od *eventbus.Conn) func() {
						return func() {
							eventbus.User.CloseConn(od)
						}
					}(oldConn))

					h.Info("auth: close old conn for slave", zap.Any("oldConn", oldConn), zap.Int64("oldConnId", oldConn.ConnId), zap.Int64("newConnId", conn.ConnId))
				}
			}
		}
	}

	// -------------------- set conn info --------------------
	timeDiff := time.Now().UnixNano()/1000/1000 - connectPacket.ClientTimestamp

	// connCtx := p.connContextPool.Get().(*connContext)

	lastVersion := connectPacket.Version
	hasServerVersion := false
	if connectPacket.Version > msproto.LatestVersion {
		lastVersion = msproto.LatestVersion
	}

	conn.AesIV = aesIV
	conn.AesKey = aesKey
	conn.Auth = true
	conn.ProtoVersion = lastVersion
	conn.DeviceLevel = devceLevel

	// 本地连接
	var realConn msnet.Conn
	if options.G.IsLocalNode(conn.NodeId) {
		realConn = service.ConnManager.GetConn(conn.ConnId)
		if realConn != nil {
			realConn.SetMaxIdle(options.G.ConnIdleTime)
		}
	}

	// -------------------- response connack --------------------

	if connectPacket.Version > 3 {
		hasServerVersion = true
	}

	if realConn != nil {
		h.Debug("auth: auth Success", zap.String("uid", conn.Uid), zap.Int64("connId", conn.ConnId), zap.Int("fd", realConn.Fd().Fd()), zap.Uint8("protoVersion", connectPacket.Version), zap.Bool("hasServerVersion", hasServerVersion))
	} else {
		h.Debug("auth: auth Success", zap.String("uid", conn.Uid), zap.Int64("connId", conn.ConnId), zap.Uint8("protoVersion", connectPacket.Version), zap.Bool("hasServerVersion", hasServerVersion))
	}
	connack := &msproto.ConnackPacket{
		Salt:          string(aesIV),
		ServerKey:     dhServerPublicKeyEnc,
		ReasonCode:    msproto.ReasonSuccess,
		TimeDiff:      timeDiff,
		ServerVersion: lastVersion,
		NodeId:        options.G.Cluster.NodeId,
	}
	connack.HasServerVersion = hasServerVersion

	return msproto.ReasonSuccess, connack, nil
}

// 获取客户端的aesKey和aesIV
// dhServerPrivKey  服务端私钥
func (u *Handler) getClientAesKeyAndIV(clientKey string, dhServerPrivKey [32]byte) ([]byte, []byte, error) {
	clientKeyBytes, err := base64.StdEncoding.DecodeString(clientKey)
	if err != nil {
		return nil, nil, err
	}

	var dhClientPubKeyArray [32]byte
	copy(dhClientPubKeyArray[:], clientKeyBytes[:32])

	// 获得DH的共享key
	shareKey := msutil.GetCurve25519Key(dhServerPrivKey, dhClientPubKeyArray) // 共享key

	aesIV := msutil.GetRandomString(16)
	aesKey := msutil.MD5(base64.StdEncoding.EncodeToString(shareKey[:]))[:16]
	return []byte(aesKey), []byte(aesIV), nil
}
