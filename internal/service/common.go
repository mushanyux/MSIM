package service

import (
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/pkg/mslog"
	msproto "github.com/mushanyux/MSIMGoProto"
	"go.uber.org/zap"
)

var CommonService ICommonService

type ICommonService interface {
	Schedule(interval time.Duration, f func()) *timingwheel.Timer
	AfterFunc(d time.Duration, f func())
}

// 判断单聊是否允许发送消息
func AllowSendForPerson(from, to string) (msproto.ReasonCode, error) {
	// 判断是否是黑名单内
	isDenylist, err := Store.ExistDenylist(to, msproto.ChannelTypePerson, from)
	if err != nil {
		mslog.Error("ExistDenylist error", zap.String("from", from), zap.String("to", to), zap.Error(err))
		return msproto.ReasonSystemError, err
	}
	if isDenylist {
		return msproto.ReasonInBlacklist, nil
	}

	if !options.G.WhitelistOffOfPerson {
		// 判断是否在白名单内
		isAllowlist, err := Store.ExistAllowlist(to, msproto.ChannelTypePerson, from)
		if err != nil {
			mslog.Error("ExistAllowlist error", zap.Error(err))
			return msproto.ReasonSystemError, err
		}
		if !isAllowlist {
			return msproto.ReasonNotInWhitelist, nil
		}
	}

	return msproto.ReasonSuccess, nil
}
