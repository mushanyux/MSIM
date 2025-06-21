package msdb

import "errors"

var (
	ErrNotFound        = errors.New("not found")
	ErrInvalidUserId   = errors.New("invalid user id")
	ErrInvalidDeviceId = errors.New("invalid device id")
	ErrAlreadyExist    = errors.New("already exist")
)
