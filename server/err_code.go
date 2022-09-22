package server

import "fmt"

//nolint: golint
var (
	OK                  = &Errno{Code: 0, Message: "OK"}
	InternalServerError = &Errno{Code: 1, Message: "Internal server error"}
	ErrToken            = &Errno{Code: 10001, Message: "token错误"}
	ErrParam            = &Errno{Code: 10002, Message: "参数有误"}
	ErrNotData          = &Errno{Code: 10003, Message: "没有数据"}
	ErrNotChangeData    = &Errno{Code: 10004, Message: "数据没有更改"}
	ErrNotRepeatData    = &Errno{Code: 10005, Message: "数据已存在"}
	ErrEngine           = &Errno{Code: 10006, Message: "Engine Not"}
	ErrCreateWallet     = &Errno{Code: 10007, Message: "创建钱包失败"}
	ErrSign             = &Errno{Code: 10008, Message: "签名错误"}
	ErrLoginNot         = &Errno{Code: 10009, Message: "用户名/密码/谷歌验证码错误"}
	ErrOldGoogleAuth    = &Errno{Code: 10010, Message: "旧谷歌验证码错误"}
	ErrGoogleAuth       = &Errno{Code: 10011, Message: "谷歌验证码错误"}
)

// Errno ...
type Errno struct {
	Code    int
	Message string
}

func (err Errno) Error() string {
	return err.Message
}

// Err represents an error
type Err struct {
	Code    int
	Message string
	Err     error
}

func (err *Err) Error() string {
	return fmt.Sprintf("Err - code: %d, message: %s, error: %s", err.Code, err.Message, err.Err)
}

// DecodeErr ...
func DecodeErr(err error) (int, string) {
	if err == nil {
		return OK.Code, OK.Message
	}

	switch typed := err.(type) {
	case *Err:
		return typed.Code, typed.Message
	case *Errno:
		return typed.Code, typed.Message
	default:
	}

	return InternalServerError.Code, err.Error()
}
