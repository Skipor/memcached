package memcached

import (
	"strconv"
	"time"

	"github.com/facebookgo/stackerr"
	"github.com/pkg/errors"
)

const (
	MaxKeyLength       = 250
	MaxCommandLength   = 1 << 12
	MaxRelativeExptime = 60 * 60 * 24 * 30 // 30 days.

	Separator = "\r\n"

	SetCommand    = "set"
	GetCommand    = "get"
	GetsCommand   = "gets"
	DeleteCommand = "delete"

	NoReplyOption = "norepry"

	StoredResponse      = "STORED"
	ValueResponse       = "VALUE"
	EndResponse         = "END"
	DeletedResponse     = "DELETED"
	NotFoundResponse    = "NOT_FOUND"
	ErrorResponse       = "ERROR"
	ClientErrorResponse = "CLIENT_ERROR"
	ServerErrorResponse = "SERVER_ERROR"
)

var (
	ErrTooLargeKey          = errors.New("too large key")
	ErrTooLargeItem         = errors.New("too large item")
	ErrInvalidOption        = errors.New("invalid option")
	ErrTooManyFields        = errors.New("too many fields")
	ErrMoreFieldsRequired   = errors.New("more fields required")
	ErrTooBigCommand        = errors.New("command length is too big")
	ErrEmptyCommand         = errors.New("empty command")
	ErrFieldsParseError     = errors.New("fields parse error ")
	ErrInvalidLineSeparator = errors.New("invalid line separator")
)

func checkKey(p []byte) error {
	if len(p) > MaxKeyLength {
		return stackerr.Wrap(ErrTooLargeKey)
	}
	return nil
}

func parseKey(p []byte) (key string, err error) {
	err = checkKey(p)
	if err != nil {
		return
	}
	key = string(p)
	return
}

func parseSetFields(fields [][]byte) (m ItemMeta, noreply bool, err error) {
	const extraRequired = 3
	var key []byte
	var extra [][]byte
	key, extra, noreply, err = parseKeyFields(fields, extraRequired)
	if err != nil {
		return
	}
	m.key, err = parseKey(key)
	if err != nil {
		return
	}
	var parsed [extraRequired]uint64
	for i, f := range extra {
		parsed[i], err = strconv.ParseUint(string(f), 10, 32)
		if err != nil {
			err = stackerr.Newf("%s: %s", ErrFieldsParseError, err)
			return
		}
	}
	m.flags = uint32(parsed[0])
	m.exptime = int64(parsed[1])
	if m.exptime > MaxRelativeExptime {
		m.exptime += time.Now().Unix()
	}
	m.bytes = int(parsed[2])
	if m.bytes < 0 {
		err = ErrTooLargeItem
	}
	return
}

func parseKeyFields(fields [][]byte, extraRequired int) (key []byte, extra [][]byte, noreply bool, err error) {
	if len(fields) < 1 {
		err = stackerr.Wrap(ErrMoreFieldsRequired)
		return
	}
	key = fields[0]
	extra = fields[1:]
	const maxOptional = 1
	switch {
	case len(extra) < extraRequired:
		err = stackerr.Wrap(ErrMoreFieldsRequired)
	case len(extra) > extraRequired+maxOptional:
		err = stackerr.Wrap(ErrTooManyFields)
	case len(extra) > extraRequired:
		if string(extra[extraRequired]) != NoReplyOption {
			err = stackerr.Wrap(ErrInvalidOption)
			return
		}
		noreply = true
	}
	return
}
