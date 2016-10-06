package memcached

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"time"

	"github.com/facebookgo/stackerr"
	"github.com/pkg/errors"
	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/recycle"
)

const (
	MaxKeySize         = 250
	MaxItemSize        = 128 * (1 << 20) // 128 MB.
	DefaultMaxItemSize = 1 << 20
	MaxCommandSize     = 1 << 12

	MaxRelativeExptime = 60 * 60 * 24 * 30 // 30 days.

	Separator = "\r\n"

	SetCommand    = "set"
	GetCommand    = "get"
	GetsCommand   = "gets"
	DeleteCommand = "delete"

	NoReplyOption = "noreply"

	StoredResponse      = "STORED"
	ValueResponse       = "VALUE"
	EndResponse         = "END"
	DeletedResponse     = "DELETED"
	NotFoundResponse    = "NOT_FOUND"
	ErrorResponse       = "ERROR"
	ClientErrorResponse = "CLIENT_ERROR"
	ServerErrorResponse = "SERVER_ERROR"

	// Implementation specific consts.
	InBufferSize  = 16 * (1 << 10)
	OutBufferSize = 16 * (1 << 10)
)

var _ = func() (_ struct{}) {
	if MaxCommandSize < InBufferSize {
		panic("max command should fit in input buffer")
	}
	return
}

var (
	ErrTooLargeKey          = errors.New("too large key")
	ErrTooLargeItem         = errors.New("too large item")
	ErrInvalidOption        = errors.New("invalid option")
	ErrTooManyFields        = errors.New("too many fields")
	ErrMoreFieldsRequired   = errors.New("more fields required")
	ErrTooLargeCommand      = errors.New("command length is too big")
	ErrEmptyCommand         = errors.New("empty command")
	ErrFieldsParseError     = errors.New("fields parse error ")
	ErrInvalidLineSeparator = errors.New("invalid line separator")
	ErrInvalidCharInKey     = errors.New("key contains invalid characters")

	separatorBytes = []byte(Separator)
)

func isInvalidFieldChar(b byte) bool {
	return b <= ' ' || b == 127
}

func checkKey(p []byte) error {
	if len(p) > MaxKeySize {
		return stackerr.Wrap(ErrTooLargeKey)
	}
	for _, b := range p {
		if isInvalidFieldChar(b) {
			return stackerr.Wrap(ErrInvalidCharInKey)
		}
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

func parseSetFields(fields [][]byte) (m cache.ItemMeta, noreply bool, err error) {
	const extraRequired = 3
	var key []byte
	var extra [][]byte
	key, extra, noreply, err = parseKeyFields(fields, extraRequired)
	if err != nil {
		return
	}
	m.Key, err = parseKey(key)
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
	m.Flags = uint32(parsed[0])
	m.Exptime = int64(parsed[1])
	if m.Exptime > MaxRelativeExptime {
		m.Exptime += time.Now().Unix()
	}
	m.Bytes = int(parsed[2])
	if m.Bytes < 0 || m.Bytes > MaxItemSize {
		err = ErrTooLargeItem
	}
	return
}

func parseKeyFields(fields [][]byte, extraRequired int) (key []byte, extra [][]byte, noreply bool, err error) {
	if len(fields) < 1+extraRequired {
		err = stackerr.Wrap(ErrMoreFieldsRequired)
		return
	}
	key = fields[0]
	extra = fields[1:][:extraRequired]
	options := fields[1:][extraRequired:]
	const maxOptions = 1
	if len(options) > maxOptions {
		err = stackerr.Wrap(ErrTooManyFields)
		return
	}
	if len(options) != 0 {
		if string(options[0]) != NoReplyOption {
			err = stackerr.Wrap(ErrInvalidOption)
			return
		}
		noreply = true
	}
	return
}

type reader struct {
	*bufio.Reader
	pool *recycle.Pool
}

func newReader(r io.Reader, p *recycle.Pool) reader {
	return reader{
		Reader: bufio.NewReaderSize(r, InBufferSize),
		pool:   p,
	}
}

// WARN: retuned byte slices points into read buffed and invalidated after next read.
func (r reader) readCommand() (command []byte, fields [][]byte, clientErr, err error) {
	var lineWithSeparator []byte
	// We accept only "\r\n" separator, so can't use ReadLine here.
	lineWithSeparator, err = r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// Too big command.
		clientErr = stackerr.Wrap(ErrTooLargeCommand)
		err = r.discardCommand()
		return
	}
	if err == io.EOF {
		if len(lineWithSeparator) != 0 {
			err = stackerr.Wrap(io.ErrUnexpectedEOF)
		}
		return
	}
	if err != nil {
		err = stackerr.Wrap(err)
		return
	}
	if !bytes.HasSuffix(lineWithSeparator, separatorBytes) {
		clientErr = stackerr.Wrap(ErrInvalidLineSeparator)
		return
	}
	line := bytes.TrimSuffix(lineWithSeparator, separatorBytes)
	split := bytes.Fields(line)
	if len(split) == 0 {
		clientErr = stackerr.Wrap(ErrEmptyCommand)
		return
	}
	command = split[0]
	fields = split[1:]
	return
}

func (r reader) readDataBlock(size int) (data *recycle.Data, clientErr, err error) {
	data, err = r.pool.ReadData(r, size)
	if err != nil {
		err = stackerr.Wrap(err)
		return
	}
	defer func() {
		if clientErr != nil || err != nil {
			data.Recycle()
			data = nil
		}
	}()
	var sep []byte
	sep, err = r.ReadSlice('\n')
	err = stackerr.Wrap(err)
	if err == nil && !bytes.Equal(sep, separatorBytes) {
		clientErr = stackerr.Wrap(ErrInvalidLineSeparator)
	}
	return
}

// discardCommand discard all input untill next separator.
func (r reader) discardCommand() error {
	for {
		lineWithSeparator, err := r.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			return err
		}
		if !bytes.HasSuffix(lineWithSeparator, separatorBytes) {
			continue
		}
		return nil
	}
}
