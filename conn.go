package memcached

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/facebookgo/stackerr"

	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

type Conn struct {
	*bufio.Reader
	*bufio.Writer
	closer      io.Closer
	log         log.Logger
	handler     Handler
	maxItemSize int
	pool        recycle.Pool
}

func NewConn(conn io.ReadWriteCloser, h Handler, log log.Logger) *Conn {
	return &Conn{
		Reader:  bufio.NewReaderSize(conn, MaxCommandLength),
		Writer:  bufio.NewWriter(conn),
		closer:  conn,
		log:     log,
		handler: h,
	}
}

func (c *Conn) Serve() {
	c.log.Debug("Serve connection.")
	defer func() {
		if r := recover(); r != nil {
			c.serverError(stackerr.Newf("Panic: %s", r))
			panic(c)
		}
		c.Close()
		c.log.Debug("Connection closed.")
	}()

	err := c.loop()
	if err != nil {
		c.serverError(err)
	}
}

func (c *Conn) Close() error {
	c.Flush()
	return c.closer.Close()
}

func (c *Conn) loop() error {
	for {
		command, fields, clientErr, err := c.readCommand()
		if err != nil {
			if err == io.EOF {
				// Just client disconnect. Ok.
				return nil
			}
			return stackerr.Wrap(err)
		}
		if clientErr == nil {
			switch string(command) { // No allocation.
			case GetCommand, GetsCommand:
				err, clientErr = c.get(fields)
			case SetCommand:
				err, clientErr = c.set(fields)
			case DeleteCommand:
				err, clientErr = c.delete(fields)
			default:
				c.log.Error("Unexpected command: %s", command)
				err = c.sendResponse(ErrorResponse)
			}
		}
		if clientErr != nil && err == nil {
			err = c.sendClientError(clientErr)
		}
		if err != nil {
			return err
		}
	}
}

func (c *Conn) get(fields [][]byte) (clientErr, err error) {
	if len(fields) == 0 {
		clientErr = stackerr.Wrap(ErrMoreFieldsRequired)
		return
	}
	for _, key := range fields {
		clientErr = checkKey(key)
		if clientErr != nil {
			return
		}
	}

	views := c.handler.Get(fields...)
	var readerIndex int
	defer func() {
		for ; readerIndex < len(views); readerIndex++ {
			views[readerIndex].Reader.Close()
		}
	}()
	for ; readerIndex < len(views); readerIndex++ {
		view := views[readerIndex]
		c.WriteString(ValueResponse)
		c.WriteByte(' ')
		c.WriteString(view.key)
		fmt.Fprintf(c, " %v %v"+Separator, view.flags, view.bytes)
		view.Reader.WriteTo(c)
		_, err = c.WriteString(Separator)
		if err != nil {
			err = stackerr.Wrap(err)
			return
		}
		view.Reader.Close()
	}
	c.sendResponse(EndResponse)
	return
}

func (c *Conn) set(fields [][]byte) (clientErr, err error) {
	var i Item
	var noreply bool

	i.ItemMeta, noreply, clientErr = parseSetFields(fields)
	if clientErr == nil && i.bytes > c.maxItemSize {
		clientErr = stackerr.Wrap(ErrTooLargeItem)
	}
	if clientErr != nil {
		err = c.discardCommand()
		return
	}

	i.data, clientErr, err = c.readItemData(i.bytes)
	if err != nil || clientErr != nil {
		return
	}
	c.handler.Set(i)
	if noreply {
		err = c.Flush()
		return
	}
	err = c.sendResponse(StoredResponse)
	return
}

func (c *Conn) delete(fields [][]byte) (clientErr, err error) {
	const extraRequired = 0
	var key []byte
	var noreply bool
	key, _, noreply, clientErr = parseKeyFields(fields, extraRequired)
	if clientErr != nil {
		return
	}
	deleted := c.handler.Delete(key)
	if noreply {
		err = c.Flush()
		return
	}

	var response string
	if deleted {
		response = DeletedResponse
	} else {
		response = NotFoundResponse
	}
	err = c.sendResponse(response)
	return
}

var separatorBytes = []byte(Separator)

// WARN: retuned byte slices points into read buffed and invalidated after next read.
func (c *Conn) readCommand() (command []byte, fields [][]byte, clientErr, err error) {
	var lineWithSeparator []byte
	// We accept only "\r\n" separator, so can't use ReadLine here.
	lineWithSeparator, err = c.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// Too big command.
		clientErr = stackerr.Wrap(ErrTooBigCommand)
		err = c.discardCommand()
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

func (c *Conn) readItemData(size int) (data *recycle.Data, clientErr, err error) {
	data, err = c.pool.ReadData(c, size)
	if err != nil {
		err = stackerr.Wrap(err)
		return
	}
	defer func() {
		if data != nil && (clientErr != nil || err != nil) {
			data.Recycle()
			data = nil
		}
	}()
	var sep []byte
	sep, err = c.ReadSlice('\n')
	err = stackerr.Wrap(err)
	if err == nil && !bytes.Equal(sep, separatorBytes) {
		clientErr = stackerr.Wrap(ErrInvalidLineSeparator)
	}
	return
}

func (c *Conn) serverError(err error) {
	c.log.Error("Server error: ", err)
	if err == io.ErrUnexpectedEOF {
		return
	}
	err = unwrap(err)
	c.sendResponse(fmt.Sprintf("%s %s", ServerErrorResponse, err))
}

func (c *Conn) sendClientError(err error) error {
	c.log.Error("Client error: ", err)
	err = unwrap(err)
	return c.sendResponse(fmt.Sprintf("%s %s", ClientErrorResponse, err))
}

func (c *Conn) sendResponse(res string) error {
	c.WriteString(res)
	c.WriteString(Separator)
	return c.Flush()
}

// discardCommand discard all input untill next separator.
func (c *Conn) discardCommand() error {
	for {
		lineWithSeparator, err := c.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			continue
		}
		if !bytes.HasSuffix(lineWithSeparator, separatorBytes) {
			continue
		}
		return nil
	}
}

func (c *Conn) Flush() error {
	return stackerr.Wrap(c.Writer.Flush())
}
