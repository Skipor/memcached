package memcached

import (
	"bufio"
	"fmt"
	"io"

	"github.com/facebookgo/stackerr"

	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

type Conn struct {
	reader
	*bufio.Writer
	closer      io.Closer
	log         log.Logger
	handler     Handler
	maxItemSize int
}

func NewConn(conn io.ReadWriteCloser, h Handler, pool *recycle.Pool, log log.Logger) *Conn {
	return &Conn{
		reader:  newReader(conn, pool),
		Writer:  bufio.NewWriterSize(conn, OutBufferSize),
		closer:  conn,
		handler: h,
		log:     log,
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
		// Close readers which was not successfully readed.
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

	i.data, clientErr, err = c.readDataBlock(i.bytes)
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

func (c *Conn) Flush() error {
	return stackerr.Wrap(c.Writer.Flush())
}
