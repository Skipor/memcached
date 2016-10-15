package memcached

import (
	"bufio"
	"fmt"
	"io"

	"github.com/facebookgo/stackerr"
	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/log"
)

type conn struct {
	reader
	*bufio.Writer
	closer io.Closer
	*ConnMeta
	log   log.Logger
	cache cache.View
}

func newConn(l log.Logger, m *ConnMeta, cache cache.View, rwc io.ReadWriteCloser) *conn {
	return &conn{
		reader:   newReader(rwc, m.Pool),
		Writer:   bufio.NewWriterSize(rwc, OutBufferSize),
		closer:   rwc,
		ConnMeta: m,
		log:      l,
		cache:    cache,
	}
}

func (c *conn) serve() {
	c.log.Info("Serve connection.")
	defer func() {
		if r := recover(); r != nil {
			c.serverError(stackerr.Newf("Panic: %s", r))
			panic(c)
		}
		c.Close()
		c.log.Info("Connection closed.")
	}()

	err := c.loop()
	if err != nil {
		c.serverError(err)
	}
}

func (c *conn) Close() error {
	c.Flush()
	return c.closer.Close()
}

func (c *conn) loop() error {
	for {
		raw, command, fields, clientErr, err := c.readCommand()
		if err != nil {
			if err == io.EOF {
				// Just client disconnect. Ok.
				return nil
			}
			return stackerr.Wrap(err)
		}
		if clientErr == nil {
			c.log.Debugf("Command: %s.", command)
			switch string(command) { // No allocation.
			case GetCommand, GetsCommand:
				getter := c.cache.NewGetter(raw)
				clientErr, err = c.get(getter, fields)
			case SetCommand:
				setter := c.cache.NewSetter(raw)
				clientErr, err = c.set(setter, fields)
			case DeleteCommand:
				deleter := c.cache.NewDeleter(raw)
				clientErr, err = c.delete(deleter, fields)
			default:
				c.log.Error("Unexpected command: ", command)
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

func (c *conn) get(getter cache.Getter, fields [][]byte) (clientErr, err error) {
	var keys [][]byte
	keys, clientErr = parseGetFields(fields)
	if clientErr != nil {
		return
	}
	views := getter.Get(keys...)

	err = c.sendGetResponse(views)
	return
}

func (c *conn) sendGetResponse(views []cache.ItemView) error {
	c.log.Debugf("Sending %v founded values.", len(views))
	var readerIndex int
	defer func() {
		// Close readers which was not successfully readed.
		for ; readerIndex < len(views); readerIndex++ {
			views[readerIndex].Reader.Close()
		}
	}()
	for ; readerIndex < len(views); readerIndex++ {
		view := views[readerIndex]
		c.log.Debugf("Sending value %v. Key %s.", readerIndex, view.Key)
		c.WriteString(ValueResponse)
		c.WriteByte(' ')
		c.WriteString(view.Key)
		fmt.Fprintf(c, " %v %v"+Separator, view.Flags, view.Bytes)
		view.Reader.WriteTo(c)
		_, err := c.WriteString(Separator)
		if err != nil {
			return stackerr.Wrap(err)
		}
		view.Reader.Close()
	}
	return c.sendResponse(EndResponse)
}

func (c *conn) set(setter cache.Setter, fields [][]byte) (clientErr, err error) {
	var i cache.Item
	var noreply bool
	i.ItemMeta, noreply, clientErr = parseSetFields(fields)
	if clientErr != nil {
		err = c.discardCommand()
		return
	}
	c.log.Debugf("set %#v", i.ItemMeta)

	if i.Bytes > c.MaxItemSize {
		clientErr = stackerr.Wrap(ErrTooLargeItem)
		_, err = c.Discard(i.Bytes + len(Separator))
		return
	}

	i.Data, clientErr, err = c.readDataBlock(i.Bytes)
	if err != nil || clientErr != nil {
		return
	}

	setter.Set(i)

	if noreply {
		err = c.Flush()
		return
	}
	err = c.sendResponse(StoredResponse)
	return
}

func (c *conn) delete(deleter cache.Deleter, fields [][]byte) (clientErr, err error) {
	var key []byte
	var noreply bool
	key, noreply, clientErr = parseDeleteFields(fields)
	if clientErr != nil {
		return
	}
	c.log.Debugf("delete %s; noreply: %v", key, noreply)

	deleted := deleter.Delete(key)

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

func (c *conn) serverError(err error) {
	c.log.Error("Server error: ", err)
	if err == io.ErrUnexpectedEOF {
		return
	}
	err = unwrap(err)
	c.sendResponse(fmt.Sprintf("%s %s", ServerErrorResponse, err))
}

func (c *conn) sendClientError(err error) error {
	c.log.Error("Client error: ", err)
	err = unwrap(err)
	return c.sendResponse(fmt.Sprintf("%s %s", ClientErrorResponse, err))
}

func (c *conn) sendResponse(res string) error {
	c.WriteString(res)
	c.WriteString(Separator)
	return c.Flush()
}

func (c *conn) Flush() error {
	return stackerr.Wrap(c.Writer.Flush())
}
