package memcached

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/facebookgo/stackerr"

	"github.com/skipor/memcached/aof"
	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/internal/util"
	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

// SnapshotCommand indicate that there is cache snapshot after it.
// First byte is invalid for any memcached command, so it is possible to understand
// is command SnapshotCommand by first byte.
const SnapshotCommand = "\x00 LOG FILE STARTS WITH GOB ENCODED CACHE SNAPSHOT \x00" + Separator

func newLoggingCacheViewFabric(l log.Logger, p *recycle.Pool, conf Config) (f *logginCacheViewFabric, err error) {
	c, err := readAOF(p, l, conf)
	if err != nil {
		if cerr, ok := err.(*CorruptedError); ok {
			err = stackerr.Newf("AOF is corrupted, but can be truncated to valid: %v\n"+
				"Pass fix-corrupted option or chose anoter AOF.", cerr.Err)
		}
		stackerr.Newf("AOF can't be read: %v", err)
	}

	rotator := aof.RotatorFunc(func(_ aof.ROFile, w io.Writer) error {
		return writeCacheSnapshot(c, w)
	})
	var AOF *aof.AOF
	AOF, err = aof.Open(l, rotator, conf.AOF)
	if err != nil {
		return
	}
	f = &logginCacheViewFabric{c, AOF}
	return
}

func writeCacheSnapshot(c *cache.LockingLRU, w io.Writer) error {
	_, err := io.WriteString(w, SnapshotCommand)
	if err != nil {
		return stackerr.Wrap(err)
	}
	c.RLock()
	s := c.Snapshot()
	c.RUnlock()
	_, err = s.WriteTo(w)
	return err

}

// ReadAOF try to make cache from AOF.
// WARN: if fixCorrupted is true, on recoverable AOF corruption error
// AOF will be truncated to valid prefix, and no error will be returned.
// If fixCorrupted is false, on recoverable AOF corruption error,
// error type of *CorruptionError will be returned.
func readAOF(p *recycle.Pool, l log.Logger, conf Config) (c *cache.LockingLRU, err error) {
	var f *os.File
	f, err = os.Open(conf.AOF.Name)
	if os.IsNotExist(err) {
		l.Info("AOF is not exists. New will be created.")
		err = nil
		c = cache.NewLockingLRU(l, conf.Cache)
		return
	}
	if err != nil {
		err = stackerr.Wrap(err)
		return
	}
	defer f.Close()
	cr := newCountingReader(f, p)
	c, err = readSnapshotIfAny(cr.reader, l, conf.Cache)
	if cache.IsCacheOverflow(err) {
		l.Warn("Cache overwlow err:", util.Unwrap(err))
		err = nil
	}
	if err != nil {
		return
	}
	var lastValidPos int64
	lastValidPos, err = readCommandLog(cr, c)
	if err != nil {
		if !conf.FixCorruptedAOF {
			err = &CorruptedError{err}
			return
		}
		l.Errorf("AOF is corrupted: %v. Truncating.", err)
		f.Close()
		err = os.Truncate(conf.AOF.Name, lastValidPos)
		if err != nil {
			err = stackerr.Wrap(err)
			return
		}
	}
	return
}

type CorruptedError struct {
	Err error
}

func (e *CorruptedError) Error() string {
	return fmt.Sprint("AOF is corrupted: ", e.Err)
}

func readSnapshotIfAny(r reader, l log.Logger, conf cache.Config) (c *cache.LockingLRU, err error) {
	b, err := r.ReadByte()
	r.UnreadByte()
	if err != nil {
		err = stackerr.Wrap(err)
		return
	}
	if b == SnapshotCommand[0] {
		l.Debug("Reading snapshot.")
		var raw []byte
		raw, _, _, _, err = r.readCommand()
		if err != nil {
			return
		}
		if !bytes.Equal(raw, []byte(SnapshotCommand)) {
			err = stackerr.New("Invalid snapshot command.")
			return
		}
		return cache.ReadLockingLRUSnapshot(r, r.pool, l, conf)
	}
	l.Debug("No snapshot detected.")
	c = cache.NewLockingLRU(l, conf)
	return
}

func readCommandLog(r *countingReader, c cache.Cache) (lastValidPos int64, err error) {
	var (
		command   []byte
		fields    [][]byte
		clientErr error
	)

	for ; ; lastValidPos = r.pos() {
		_, command, fields, clientErr, err = r.readCommand()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		if clientErr != nil {
			err = clientErr
			return
		}

		switch string(command) { // No allocation.
		case GetCommand, GetsCommand:
			var keys [][]byte
			keys, err = parseGetFields(fields)
			if err != nil {
				return
			}
			c.Touch(keys...)

		case SetCommand:
			var meta cache.ItemMeta
			meta, _, err = parseSetFields(fields)
			if err != nil {
				return
			}
			var data *recycle.Data
			data, clientErr, err = r.readDataBlock(meta.Bytes)
			if err != nil {
				return
			}
			if clientErr != nil {
				err = clientErr
				return
			}
			c.Set(cache.Item{meta, data})

		case DeleteCommand:
			var key []byte
			key, _, err = parseDeleteFields(fields)
			if err != nil {
				return
			}
			c.Delete(key)

		default:
			err = stackerr.Newf("Unexpected command: %q", command)
			return
		}
	}

}

func newCountingReader(r io.Reader, p *recycle.Pool) *countingReader {
	cr := &countingReader{}
	count := readerFunc(func(p []byte) (n int, err error) {
		n, err = r.Read(p)
		cr.readedFromUnderlying += int64(n)
		return
	})
	cr.reader = newReader(count, p)
	return cr
}

type countingReader struct {
	reader
	readedFromUnderlying int64
}

func (cr *countingReader) pos() int64 {
	return cr.readedFromUnderlying - int64(cr.Buffered())
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (n int, err error) { return f(p) }
