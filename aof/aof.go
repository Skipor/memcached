package aof

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/facebookgo/stackerr"

	"github.com/skipor/memcached/log"
)

const MinSyncPeriod = 100 * time.Millisecond
const MinRotateCompress = 0.7
const Perm = 0664 // TODO make configurable.

type file interface {
	io.WriteCloser
	Sync() error
}

type Config struct {
	Name       string
	SyncPeriod time.Duration
	RotateSize int64 // AOF size, after which Rotator will be called.
	BuffSize   int   // 0 if no buffering.
}

// AOF represents Append Only File.
type AOF struct {
	config  Config
	rotator Rotator
	log     log.Logger
	sync.Mutex
	// writer is current proxy io.Writer to write AOF.
	// It can be file, *bufio.Writer or another proxy.
	writer io.Writer
	// If buffering is on, flusher.Flush() flushes buffer into file.
	flusher flusher
	file    file
	// Current AOF size.
	size            int64
	rotateInProcess bool
}

func Open(log log.Logger, r Rotator, conf Config) (aof *AOF, err error) {
	aof = &AOF{
		log:     log,
		rotator: r,
		config:  conf,
	}
	err = aof.init()
	if err != nil {
		return
	}
	if !aof.isSyncEveryTransaction() {
		aof.startSync()
	}
	return
}

func (f *AOF) init() (err error) {
	var file *os.File
	file, err = os.OpenFile(f.config.Name, os.O_WRONLY|os.O_APPEND, Perm|os.ModeAppend)
	if err != nil {
		return stackerr.Wrap(err)
	}
	stat, err := file.Stat()
	if err != nil {
		return stackerr.Wrap(err)
	}
	f.size = stat.Size()
	f.file = file

	if f.config.BuffSize == 0 {
		f.writer = file
		f.flusher = nopFlusher{}
		return
	}
	bufWriter := bufio.NewWriterSize(f.file, f.config.BuffSize)
	f.writer = bufWriter
	f.flusher = bufWriter
	f.log.Debug("AOF opened.")
	return
}

func (f *AOF) isSyncEveryTransaction() bool {
	return f.config.SyncPeriod < MinSyncPeriod
}

func (f *AOF) sync() (err error) {
	err = f.flusher.Flush()
	if err != nil {
		return stackerr.Wrap(err)
	}
	err = f.file.Sync()
	return stackerr.Wrap(err)
}

func (f *AOF) isClosed() bool {
	return f.file == nil
}

func (f *AOF) Close() error {
	f.flusher.Flush()
	err := f.file.Close()
	f.file = nil // Mark as closed.
	return stackerr.Wrap(err)
}

// NewTransaction create new AOF transaction.
// Returned transaction hold AOF lock until close,
// so callee should write data and close it, as soon as possible.
func (f *AOF) NewTransaction() io.WriteCloser {
	f.Lock()
	return &transaction{f}
}

// rotate start background rotation of file snapshot into new file.
// While rotation in process, all appended data is buffering in memory.
// When rotation complete, all buffered data is appended to new file and
// old file is atomically substituted with new.
// rotate should be called without acquired lock.
func (f *AOF) startRotate() {
	go func() {
		assertNoErr := func(err error) {
			if err != nil {
				f.log.Panicf("AOF roatation error: %v", err)
			}
		}
		f.log.Info("AOF rotation started.")
		// Note: No recover. Crushing program on error.
		// So no unlocks in defer.
		newFile, err := newRotationFile()
		assertNoErr(err)

		// Buffer for extra data appended after rotation start.
		extra := &bytes.Buffer{}

		f.Lock()
		if f.rotateInProcess == false {
			f.log.Panic("AOF rotation in process, but flag is not set.")
		}
		oldWriter := f.writer
		f.writer = io.MultiWriter(oldWriter, extra)
		size := f.size
		f.Unlock()

		afterFileSnapshotRotationTestHook()

		f.log.Debug("AOF snapshot rotation started.")
		err = RotateFile(f.rotator, f.config.Name, size, newFile)
		assertNoErr(err)
		newFileStat, err := newFile.Stat()
		assertNoErr(err)
		if newFileStat.Size() > size*(MinRotateCompress*100)/100 {
			f.log.Panic("rotation doesn't compress AOF enough")
		}
		f.log.Debug("AOF snapshot rotation finished.")

		// Meanwhile extra can grow large. Writing it in background decreases lock time.
		newExtra := &bytes.Buffer{}

		f.Lock()
		f.writer = io.MultiWriter(oldWriter, newExtra)
		f.Unlock()

		_, err = extra.WriteTo(newFile)
		assertNoErr(err)
		err = newFile.Sync() // Do without lock as much work, as we can.
		assertNoErr(err)
		newFileName := newFile.Name()

		afterExtraWriteRotationTestHook()

		f.Lock()
		_, err = newExtra.WriteTo(newFile)
		assertNoErr(err)

		err = f.Close()
		assertNoErr(err)
		err = newFile.Close()
		assertNoErr(err)

		err = os.Rename(newFileName, f.config.Name) // Atomic. No data corruption on fail.
		assertNoErr(err)
		err = f.init()
		assertNoErr(err)
		f.rotateInProcess = false
		f.Unlock()
		f.log.Info("AOF rotation finished.")

		afterFinishTakeRotationTestHook()
	}()
}

var (
	afterFileSnapshotRotationTestHook = func() {}
	afterExtraWriteRotationTestHook   = func() {}
	afterFinishTakeRotationTestHook   = func() {}
)

func (f *AOF) startSync() {
	go func() {
		ticker := time.NewTicker(f.config.SyncPeriod)
		defer ticker.Stop()
		var prevSize int64
		for {
			_ = <-ticker.C
			f.Lock()
			if f.isClosed() {
				f.Unlock()
				return
			}
			if f.size != prevSize {
				prevSize = f.size
				f.sync()
			}
			f.Unlock()
		}
	}()
}

func newRotationFile() (file *os.File, err error) {
	file, err = ioutil.TempFile("", "rotating_aof_")
	if err != nil {
		err = stackerr.Wrap(err)
		return
	}
	err = file.Chmod(Perm)
	err = stackerr.Wrap(err)
	return
}
