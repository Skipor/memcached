package memcached

import (
	"errors"
	"io"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Skipor/memcached/aof"
	"github.com/Skipor/memcached/cache"
	"github.com/Skipor/memcached/internal/tag"
	"github.com/Skipor/memcached/log"
	"github.com/Skipor/memcached/recycle"
)

const DefaultAddr = ":11211"

var ErrStoped = errors.New("memcached server have been stoped")

type Config struct {
	Addr           string
	LogDestination io.Writer
	LogLevel       log.Level

	MaxItemSize int64
	Cache       cache.Config

	FixCorruptedAOF bool
	AOF             aof.Config
}

func NewServer(conf Config) (s *Server, err error) {
	l := log.NewLogger(conf.LogLevel, conf.LogDestination)
	p := recycle.NewPool()
	if err != nil {
		return
	}

	var onStop func()
	var newCacheView func() cache.View
	if conf.AOF.Name != "" {
		var fabric *logginCacheViewFabric
		fabric, err = newLoggingCacheViewFabric(l, p, conf)
		if err != nil {
			return
		}
		newCacheView = fabric.New

		// We need to flush and sync AOF data on quit.
		onStop = func() {
			err := fabric.aof.Close()
			if err != nil {
				l.Error("AOF close error: ", err)
			}
		}
	} else {
		c := cache.NewLRU(l, conf.Cache)
		newCacheView = func() cache.View {
			return c
		}
	}

	s = &Server{
		Addr:         conf.Addr,
		Log:          l,
		NewCacheView: newCacheView,
		ConnMeta: ConnMeta{
			Pool:        p,
			MaxItemSize: int(conf.MaxItemSize),
		},
		onStop: onStop,
	}
	l.Debugf("Config: %#v", conf)
	return
}

// Server serves memcached text protocol over tcp.
// Only Cache field is required, other have reasonable defaults.
type Server struct {
	ConnMeta
	Addr         string
	Log          log.Logger
	NewCacheView func() cache.View
	connCounter  int64

	stopState int32 // Atomic.
	listener  net.Listener
	onStop    func()
	sigs      chan os.Signal
}

// connMeta is data shared between connections.
type ConnMeta struct {
	Pool        *recycle.Pool
	MaxItemSize int
}

func (s *Server) ListenAndServe() error {
	if s.Addr == "" {
		s.Addr = DefaultAddr
	}
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(l)
}

func (s *Server) Serve(l net.Listener) error {
	s.listener = l
	s.init()
	if s.onStop != nil {
		s.sigs = make(chan os.Signal)
		signal.Notify(s.sigs, syscall.SIGINT, syscall.SIGTERM)
		defer func() {
			s.onStop()
			close(s.sigs)
		}()
		go func() {
			sig, ok := <-s.sigs
			if !ok {
				return
			}
			s.Log.Info("Signal received: ", sig)
			s.onStop()
			os.Exit(0)
		}()
	}
	// Temporary errors handling copy-pasted from http.Server.Serve().
	var tempDelay time.Duration // How long to sleep on accept failure.
	for {
		c, err := l.Accept()
		if err != nil {
			if s.isStoped() {
				s.Log.Info("Server was stopped. Accept return: ", err)
				return ErrStoped
			}
			if ne, ok := err.(net.Error); !(ok && ne.Temporary()) {
				return err
			}
			if tempDelay == 0 {
				tempDelay = 5 * time.Millisecond
			} else {
				tempDelay *= 2
			}
			if max := 1 * time.Second; tempDelay > max {
				tempDelay = max
			}
			s.Log.Errorf("memcached: Accept error: %v; retrying in %v", err, tempDelay)
			time.Sleep(tempDelay)
			continue
		}
		tempDelay = 0
		go s.newConn(c).serve()
	}
}

const (
	serverActive int32 = iota
	serverStopped
)

func (s *Server) Stop() {
	s.Log.Info("Stopping server.")
	atomic.StoreInt32(&s.stopState, serverStopped)
	s.listener.Close()
	// Accept will return error, and listening goroutine will call s.onStop().
}

func (s *Server) isStoped() bool {
	return atomic.LoadInt32(&s.stopState) == serverStopped
}

func (s *Server) newConn(c net.Conn) *conn {
	conn := newConn(
		s.Log.WithFields(log.Fields{"conn": s.connCounter}),
		&s.ConnMeta,
		s.NewCacheView(),
		c,
	)
	s.connCounter++
	return conn
}

func (s *Server) init() {

	if s.Log == nil {
		s.Log = log.NewLogger(log.ErrorLevel, os.Stderr)
	}
	s.ConnMeta.init()
	if s.NewCacheView == nil {
		s.Log.Panic("No cache fabric provided.")
	}

	maxChunkSize := s.Pool.MaxChunkSize()
	if maxChunkSize < InBufferSize || maxChunkSize < OutBufferSize {
		s.Log.Panic("Too small max chunk size. It should be larger than buffers size, for zero copy send of large items.")
	}
	if tag.Debug {
		s.Pool.SetLeakCallback(func(d *recycle.Data) {
			s.Log.Errorf("recycle.Data not recycled. Ptr: %p", d)
		})
	}
}

func (m *ConnMeta) init() {
	if m.Pool == nil {
		m.Pool = recycle.NewPool()
	}
	if m.MaxItemSize == 0 {
		m.MaxItemSize = DefaultMaxItemSize
	}
}
