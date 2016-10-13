package memcached

import (
	"errors"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/internal/tag"
	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

const (
	active int32 = iota
	stoped
)
const DefaultAddr = ":11211"

var ErrStoped = errors.New("memcached server have been stoped")

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
}

// ConnMeta is data shared between connections.
type ConnMeta struct {
	Pool        *recycle.Pool
	MaxItemSize int
}

func (s *Server) ListenAndServe() error {
	if s.Addr == "" {
		s.Addr = DefaultAddr
	}
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(l net.Listener) error {
	s.listener = l
	s.init()
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

func (s *Server) Stop() {
	s.Log.Info("Stopping server.")
	atomic.StoreInt32(&s.stopState, stoped)
	s.listener.Close()
}

func (s *Server) isStoped() bool {
	return atomic.LoadInt32(&s.stopState) == stoped
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
			s.Log.Errorf("recycle.Data leak. Ptr: %p", d)
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
