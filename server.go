package memcached

import (
	"net"
	"os"
	"time"

	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

type Server struct {
	Addr string
	ConnMeta
	Log         log.Logger
	connCounter int64
}

// ConnMeta is data shared between connections.
type ConnMeta struct {
	Cache       cache.Cache
	Pool        *recycle.Pool
	MaxItemSize int
}

func (s *Server) ListenAndServe() error {
	if s.Addr == "" {
		s.Addr = ":11211"
	}
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(l net.Listener) error {
	s.init()
	var tempDelay time.Duration // How long to sleep on accept failure.
	for {
		c, err := l.Accept()
		if err != nil {
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

func (s *Server) newConn(c net.Conn) *conn {
	conn := newConn(s.Log.WithFields(log.Fields{"conn": s.connCounter}), &s.ConnMeta, c)
	s.connCounter++
	return conn
}

func (s *Server) init() {
	if s.Log == nil {
		s.Log = log.NewLogger(log.ErrorLevel, os.Stderr)
	}
	s.ConnMeta.init()
	maxChunkSize := s.Pool.MaxChunkSize()
	if maxChunkSize < InBufferSize || maxChunkSize < OutBufferSize {
		s.Log.Panic("Too small max chunk size. It should be larger than buffers size, to zero copy send of large items.")
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
