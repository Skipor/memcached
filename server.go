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
}

// ConnMeta is data shared between connections.
type ConnMeta struct {
	Cache       cache.Cache
	Log         log.Logger
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
		go s.newConn(c).Serve()
	}
}

func (s *Server) newConn(c net.Conn) *conn {
	return newConn(&s.ConnMeta, c)
}

func (s *Server) init() {
	if s.Log == nil {
		s.Log = log.NewLogger(log.ErrorLevel, os.Stderr)
	}
	if s.Pool == nil {
		s.Pool = recycle.NewPool()
	}
	if s.MaxItemSize == 0 {
		s.MaxItemSize = DefaultMaxItemSize
	}
}
