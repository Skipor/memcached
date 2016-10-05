package memcached

import (
	"errors"
	"net"
	"os"
	"time"

	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

type Server struct {
	Handler Handler
	Addr    string
	Log     log.Logger
	Pool    *recycle.Pool
}

func (s *Server) ListenAndServe() error {
	if s.Addr == "" {
		return errors.New("empty address")
	}
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln.(*net.TCPListener))
}

func (s *Server) Serve(l net.Listener) error {
	if s.Log == nil {
		s.Log = log.NewLogger(log.ErrorLevel, os.Stderr)
	}
	if s.Pool == nil {
		s.Pool = recycle.NewPool()
	}
	var tempDelay time.Duration // How long to sleep on accept failure.
	for {
		conn, e := l.Accept()
		if e != nil {
			ne, ok := e.(net.Error)
			if !(ok && ne.Temporary()) {
				return e
			}
			if tempDelay == 0 {
				tempDelay = 5 * time.Millisecond
			} else {
				tempDelay *= 2
			}
			if max := 1 * time.Second; tempDelay > max {
				tempDelay = max
			}
			s.Log.Errorf("memcached: Accept error: %v; retrying in %v", e, tempDelay)
			time.Sleep(tempDelay)
			continue
		}
		tempDelay = 0
		c := NewConn(conn, s.Handler, s.Pool, s.Log)
		go c.Serve()
	}
}
