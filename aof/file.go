package aof

import "io"

//go:generate mockery -name=file -inpkg -testonly

type file interface {
	io.WriteCloser
	Sync() error
}
