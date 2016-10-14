package aof

import (
	"bufio"
	"io"
	"os"

	"github.com/facebookgo/stackerr"
)

type Rotator interface {
	Rotate(r ROFile, w io.Writer) error
}

type RotatorFunc func(r ROFile, w io.Writer) error

func (f RotatorFunc) Rotate(r ROFile, w io.Writer) error {
	return f(r, w)
}

type ROFile interface {
	io.Reader
	// TODO add more methods for another Rotators.
	// It is no need for them now, and it requires some work to limit wrapped file properly.
	//io.Seeker
	//io.ReaderAt
}

// RotateFile rotates fname file prefix size of limit into w.
func RotateFile(rot Rotator, fname string, limit int64, w io.Writer) (err error) {
	var file *os.File
	file, err = os.Open(fname)
	if err != nil {
		return stackerr.Wrap(err)
	}
	bufW := bufio.NewWriter(w)
	r := io.LimitReader(file, limit)
	r = bufio.NewReader(r)
	err = rot.Rotate(r, bufW)
	if err != nil {
		return stackerr.Wrap(err)
	}
	err = file.Close()
	if err != nil {
		return stackerr.Wrap(err)
	}
	err = bufW.Flush()
	return stackerr.Wrap(err)
}
