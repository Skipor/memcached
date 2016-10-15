package aof

import "github.com/facebookgo/stackerr"

type transaction struct{ *AOF }

func (t *transaction) Write(p []byte) (n int, err error) {
	n, err = t.writer.Write(p)
	err = stackerr.Wrap(err)
	t.size += int64(n)
	return
}

func (t *transaction) Close() (err error) {
	if t.AOF == nil {
		return
	}
	if t.isSyncEveryTransaction() {
		err = t.sync()
	}
	startRotate := t.size > t.config.RotateSize && !t.rotateInProcess
	if startRotate {
		t.rotateInProcess = true
	}
	t.lock.Unlock()
	if startRotate {
		t.startRotate()
	}
	t.AOF = nil
	return
}
