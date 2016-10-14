package recycle

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Data represents data which can have multiple concurrent readers
// and should been recycled in pool after Recycle call and when all concurrent reads are finished.
type Data struct {
	pool          *Pool
	recycleCalled int32 // Atomic.
	references    int32 // Atomic.
	chunks        [][]byte
}

func newData(p *Pool, chunks [][]byte) *Data {
	return &Data{
		pool:       p,
		references: 1,
		chunks:     chunks,
	}
}

func (d *Data) NewReader() *DataReader {
	if atomic.LoadInt32(&d.recycleCalled) == 1 {
		panic("read access after recycle call")
	}
	atomic.AddInt32(&d.references, 1)
	return &DataReader{data: d}
}

func (d *Data) Recycle() {
	if !atomic.CompareAndSwapInt32(&d.recycleCalled, 0, 1) {
		panic("second recycle call")
	}
	d.decReference()
}

func (d *Data) WriteTo(w io.Writer) (nn int64, err error) {
	r := d.NewReader()
	nn, err = r.WriteTo(w)
	r.Close()
	return
}

func (d *Data) decReference() {
	readersLeft := atomic.AddInt32(&d.references, -1)
	if readersLeft == 0 {
		if atomic.LoadInt32(&d.recycleCalled) != 1 {
			panic("no readers but recycle not called")
		}
		d.pool.recycleData(d)
		d.pool = nil
		d.chunks = nil
	}
}

func (d *Data) isRecycled() bool {
	return d.pool == nil
}

func (d *Data) GoString() string {
	return fmt.Sprintf("{recycleCalled:%v, refs:%v, chunks:%v}",
		d.recycleCalled == 1, d.references, d.chunks)

}
