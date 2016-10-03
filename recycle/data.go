package recycle

import (
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
	return &DataReader{
		data: d,
	}
}

func (d *Data) Recycle() {
	if !atomic.CompareAndSwapInt32(&d.recycleCalled, 0, 1) {
		panic("second recycle call")
	}
	d.decReference()
}

type DataReader struct {
	data       *Data
	chunkIndex int
	byteIndex  int
}

var _ interface {
	io.ReadCloser
	io.WriterTo
} = (*DataReader)(nil)

func (r *DataReader) Read(p []byte) (int, error) {
	panic("use WriteTo (io.Copy can do it for you) to avoid copy and allocations")
}

func (r *DataReader) WriteTo(w io.Writer) (n int64, err error) {
	var nChunk int
	for r.chunkIndex < len(r.data.chunks) {
		chunk := r.data.chunks[r.chunkIndex]
		nChunk, err = w.Write(chunk[r.byteIndex:])
		n += (int64)(nChunk)
		if err != nil {
			r.byteIndex += nChunk
			return
		}
		r.byteIndex = 0
		r.chunkIndex++
	}
	return
}

func (r *DataReader) Close() error {
	if !r.isClosed() {
		// It is good style to handle multiple Close calls,
		// because exists some bad code than can call Close on Reader silently.
		// It is bad style do such things.
		r.data.decReference()
		r.data = nil
	}
	return nil
}

func (r *DataReader) isClosed() bool {
	return r.data == nil
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
