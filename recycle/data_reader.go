package recycle

import "io"

type DataReader struct {
	data       *Data
	chunkIndex int
	byteIndex  int
}

var _ interface {
	io.ReadCloser
	io.WriterTo
} = (*DataReader)(nil)

func (r *DataReader) WriteTo(w io.Writer) (nn int64, err error) {
	for !r.eof() {
		var n int
		n, err = w.Write(r.chunk())
		r.readed(n)
		nn += (int64)(n)
		if err != nil {
			return
		}
	}
	return
}

// Read method is for test purpose only. WriteTo should be uses instead.
func (r *DataReader) Read(p []byte) (nn int, err error) {
	// panic("use WriteTo (io.Copy can do it for you) to avoid copy and allocations")
	for nn < len(p) && !r.eof() {
		n := copy(p[nn:], r.chunk())
		r.readed(n)
		nn += n
	}
	if r.eof() {
		err = io.EOF
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

func (r *DataReader) eof() bool {
	return r.chunkIndex >= len(r.data.chunks)
}

func (r *DataReader) chunk() []byte {
	return r.data.chunks[r.chunkIndex][r.byteIndex:]
}

func (r *DataReader) readed(n int) {
	if n < len(r.chunk()) {
		r.byteIndex += n
		return
	}
	r.chunkIndex++
	r.byteIndex = 0
}
