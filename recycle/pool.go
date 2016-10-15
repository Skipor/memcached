// Package recycle contains utilities for recyclable, concurrent read only memory usage.
package recycle

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"
)

const minDefChunkSize = 1 << 7
const maxDefChunkSize = 1 << 20

var DefaultChunkSizes = func() (sz []int) {
	for chSz := minDefChunkSize; chSz <= maxDefChunkSize; chSz *= 2 {
		sz = append(sz, chSz)
	}
	return
}()

// TODO bench for performance and allocations. Single and concurrent.

type Pool struct {
	leakCallback LeakCallback
	chunkSizes   []int
	chunkPools   []sync.Pool
}

func NewPool() *Pool {
	return NewPoolSizes(DefaultChunkSizes)
}

// NewPoolSizes creates new pool, which produce chunks with sizes described in chunkSizes.
// chunkSizes should be sorted.
func NewPoolSizes(chunkSizes []int) *Pool {
	if chunkSizes == nil {
		chunkSizes = DefaultChunkSizes[:]
	}
	for i := 0; i < len(chunkSizes); i++ {
		size := chunkSizes[i]
		if size <= 0 {
			panic("non positive size")
		}
		if i != 0 && chunkSizes[i-1] >= size {
			panic("sizes unsorted or have duplicates")
		}
	}
	chunkPools := make([]sync.Pool, len(chunkSizes))
	for i := range chunkSizes {
		size := chunkSizes[i] // Move into range declaration cause using same size.
		chunkPools[i].New = func() interface{} {
			return make([]byte, size)
		}
	}
	return &Pool{
		chunkSizes: chunkSizes,
		chunkPools: chunkPools,
	}
}

func (p *Pool) ReadData(r io.Reader, size int) (*Data, error) {
	chunksNum := (size + p.MaxChunkSize() - 1) / p.MaxChunkSize()
	chunks := make([][]byte, chunksNum)
	for i := 0; i < chunksNum; i++ {
		chunks[i] = p.chunk(size)
		n, err := io.ReadFull(r, chunks[i])
		if err != nil {
			return nil, err
		}
		size -= n
	}

	d := newData(p, chunks)
	if p.leakCallback != nil {
		runtime.SetFinalizer(d, checkLeakFinalizer(p.leakCallback))
	}
	return d, nil
}

type LeakCallback func(*Data)

// SetLeakCallback sets callback, which is called before GC of not recycled data.
// Note: this is for test and debug purpose only.
func (p *Pool) SetLeakCallback(cb LeakCallback) {
	p.leakCallback = cb
}

func NotifyOnLeak(leak chan<- *Data) LeakCallback {
	return func(d *Data) {
		select {
		case leak <- d:
		case <-time.After(5 * time.Second):
			panic("Nobody is listening for leak notification")
		}
	}
}

var PanicOnLeak LeakCallback = func(d *Data) {
	panic(fmt.Sprintf("recycle.Data leaked: %#v.", d))
}
var WarnOnLeak LeakCallback = func(d *Data) {
	println("WARN: recycle.Data leaked.")
}

func (p *Pool) recycleData(d *Data) {
	for _, ch := range d.chunks {
		p.recycleChunk(ch)
	}
}

// chunk return chunk for Data.
// returned slice len equal to size or p.maxChunkSize()
func (p *Pool) chunk(size int) []byte {
	if p.isGCChunkSize(size) {
		// GC will handle such case better.
		return make([]byte, size)
	}
	var i int
	// O(n) but len(chunkSizes) should be <= 30 normally.
	for i = range p.chunkSizes {
		if size <= p.chunkSizes[i] {
			return p.chunkPools[i].Get().([]byte)[0:size]
		}
	}
	return p.chunkPools[i].Get().([]byte)
}

func (p *Pool) recycleChunk(chunk []byte) {
	size := cap(chunk)
	if p.isGCChunkSize(size) {
		// Garbage, that should be collected by GC.
		return
	}
	// O(n) but len(chunkSizes) should be <= 30 normally.
	for i := range p.chunkSizes {
		if size == p.chunkSizes[i] {
			p.chunkPools[i].Put(chunk[:size])
			return
		}
	}
	panic(fmt.Errorf("unexpected chunk size: %s", size))
}

func (p *Pool) MinChunkSize() int {
	return p.chunkSizes[0]
}

func (p *Pool) MaxChunkSize() int {
	return p.chunkSizes[len(p.chunkSizes)-1]
}

func (p *Pool) isGCChunkSize(size int) bool {
	return size <= p.MinChunkSize()/2
}

func checkLeakFinalizer(cb LeakCallback) func(*Data) {
	return func(d *Data) {
		if !d.isRecycled() {
			cb(d)
		}
	}
}
