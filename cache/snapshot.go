package cache

import (
	"encoding/gob"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/facebookgo/stackerr"

	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

var ErrCacheOverflow = errors.New("readed cache doesn't larger than provided size: some data lost")

type SnapshotReader interface {
	io.Reader
	io.ByteReader
}

// Read reads cache snapshot and make cache from it.
// r as io.ByteReader required, because gob.Decoder will wrap io.Reader into bufio.Reader,
// what will cause extra data read that will remain in bufio.Reader.
func ReadSnapshot(r SnapshotReader, p *recycle.Pool, l log.Logger, conf Config) (c *cache, err error) {
	decoder := gob.NewDecoder(r)
	var info snapshotInfo
	err = decoder.Decode(&info)
	if err != nil {
		err = stackerr.Wrap(err)
		return
	}
	sizes := info.Sizes
	c = NewCache(l, conf)
	c.table = make(map[string]*node, sizes[hot]+sizes[warm]+sizes[cold])
	now := nowUnix()
	discard := newDiscard()
	for li, lru := range c.lrus {
		for i := 0; i < sizes[li]; i++ {
			var meta nodeMeta // Should be zeroed before every decode.
			err = decoder.Decode(&meta)
			if err != nil {
				err = stackerr.Wrap(err)
				return
			}
			if meta.expired(now) {
				err = discard(r, meta.Bytes)
				if err != nil {
					return
				}
				continue
			}
			var data *recycle.Data
			data, err = p.ReadData(r, meta.Bytes)
			if err != nil {
				err = stackerr.Wrap(err)
				return
			}

			n := newNode(Item{meta.ItemMeta, data})
			lru.push(n)
			if meta.Active {
				n.active = active
			}
			c.table[n.Key] = n
		}
	}
	if c.hotOverflow() || c.warmOverflow() || c.totalOverflow() {
		err = ErrCacheOverflow
		c.fixOverflows()
	}
	c.checkInvariants()
	return
}

// Snapshot returns made snapshot. Method requires read lock be acquired.
func (c *cache) Snapshot() *Snapshot {
	lrus := make([]lruSnapshot, temps)
	wg := sync.WaitGroup{}
	wg.Add(temps)
	// Cache can contain millions of nodes. So it is better to make snapshot parallel.
	for cycleIndex := 0; cycleIndex < temps; cycleIndex++ {
		go func(i int) {
			lru := c.lrus[i]
			s := lru.snapshot()
			lrus[i] = s
			wg.Done()
		}(cycleIndex)
	}
	wg.Wait()
	return &Snapshot{lrus}
}

// Snapshot hold cache LRUs state for serialization.
// lruSnapshot is serialized as gob encoded snapshotInfo and sequence of lruSnapshots
// Note: until snapshot write it hold item data readers,
// what prevent data recycle. If snapshot will not be written, all data leak.
type Snapshot struct {
	lrus []lruSnapshot
}

var _ io.WriterTo = (*Snapshot)(nil)

// snapshotInfo contains information about encoded snapshot.
// Is gob encoded, so fields should be exported.
type snapshotInfo struct {
	Sizes [temps]int
}

func (s *Snapshot) WriteTo(w io.Writer) (nn int64, err error) {
	if s.lrus == nil {
		panic("snapshot has been writen already or isn't initialized")
	}
	oldWriter := w
	w = writerFunc(func(p []byte) (n int, err error) {
		n, err = oldWriter.Write(p)
		nn += int64(n)
		return
	})

	encoder := gob.NewEncoder(w)
	err = encoder.Encode(s.info())
	if err != nil {
		err = stackerr.Wrap(err)
		return
	}
	for _, lru := range s.lrus {
		for _, n := range lru.nodes {
			err = encoder.Encode(n.meta)
			if err != nil {
				err = stackerr.Wrap(err)
				return
			}
			_, err = n.reader.WriteTo(w)
			if err != nil {
				err = stackerr.Wrap(err)
				return
			}
			n.reader.Close()
		}
	}
	s.lrus = nil
	return
}

func (s *Snapshot) info() (info snapshotInfo) {
	for i, lru := range s.lrus {
		info.Sizes[i] = len(lru.nodes)
	}
	return
}

// lruSnapshot is serialized as sequence of nodeSnapshots.
type lruSnapshot struct {
	nodes []nodeSnapshot
}

// nodeSnapshot is serialized as gob encoded nodeMeta and raw chunk of data.
type nodeSnapshot struct {
	meta   nodeMeta
	reader *recycle.DataReader
}

type nodeMeta struct {
	Active bool
	ItemMeta
}

func (l *lru) snapshot() lruSnapshot {
	approxNodesNum := 2 * l.size / extraSizePerNode // Decrease allocations number for resize.
	nodes := make([]nodeSnapshot, 0, approxNodesNum)
	for n := l.head(); !l.end(n); n = n.next {
		nodes = append(nodes, n.snapshot())
	}
	return lruSnapshot{nodes}
}

func (n *node) snapshot() nodeSnapshot {
	s := nodeSnapshot{
		nodeMeta{
			Active:   atomic.LoadInt32(&n.active) == active,
			ItemMeta: n.ItemMeta,
		},
		n.Data.NewReader(),
	}

	return s
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func newDiscard() func(io.Reader, int) error {
	var discard []byte
	return func(r io.Reader, n int) (err error) {
		if len(discard) == 0 {
			discard = make([]byte, 4<<10)
		}
		toDiscard := n
		for toDiscard > 0 {
			n := toDiscard
			if n > len(discard) {
				n = len(discard)
			}
			_, err = io.ReadFull(r, discard[:n])
			if err != nil {
				err = stackerr.Wrap(err)
				return
			}
			toDiscard -= n
		}
		return
	}
}
