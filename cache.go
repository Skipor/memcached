package memcached

import (
	"sync"
	"sync/atomic"

	"github.com/skipor/memcached/recycle"
)

type Temp uint8

const (
	Cold Temp = iota
	Warm
	Hot
	MaxTemp = Hot
)
const (
	inActive = iota
	active
)

type Node struct {
	Item
	active int32 // Atomic concurrent access. 0 or 1.
	prev   *Node
	next   *Node
}

func (n *Node) SetActive(a bool) {
	var activeInt32 int32
	if a {
		activeInt32 = active
	} else {
		activeInt32 = inActive
	}
	atomic.StoreInt32(&n.active, activeInt32)
}

func (n *Node) Active() bool {
	return atomic.LoadInt32(&n.active) == active
}

// extraMemoryForItem is approximation how much memory needed to save empty item.
// Without such compensation it is possible to blow up cache with small values.
const extraMemoryForItem = 256 // Item, ItemData, Node, two hash table cells.

type ItemMeta struct {
	key     string
	flags   uint32
	exptime int64
	bytes   int
}

type Item struct {
	ItemMeta
	data *recycle.Data
}

func (i Item) NewView() ItemView {
	return ItemView{
		i.ItemMeta,
		i.data.NewReader(),
	}
}

type ItemView struct {
	ItemMeta
	Reader *recycle.DataReader
}

// MemSize return approximation how much memory needed to save empty item.
func (n *Node) MemSize() int {
	return extraMemoryForItem + len(n.key) + n.bytes
}

type ItemList struct {
	memSize int64
	// Fake nodes.
	head *Node
	tail *Node
}

type Cache struct {
	sync.RWMutex
	table             map[string]*Node
	lists             [MaxTemp]ItemList
	dataSizeLimit     int64
	hotDataSizeLimit  int64
	warmDataSizeLimit int64
}

func (c *Cache) memSize() int64 {
	var size int64
	for i := range c.lists[:] {
		size += c.lists[i].memSize
	}
	return size
}

// Snapshot requires write lock be acquired.
func (c *Cache) Snapshot() *CacheSnapshot {
	// TODO after main logic
	panic("NIY")
}

type CacheSnapshot struct {
}
