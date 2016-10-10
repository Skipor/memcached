package cache

import (
	"fmt"
	"sync/atomic"
)

const (
	inactive = iota
	active
)

// Invariants:
// * lru owns nodes between fakeHead and fakeTail.
// * {fakeHead, all owned nodes, fakeTail} are correct doubly linked list.
// * all nodes owned by lru have field node.owner equal to &lru
// * lru.size equal sum of owned nodes size()
type lru struct {
	size int64
	// On callback call node owned by callee, so call back should save invariants.

	onExpire        func(*node)
	onEvictActive   func(*node)
	onEvictInactive func(*node)

	// Fake nodes. Real nodes are between them.
	// nil <- fakeHead <-> node_0 <-> ... <-> node_(n-1) <-> fakeTail -> nil
	// Such structure prevent nil checks in code.

	// fakeHead is bottom of lru. fakeHead.next is most lately added item.
	fakeHead *node

	// fakeTail is top of lru. All new
	fakeTail *node
}

func (l *lru) pushBack(n *node) {
	n.active = inactive
	n.owner = l
	link(l.tail(), n)
	link(n, l.fakeTail)
	l.size += n.size()
}

func (l *lru) shrink(toSize int64, now int64) {
	if toSize < 0 {
		panic(fmt.Sprintf("try shrink to negative size %v", toSize))
	}
	cur, next := l.head(), l.head().next
	for ; toSize < l.size; cur, next = next, next.next {
		l.assertNotTail(cur)
		if cur.expired(now) {
			l.onExpire(cur)
			continue
		}
		l.onEvict(cur)
	}
	link(l.fakeHead, cur)
}

func (l *lru) onEvict(n *node) {
	if n.isActive() {
		l.onEvictActive(n)
	} else {
		l.onEvictInactive(n)
	}
}

func (l *lru) init() {
	l.fakeHead, l.fakeTail = &node{}, &node{}
	link(l.fakeHead, l.fakeTail)
}

func (l *lru) head() *node      { return l.fakeHead.next }
func (l *lru) tail() *node      { return l.fakeTail.prev }
func (l *lru) end(n *node) bool { return n == l.fakeTail }

type node struct {
	Item
	// active can have concurrent and atomic access with read lock acquired,
	// or exclusive access with write lock acquired.
	active int32
	owner  *lru
	prev   *node
	next   *node
}

func newNode(i Item) *node { return &node{Item: i} }

// require read lock be acquired
func (n *node) setActive() { atomic.StoreInt32(&n.active, active) }

// require write lock be acquired
func (n *node) isActive() bool { return n.active == active }

// extraMemoryForItem is approximation how much memory needed to save empty item.
// Without such compensation it is possible to blow up cache with small values.
const extraMemoryPerNode = 256 // Item, recycle.Data, node, two hash table cells.

// MemSize return approximation how much memory needed to save empty item.
func (n *node) size() int64 {
	return int64(extraMemoryPerNode + len(n.Key) + n.Bytes)
}

func (l *lru) assertNotTail(n *node) {
	if n == l.fakeTail {
		panic("node pointer out of range")
	}
}

func link(a, b *node) { a.next, b.prev = b, a }

func moveToTail(n *node) {
	link(n.owner.tail(), n)
	link(n, n.owner.fakeTail)
}

func moveTo(other *lru) func(*node) {
	return func(n *node) {
		n.owner.size -= n.size()
		other.pushBack(n)
	}
}
