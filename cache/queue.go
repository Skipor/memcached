package cache

import (
	"fmt"
	"sync/atomic"

	"github.com/skipor/memcached/internal/tag"
)

const (
	inactive int32 = iota
	active
)

// Pre and post conditions (Invariants) for pushBack and shrink methods:
// * queue owns nodes between fakeHead and fakeTail.
// * {fakeHead, all owned nodes, fakeTail} are correct doubly linked list.
// * all nodes owned by queue have field node.owner equal to &queue
// * queue.size equal sum of owned nodes size()
// * there are no recycled data in nodes.
type queue struct {
	size int64
	// callbacks called in shrink.
	// Callback should save queue invariants: attach to same owner or disown node.
	callbacks

	// Fake nodes. Real nodes are between them.
	// nil <- fakeHead <-> node_0 <-> ... <-> node_(n-1) <-> fakeTail -> nil
	// Such structure prevent nil checks in code.

	// fakeHead is bottom of queue. fakeHead.next is most lately added item.
	fakeHead *node

	// fakeTail is top of queue. All new added before fakeTail.
	fakeTail *node
}

type callbacks struct {
	onExpire   func(*node)
	onActive   func(*node)
	onInactive func(*node)
}

// For debug output.
const fakeHeadKey = " !HEAD! "
const fakeTailKey = " !TAIL! "

func newQueue() *queue {
	l := &queue{}
	l.fakeHead, l.fakeTail = &node{}, &node{}
	l.fakeHead.Key = fakeHeadKey
	l.fakeTail.Key = fakeTailKey
	link(l.fakeHead, l.fakeTail)
	return l
}

func (q *queue) push(n *node) {
	n.owner = q
	q.size += n.size()
	attachAsInactive(n)
}

// shrink detach nodes from head to tail, and call callback chosen on node state
// (expired, active, inactive). Nodes detached in shrink have invalid node.prev pointer.
// node.next is valid during callback call.
func (q *queue) shrink(toSize int64, now int64) {
	if toSize < 0 {
		panic(fmt.Sprintf("try shrink to negative size %v", toSize))
	}
	q.shrinkWhile(func() bool {
		return toSize < q.size
	}, now)
}

func (q *queue) shrinkWhile(while func() bool, now int64) {
	cur, next := q.head(), q.head().next
	for ; while(); cur, next = next, next.next {
		q.assertNotTail(cur)
		if tag.Debug {
			cur.prev = nil
		}
		if cur.expired(now) {
			q.onExpire(cur)
			continue
		}
		if cur.isActive() {
			q.onActive(cur)
			continue
		}
		q.onInactive(cur)
	}
	link(q.fakeHead, cur)
}

func (q *queue) head() *node { return q.fakeHead.next }
func (q *queue) tail() *node { return q.fakeTail.prev }
func (q *queue) end(n *node) bool {
	if tag.Debug {
		if n.owner != q {
			panic("check end of not owned node")
		}
	}
	return n == q.fakeTail
}
func (q *queue) empty() bool { return q.size == 0 }

type node struct {
	Item
	// active can have concurrent and atomic access with read lock acquired,
	// or exclusive access with write lock acquired.
	active int32
	owner  *queue
	prev   *node
	next   *node
}

func newNode(i Item) *node { return &node{Item: i} }

func (n *node) disown() {
	n.owner.size -= n.size()
	if tag.Debug {
		n.owner = nil
	}
}

func (n *node) detach() {
	link(n.prev, n.next)
	if tag.Debug {
		n.prev = nil
		n.next = nil
	}
}

// require read lock be acquired
func (n *node) setActive() { atomic.StoreInt32(&n.active, active) }

// require write lock be acquired
func (n *node) isActive() bool { return n.active == active }

// extraMemoryForItem is approximation how much memory needed to save empty item.
// Without such compensation it is possible to blow up cache with small values.
const extraSizePerNode = 256 // Item, recycle.Data, node, two hash table cells.

// MemSize return approximation how much memory needed to save empty item.
func (n *node) size() int64 {
	return int64(extraSizePerNode + len(n.Key) + n.Bytes)
}

func (q *queue) assertNotTail(n *node) {
	if n == q.fakeTail {
		panic("node pointer out of range")
	}
}

func link(a, b *node) { a.next, b.prev = b, a }

func attachAsInactive(n *node) {
	n.active = inactive
	link(n.owner.tail(), n)
	link(n, n.owner.fakeTail)
}

func moveTo(other *queue) func(*node) {
	return func(n *node) {
		n.disown()
		other.push(n)
	}
}

func (n *node) GoString() string {
	key := func(n *node) interface{} {
		if n == nil {
			return nil
		}
		return n.Key
	}
	return fmt.Sprintf("{Item:%#v, active:%v, owner:%p, prev:%v, next:%v}",
		n.Item, n.isActive(), n.owner, key(n.prev), key(n.next))
}

var _ fmt.GoStringer
