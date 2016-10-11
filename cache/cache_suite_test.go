package cache

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/onsi/gomega/format"
	"github.com/skipor/memcached/recycle"
	. "github.com/skipor/memcached/testutil"
)

func TestCache(t *testing.T) {
	format.MaxDepth = 4
	format.UseStringerRepresentation = true
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cache Suite")
}

func (l *lru) ExpectInvariantsOk() {
	Expect(l.fakeHead.prev).To(BeNil())
	Expect(l.fakeTail.next).To(BeNil())
	Expect(l.fakeHead.owner).To(BeNil())
	Expect(l.fakeTail.owner).To(BeNil())
	var actualSize int64
	for n := l.head(); !l.end(n); n = n.next {
		actualSize += n.size()
		Expect(n.prev.next).To(BeIdenticalTo(n))
		Expect(n.owner).To(BeIdenticalTo(l))
	}
	Expect(l.tail().next).To(BeIdenticalTo(l.fakeTail))
	Expect(actualSize).To(BeIdenticalTo(l.size))
}

func (c *cache) ExpectInvariantsOk() {
	var items int
	for _, l := range c.lrus {
		l.ExpectInvariantsOk()
		for n := l.head(); !l.end(n); n = n.next {
			items++
			tn, ok := c.table[n.Key]
			Expect(ok).To(BeTrue(), n.Key, "no table ref to item")
			Expect(tn).To(BeIdenticalTo(n), "table refs to another node")
		}
	}
	ExpectWithOffset(1, items).To(Equal(len(c.table)), "too many items in table")
	ExpectWithOffset(1, c.totalOverflow()).To(BeFalse(), "total overflow")
	ExpectWithOffset(1, c.hotOverflow()).To(BeFalse(), "hot overflow")
	ExpectWithOffset(1, c.warmOverflow()).To(BeFalse(), "warm overflow")
}

func (l *lru) nodes() (nodes []*node) {
	for n := l.head(); !l.end(n); n = n.next {
		nodes = append(nodes, n)
	}
	return
}

func (l *lru) items() (items []Item) {
	for n := l.head(); !l.end(n); n = n.next {
		items = append(items, n.Item)
	}
	return
}

var testKey, resetTestKeys = func() (k func() string, rk func()) {
	var i int
	k = func() string {
		key := fmt.Sprintf("test_key_%v", i)
		i++
		return key
	}
	rk = func() {
		i = 0
	}
	return
}()

func now() int64 {
	return time.Now().Unix()
}

type testPool struct{ *recycle.Pool }

func newTestPool() testPool {
	return testPool{recycle.NewPool()}
}

func (p testPool) testItem() (i Item) {
	i.Key = testKey()
	i.Exptime = now() + 100
	i.Bytes = testNodeSize - int((&node{Item: i}).size())
	i.Data, _ = p.ReadData(Rand, i.Bytes)
	return
}

func (p testPool) testNode() *node {
	return newNode(p.testItem())
}

func testNode() *node {
	n := expiredNode()
	n.Exptime = now() + 100
	return n
}

func expiredNode() *node {
	n := newNode(Item{ItemMeta{Key: testKey()}, nil})
	n.Bytes = testNodeSize - int(n.size())
	return n
}
