package cache

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"fmt"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
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
		Expect(n.prev.next).To(Equal(n))
		Expect(n.owner).To(Equal(l))
	}
	Expect(l.tail().next).To(Equal(l.fakeTail))
	Expect(actualSize).To(Equal(l.size))
}

func (l *lru) nodes() []*node {
	var nodes []*node
	for n := l.head(); !l.end(n); n = n.next {
		nodes = append(nodes, n)
	}
	return nodes
}

var testKey, resetKeys = func() (k func() string, rk func()) {
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

func (c *cache) checkInvariants() {
	var items int
	for _, l := range c.lrus {
		l.ExpectInvariantsOk()
		for n := l.head(); !l.end(n); n = n.next {
			items++
			tn, ok := c.table[n.Key]
			Expect(ok).To(BeTrue())
			Expect(tn).To(BeIdenticalTo(n))
		}
	}
	Expect(items).To(Equal(len(c.table)))
	Expect(!c.overflow())
	Expect(!c.hotOverflow())
	Expect(!c.warmOverflow())
}
