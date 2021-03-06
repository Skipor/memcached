// +build debug

// Gomega should not be dependency in non-debug build.

package cache

import (
	"errors"
	"log"

	"github.com/facebookgo/stackerr"
	. "github.com/onsi/gomega"
)

var _ = func() (_ struct{}) {
	RegisterFailHandler(GomegaFailHandler)
	return
}()

func GomegaFailHandler(message string, callerSkip ...int) {
	skip := callerSkip[0] + 1
	log.Fatal("FATAL: invariants are broken:", stackerr.WrapSkip(errors.New(message), skip))
}

func (q *queue) checkInvariants() {
	Expect(q.fakeHead.prev).To(BeNil())
	Expect(q.fakeTail.next).To(BeNil())
	Expect(q.fakeHead.owner).To(BeNil())
	Expect(q.fakeTail.owner).To(BeNil())
	var actualSize int64
	for n := q.head(); !q.end(n); n = n.next {
		actualSize += n.size()
		Expect(n.prev.next).To(BeIdenticalTo(n))
		Expect(n.owner).To(BeIdenticalTo(q))
	}
	Expect(q.tail().next).To(BeIdenticalTo(q.fakeTail))
	Expect(actualSize).To(BeIdenticalTo(q.size))
}

func (c *lru) checkInvariants() {
	var items int
	for _, l := range c.queues {
		l.checkInvariants()
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
