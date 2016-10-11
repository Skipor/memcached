package cache

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"time"
)

const testNodeSize = 2 * extraSizePerNode

var _ = Describe("LRU", func() {
	var (
		l lru
	)
	BeforeEach(func() {
		resetKeys()
		l = lru{}
		l.init()
	})
	AfterEach(func() {
		l.ExpectInvariantsOk()
	})
	It("init", func() {})

	It("push", func() {
		l.pushBack(testNode())
	})

	It("push multi", func() {
		l.pushBack(testNode())
		l.pushBack(testNode())
	})

	Context("shrink", func() {
		var (
			mc *MockCallback
		)
		BeforeEach(func() {
			mc = &MockCallback{}
			l.onExpire = mc.Expire
		})
		AfterEach(func() { mc.AssertExpectations(GinkgoT()) })

		Context("move to tail", func() {
			var an2 *node
			BeforeEach(func() {
				l.onInactive = mc.Evict
				l.onActive = mc.AttachAsInactive

				en := expiredNode()
				ian := testNode()
				an1 := testNode()
				an2 = testNode()
				mc.On("Expire", en).Once()
				mc.On("Evict", ian).Once()
				mc.On("AttachAsInactive", an1).Once()
				mc.On("AttachAsInactive", an2).Once()
				mc.On("Evict", an1).Once()
				for _, n := range []*node{en, ian, an1, an2} {
					l.pushBack(n)
				}
				an1.setActive()
				an2.setActive()

				l.ExpectInvariantsOk()
			})
			It("to some", func() {
				l.shrink(1*testNodeSize, time.Now().Unix())
				Expect(l.nodes()).To(ConsistOf(an2))
			})
			It("to zero", func() {
				mc.On("Evict", an2).Once()
				l.shrink(0*testNodeSize, time.Now().Unix())
				Expect(l.nodes()).To(BeEmpty())
			})
		})

		It("move to other", func() {
			otherLRU := lru{}
			otherLRU.init()
			l.onInactive = mc.MoveTo(&otherLRU)
			l.onActive = mc.AttachAsInactive

			en := expiredNode()
			ian := testNode()
			an1 := testNode()
			an2 := testNode()
			mc.On("Expire", en).Once()
			mc.On("Moved", ian).Once()
			mc.On("AttachAsInactive", an1).Once()
			mc.On("AttachAsInactive", an2).Once()
			mc.On("Moved", an1).Once()
			for _, n := range []*node{en, ian, an1, an2} {
				l.pushBack(n)
			}
			an1.setActive()
			an2.setActive()
			l.ExpectInvariantsOk()
			l.shrink(1*testNodeSize, time.Now().Unix())
			otherLRU.ExpectInvariantsOk()
			Expect(l.nodes()).To(ConsistOf(an2))
			Expect(otherLRU.nodes()).To(ConsistOf(ian, an2))
		})

	})

})
