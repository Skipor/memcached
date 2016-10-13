package cache

import (
	"runtime"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

func testLimits(n int64) limits {
	return limits{
		total: 3 * n * testNodeSize,
		hot:   n * testNodeSize,
		warm:  n * testNodeSize,
	}
}

var _ = Describe("Cache", func() {
	var (
		p            testPool
		c            *cache
		hotWarmLimit int64
		leak         chan *recycle.Data
	)
	CheckLeaks := func() { leak = make(chan *recycle.Data) }
	BESetHotWarmLimit := func(n int64) { BeforeEach(func() { hotWarmLimit = n }) }
	BeforeEach(func() {
		leak = nil
		resetTestKeys()
		hotWarmLimit = 0
		p = newTestPool()
	})
	AfterEach(func() {
		if leak != nil {
			runtime.GC()
			Consistently(leak).ShouldNot(Receive())
		}
	})
	JustBeforeEach(func() {
		c = newCache(log.NewLogger(log.DebugLevel, GinkgoWriter), Config{})
		c.limits = testLimits(hotWarmLimit)
		if leak != nil {
			p.SetLeakCallback(recycle.NotifyOnLeak(leak))
		}
	})
	Context("limits check", func() {
		BESetHotWarmLimit(1)
		It("total overflow", func() {
			c.hot().push(p.testNode())
			c.warm().push(p.testNode())
			c.cold().push(p.testNode())
			Expect(c.totalOverflow()).To(BeFalse())
			Expect(c.free()).To(BeEquivalentTo(0))

			c.cold().push(p.testNode())
			Expect(c.totalOverflow()).To(BeTrue())
			Expect(c.free()).To(BeEquivalentTo(-testNodeSize))
		})

		It("hot overflow", func() {
			c.hot().push(p.testNode())
			Expect(c.hotOverflow()).To(BeFalse())
			c.hot().push(p.testNode())
			Expect(c.hotOverflow()).To(BeTrue())
		})

		It("warm overflow", func() {
			c.warm().push(p.testNode())
			Expect(c.warmOverflow()).To(BeFalse())
			c.warm().push(p.testNode())
			Expect(c.warmOverflow()).To(BeTrue())
		})
	})

	const k = 10
	var it []Item
	Key := func(i int) []byte { return []byte(it[i].Key) }
	Node := func(i int) *node { return c.table[it[i].Key] }
	Touch := func(i int) {
		views := c.Get([]byte(it[i].Key))
		views[0].Reader.Close()
	}
	BeforeEach(func() {
		it = nil
		for i := 0; i < k; i++ {
			it = append(it, p.testItem())
		}
	})
	ExpectContainsItem := func(i Item) {
		views := c.Get([]byte(i.Key))
		Expect(views).To(HaveLen(1))
		ExpectViewOfItem(views[0], i)
		Expect(views).To(HaveLen(1))
	}

	Context("behaviour", func() {
		AfterEach(func() { c.ExpectInvariantsOk() })
		It("init", func() {})
		Context("set", func() {
			Test := func(msg string, hwl int64, k int) {
				Context(msg, func() {
					BESetHotWarmLimit(hwl)
					It("", func() {
						for i := 0; i < k; i++ {
							c.Set(it[i])
							ExpectContainsItem(it[i])
						}
					})
				})
			}
			Test("one", 1, 1)
			Test("some", 3, 6)
			Test("overflow", 5, 1)

			Context("override", func() {
				BeforeEach(func() {
					hotWarmLimit = 1
					CheckLeaks()
				})
				It("", func() {
					c.Set(it[0])
					it[1].Key = it[0].Key
					it[1].Bytes -= 1 // Check that different size does not break invariant.
					c.Set(it[1])
					Expect(c.hot().items()).To(ConsistOf(it[1]))
					ExpectContainsItem(it[1])
				})
			})
		})

		Context("delete", func() {
			BESetHotWarmLimit(1)
			// TODO get test
			It("not found", func() {
				c.Set(it[0])
				deleted := c.Delete(Key(1))
				Expect(c.itemsNum()).To(Equal(1))
				Expect(deleted).To(BeFalse())
				ExpectContainsItem(it[0])
			})

			BeforeEach(CheckLeaks)
			It("found", func() {
				c.Set(it[0])
				deleted := c.Delete(Key(0))
				Expect(c.itemsNum()).To(BeZero())
				Expect(deleted).To(BeTrue())
				Expect(c.Get(Key(0))).To(BeEmpty())
			})
		})
	})

	Context("item flow", func() {
		BESetHotWarmLimit(1)
		AfterEach(func() { c.ExpectInvariantsOk() })

		It("active after active overwrite", func() {
			c.Set(it[0])
			Touch(0)
			c.Set(it[0])
			Expect(Node(0).isActive()).To(BeTrue())
		})
		It("inactive after inactive overwrite", func() {
			c.Set(it[0])
			c.Set(it[0])
			Expect(Node(0).isActive()).To(BeFalse())
		})
		It("active after get", func() {
			c.Set(it[0])
			Touch(0)
			Expect(Node(0).isActive()).To(BeTrue())
		})

		BeforeEach(CheckLeaks)
		It("items flow", func() {
			c.Set(it[0])
			Touch(0)
			Expect(c.hot().items()).To(ConsistOf(it[0]))
			// h: {it0'}, w:{}, c{}

			c.Set(it[1])
			// it1 evict hot it0'
			// it0 evict hot it1
			// h: {it0}, w:{}, c{it1}
			c.ExpectInvariantsOk()
			By("avtive hot evict added inactive")
			Expect(c.hot().items()).To(ConsistOf(it[0]))
			Expect(Node(0).isActive()).To(BeFalse())
			Expect(c.cold().items()).To(ConsistOf(it[1]),
				"on hot overflow inactive hot flow to cold")

			Touch(1)
			// h: {it0}, w:{}, c{it1'}
			c.Set(it[2])
			// it2 evict hot it0
			// h: {it2}, w:{}, c{it1', it0}
			c.ExpectInvariantsOk()
			Expect(c.hot().items()).To(ConsistOf(it[2]))
			Expect(c.warm().items()).To(BeEmpty())
			Expect(c.cold().items()).To(Equal([]Item{it[1], it[0]}))
			Expect(Node(1).isActive()).To(BeTrue())

			By("cold evict active cold to warm")
			c.Set(it[3])
			// it3 evict hot it2
			// it2 evict cold it1' to warm
			// it2 evict cold it0
			// h: {it3}, w:{it1}, c{it2}
			c.ExpectInvariantsOk()
			Expect(c.hot().items()).To(ConsistOf(it[3]))
			Expect(c.warm().items()).To(ConsistOf(it[1]))
			Expect(c.cold().items()).To(ConsistOf(it[2]))

			By("active warm stay as inactive, inactive flow to cold")
			Touch(1)
			Touch(2)
			// h{it3}, w:{it1'}, c{it2'}
			c.Set(it[4])
			// it4 evict hot it3
			// it3 evict cold it2'
			// it2 evict warm it1'
			// it1 evict warm it2
			// it2 evict cold it3
			// h:{it4}, w:{it1}, c:{it2}
			c.ExpectInvariantsOk()
			Expect(c.hot().items()).To(ConsistOf(it[4]))
			Expect(c.warm().items()).To(ConsistOf(it[1]))
			Expect(c.cold().items()).To(ConsistOf(it[2]))
			for _, n := range []*node{Node(4), Node(1), Node(2)} {
				Expect(n.isActive()).To(BeFalse(), n.Key)
			}

			By("expired evicted by inactive")
			Node(4).Exptime = nowUnix() - 1
			// h:{it4*}, w:{it1}, c:{it2}
			c.Set(it[5])
			// it5 evict expired hot it4
			// h:{it5}, w:{it1}, c:{it2}
			Expect(c.hot().items()).To(ConsistOf(it[5]))
			Expect(c.warm().items()).To(ConsistOf(it[1]))
			Expect(c.cold().items()).To(ConsistOf(it[2]))
		})
	})

	Context("total owerflow with empty warm and active cold", func() {
		const limit = 6
		BeforeEach(func() {
			hotWarmLimit = limit / 3
		})

		It("overflow fixed", func() {
			for i := 0; i < limit; i++ {
				c.Set(it[i])
			}
			Expect(c.itemsNum()).To(Equal(limit))

			sep := int(hotWarmLimit * 2)
			Expect(c.hot().items()).To(ConsistOf(it[sep:limit]))
			Expect(c.cold().items()).To(ConsistOf(it[:sep]))
			// Set cold items active.
			for i := 0; i < sep; i++ {
				Touch(i)
			}
			c.Set(it[limit])
			Expect(c.itemsNum()).To(Equal(limit))
			Expect(c.hot().items()).To(ConsistOf(it[sep+1 : limit+1]))
			Expect(c.warm().items()).To(ConsistOf(it[hotWarmLimit:sep]))
			Expect(c.cold().items()).To(ConsistOf(it[:hotWarmLimit]))
		})
	})

})
