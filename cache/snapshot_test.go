package cache

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/Skipor/memcached/internal/util"
	"github.com/Skipor/memcached/log"
	"github.com/Skipor/memcached/recycle"
	. "github.com/Skipor/memcached/testutil"
)

var _ = Describe("Snapshot", func() {
	var (
		l            log.Logger
		p            testPool
		expected     *lru
		actual       *lru
		expectedConf Config
		actualConf   Config
		snapshot     *bytes.Buffer
		err          error
	)
	const actualSize = 64 * (1 << 10)
	BeforeEach(func() {
		resetTestKeys()
		l = log.NewLogger(log.DebugLevel, GinkgoWriter)
		p = testPool{recycle.NewPool()}
		expectedConf.Size = actualSize
		actualConf = expectedConf // Test can override actual conf.
		expected = newLRU(l, expectedConf)
		snapshot = &bytes.Buffer{}
	})

	JustBeforeEach(func() {
		s := expected.snapshot()
		var n int64
		n, err = s.WriteTo(snapshot)
		Expect(err).To(BeNil())
		Expect(n).ToNot(BeZero())
		Byf("Snapshot size: %v", n)
	})
	DoRead := func() {
		actual, err = readSnapshot(snapshot, p.Pool, l, actualConf)
	}

	AssertEquvalent := func() {
		It("actual equalent expected", func() {
			DoRead()
			Expect(err).To(BeNil())
			ExpectLRUsToBeEquvalent(actual, expected)
		})
	}

	Context("empty", func() {
		AssertEquvalent()
	})
	Context("with inactive node", func() {
		BeforeEach(func() {
			it := p.randSizeItem()
			expected.set(it)
		})
		AssertEquvalent()
	})
	Context("with active node", func() {
		BeforeEach(func() {
			it := p.randSizeItem()
			expected.set(it)
			expected.touch([]byte(it.Key))
		})
		AssertEquvalent()
	})

	Context("active and inactive", func() {
		BeforeEach(func() {
			it := p.randSizeItem()
			expected.set(it)
			expected.touch([]byte(it.Key))
			expected.set(p.randSizeItem())
		})
		AssertEquvalent()
	})

	Context("with one queue", func() {
		BeforeEach(func() {
			for i := 0; i < Rand.Intn(10)+3; i++ {
				it := p.randSizeItem()
				expected.set(it)
				if Rand.Intn(2) == 0 {
					expected.touch([]byte(it.Key))
				}
			}
		})
		AssertEquvalent()
	})

	Context("with empty item", func() {
		BeforeEach(func() {
			for i := 0; i < 2; i++ {
				for i := 0; i < Rand.Intn(3); i++ {
					expected.set(p.randSizeItem())
				}
				expected.set(p.sizeItem(0))
			}
		})
		AssertEquvalent()
	})

	Context("with many queues", func() {
		BeforeEach(func() {
			for i := 0; expected.size() < expected.limits.total-testNodeSize; i++ {
				it := p.randSizeItem()
				expected.set(it)
			}
			for _, n := range expected.table {
				if Rand.Intn(2) == 0 {
					n.active = active
				}
			}
			for i := 0; i < expected.itemsNum() && expected.warm().size < 3*testNodeSize; i++ {
				it := p.randSizeItem()
				expected.set(it)
			}
		})
		AssertEquvalent()
	})

	Context("with expired item", func() {
		var expiredKey string
		BeforeEach(func() {
			for i := 0; i < Rand.Intn(5); i++ {
				expected.set(p.randSizeItem())
			}

			it := p.sizeItem(Rand.Intn(8 << 10))
			expected.set(it)
			expiredKey = it.Key
			expected.table[expiredKey].Exptime = nowUnix() - 3

			for i := 0; i < Rand.Intn(5); i++ {
				expected.set(p.randSizeItem())
			}
		})
		It("actual equalent expected withoud expired node", func() {
			DoRead()
			Expect(err).To(BeNil())
			Expect(actual.itemsNum()).To(Equal(expected.itemsNum() - 1))
			expected.delete([]byte(expiredKey))
			ExpectLRUsToBeEquvalent(actual, expected)
		})
	})

	Context("with extra data after", func() {
		var data []byte
		BeforeEach(func() {
			for i := 0; i < Rand.Intn(10)+3; i++ {
				expected.set(p.randSizeItem())
			}
			Fuzz(&data)
		})
		AssertEquvalent()
		It("extra data not corrupted", func() {
			snapshot.Write(data)
			DoRead()
			ExpectBytesEqual(snapshot.Bytes(), data)
		})

	})

	Context("overflow after read", func() {
		BeforeEach(func() {
			actualConf = Config{
				Size: testNodeSize + Rand.Int63n(10*testNodeSize),
			}
			for i := 0; expected.size() < actualConf.Size+10*testNodeSize; i++ {
				expected.set(p.randSizeItem())
			}
		})
		It("ErrCacheOverflow", func() {
			DoRead()
			Expect(IsCacheOverflow(err)).To(BeTrue())
			Expect(util.Unwrap(err)).To(BeIdenticalTo(errCacheOverflow))
		})
		It("equalent to fixed overflows", func() {
			DoRead()
			expected.limits = actual.limits
			expected.fixOverflows()
			ExpectLRUsToBeEquvalent(actual, expected)
		})
	})

})
