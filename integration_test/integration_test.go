package integration

import (
	"net"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/skipor/memcached"
	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/log"
)

var _ = Describe("Integration", func() {
	const cacheSize = 64 * (1 << 20)
	// Should not use debug in race detector tests - debug logging add many "happens before" relations,
	// because of locking output.
	const logLevel = log.InfoLevel
	//const logLevel = log.DebugLevel
	var (
		addr = memcached.DefaultAddr
		l    log.Logger
		s    *memcached.Server
		stop chan struct{}
		c    *memcache.Client
		err  error
	)
	BeforeEach(func() {
		l = log.NewLogger(logLevel, GinkgoWriter)
		ResetTestKeys()
		stop = make(chan struct{})
		s = &memcached.Server{
			Addr: addr,
			Log:  l,
		}
		localCache := cache.NewLRU(l, cache.Config{Size: cacheSize})
		s.NewCacheView = func() cache.View { return localCache }
		go func() {
			err := s.ListenAndServe()
			Expect(err).To(BeIdenticalTo(memcached.ErrStoped))
			close(stop)
		}()
		ServerListening := func() bool {
			conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
			if err != nil {
				l.Warn("Server not listening: ", err)
				return false
			}
			conn.Close()
			return true
		}
		Eventually(ServerListening).Should(BeTrue())
		var err error
		c = memcache.New(addr)
		Expect(err).To(BeNil())
	})
	AfterEach(func() {
		s.Stop()
		Eventually(stop).Should(BeClosed())
	})
	It("get what set", func() {
		set := RandSizeItem()
		err = c.Set(set)
		Expect(err).To(BeNil())
		get, err := c.Get(set.Key)
		Expect(err).To(BeNil())
		ExpectItemsEqual(get, set)
	})

	It("overwrite", func() {
		set := RandSizeItem()
		overwrite := RandSizeItem()
		overwrite.Key = set.Key
		err = c.Set(set)
		Expect(err).To(BeNil())
		err = c.Set(overwrite)
		Expect(err).To(BeNil())

		get, err := c.Get(set.Key)
		Expect(err).To(BeNil())
		ExpectItemsEqual(get, overwrite)
	})

	It("delete", func() {
		set := RandSizeItem()
		err = c.Set(set)
		Expect(err).To(BeNil())

		err = c.Delete(set.Key)
		_, err = c.Get(set.Key)
		Expect(err).To(Equal(memcache.ErrCacheMiss))
	})

	It("multi get", func() {
		var keys []string
		items := map[string]*memcache.Item{}
		for i := 0; i < 10; i++ {
			i := RandSizeItem()
			keys = append(keys, i.Key)
			items[i.Key] = i
			err = c.Set(i)
			Expect(err).To(BeNil())
		}
		gotItems, err := c.GetMulti(keys)
		Expect(err).To(BeNil())
		Expect(len(gotItems)).To(Equal(len(items)))
		for k, v := range gotItems {
			ExpectItemsEqual(v, items[k])
		}
	})
	Context("load", func() {
		It("", func() {
			LoadTest(addr)
		})
	})
})
