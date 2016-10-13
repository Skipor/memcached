package integration

import (
	"fmt"
	"math"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rcrowley/go-metrics"

	"github.com/skipor/memcached/testutil"
)

func IsTemporary(err error) bool {
	if ne, ok := err.(net.Error); ok {
		return ne.Temporary()
	}
	return false
}

func IsTimeout(err error) bool {
	if _, ok := err.(*memcache.ConnectTimeoutError); ok {
		return true
	}
	if ne, ok := err.(net.Error); ok {
		return ne.Timeout()
	}
	return false
}

func LoadTest(addr string) {
	prevMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer runtime.GOMAXPROCS(prevMaxProcs)

	const (
		checkGotItems     = false
		aproxCacheHit     = 0.92
		cacheSize     int = 128 * (1 << 20)
		itemsNum          = 16 * (1 << 10)
		meanItemSize      = cacheSize * (aproxCacheHit * 100) / 100 / itemsNum
		//meanExptime   = 20
		stddev = itemsNum / 3
		setP   = 0.01
		delP   = 0.01

		clientsNum    = 10
		totalRequests = 16 * itemsNum
	)

	ResetTestKeys()
	start := &sync.WaitGroup{}
	start.Add(clientsNum)
	finish := &sync.WaitGroup{}
	finish.Add(clientsNum)
	items := make([]*memcache.Item, itemsNum)

	{
		By("Warmup cache.")
		c := memcache.New(addr)
		// Prepare items, warmup cache.
		for i := 0; i < itemsNum; i++ {
			it := NewItem(testutil.Rand.Intn(2 * meanItemSize))
			items[i] = it
			err := c.Set(it)
			if err != nil {
				for IsTemporary(err) {
					testutil.Byf("Conn %v temporary err: %v", i, err)
					time.Sleep(100 * time.Millisecond)
					err = c.Set(it)
				}
				Expect(err).To(BeNil())
			}
		}
		By("Warmup done.")
	}

	var requests int32
	Next := func() bool { return atomic.AddInt32(&requests, 1) < totalRequests }
	ItemIndex := func(r *rand.Rand) (index int) {
		index = itemsNum
		for index >= itemsNum {
			index = int(math.Abs(r.NormFloat64() * stddev))
		}
		return
	}

	registry := metrics.NewRegistry()
	getTimer := metrics.NewRegisteredTimer("get", registry)
	setTimer := metrics.NewRegisteredTimer("set", registry)
	delTimer := metrics.NewRegisteredTimer("del", registry)
	missCounter := metrics.NewRegisteredCounter("cache.miss", registry)
	timeoutCounter := metrics.NewRegisteredCounter("err.timeout", registry)
	temporaryCounter := metrics.NewRegisteredCounter("err.temporary", registry)

	// From 1, so conn number in server log equals client i.
	for i := 1; i <= clientsNum; i++ {
		client := i
		source := rand.NewSource(testutil.Rand.Int63())
		Rand := rand.New(source)
		c := memcache.New(addr)
		// Establish and check conn.
		_, err := c.Get("no_such_key")
		Expect(err).To(Equal(memcache.ErrCacheMiss))
		go func() {
			defer GinkgoRecover()
			start.Done()
			start.Wait()
			defer func() {
				testutil.Byf("Client %v done.", client)
				finish.Done()
			}()
			var err error
			var gotItem *memcache.Item
			for Next() {
				it := items[ItemIndex(Rand)]
				p := Rand.Float64()
				switch {
				case p <= setP:
					setTimer.Time(func() { err = c.Set(it) })
				case p <= setP+delP:
					delTimer.Time(func() { err = c.Delete(it.Key) })
				default:
					getTimer.Time(func() {
						gotItem, err = c.Get(it.Key)
					})
					if checkGotItems {
						if err == nil {
							ExpectItemsEqual(gotItem, it)
						}
					}
				}
				if err != nil {
					if err == memcache.ErrCacheMiss {
						missCounter.Inc(1)
						continue
					}
					if IsTimeout(err) {
						testutil.Byf("Client %v timeout error: %v", client, err)
						err = nil
						timeoutCounter.Inc(1)
						continue
					}
					if IsTemporary(err) {
						testutil.Byf("Client %v temporary error: %v", client, err)
						err = nil
						temporaryCounter.Inc(1)
						continue
					}
					testutil.Byf("Client %v error: %v", client, err)
					Expect(err).To(BeNil())
				}
			}
		}()
	}

	logging := &sync.WaitGroup{}
	logging.Add(1)
	go func() {
		By("logging start")
		defer GinkgoRecover()
		tick := time.NewTicker(time.Second / 2)
		defer func() {
			tick.Stop()
			logging.Done()
		}()
		for ; ; _ = <-tick.C {
			req := atomic.LoadInt32(&requests)
			if req < totalRequests {
				fmt.Fprintf(GinkgoWriter, "%v%% requests done.\n", req*100/totalRequests)
				continue
			}
			break
		}
		metrics.WriteOnce(registry, GinkgoWriter)
		fmt.Fprintf(GinkgoWriter, "%v%% cache miss.\n",
			missCounter.Count()*100/(getTimer.Count()+delTimer.Count()))
		fmt.Fprintf(GinkgoWriter, "%v%% deletes.\n",
			delTimer.Count()*100/totalRequests)
		fmt.Fprintf(GinkgoWriter, "%v%% set.\n",
			setTimer.Count()*100/totalRequests)
	}()
	finish.Wait()
	By("finish done")
	logging.Wait()
	By("logging done")
}
