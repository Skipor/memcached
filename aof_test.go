package memcached

import (
	"bytes"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/Skipor/memcached/aof"
	"github.com/Skipor/memcached/cache"
	"github.com/Skipor/memcached/log"
	"github.com/Skipor/memcached/recycle"
	. "github.com/Skipor/memcached/testutil"
)

var _ = Describe("cache aof", func() {
	const (
		xxxData = "12345"
		setXXX  = "set xxx 100 100 5" + Separator + xxxData + Separator
		delYYY  = "delete yyy noreply" + Separator
		getXXX  = "get xxx" + Separator
	)
	var (
		data      *bytes.Buffer
		l         log.Logger
		r         reader
		p         *recycle.Pool
		itYYY     cache.Item
		xxxMeta   cache.ItemMeta
		cr        *countingReader
		cacheConf cache.Config
	)
	BeforeEach(func() {
		data = &bytes.Buffer{}
		p = recycle.NewPool()
		cr = newCountingReader(data, p)
		r = cr.reader
		l = log.NewLogger(log.DebugLevel, GinkgoWriter)
		cacheConf = cache.Config{Size: 1 << 20}
		itYYY = cache.Item{}
		itYYY.Key = "yyy"
		itYYY.Bytes = Rand.Intn(500)
		itYYY.Data, _ = p.ReadData(Rand, itYYY.Bytes)
		xxxMeta = cache.ItemMeta{"xxx", 100, 100, 5}
	})

	It("read no snapshot", func() {
		data.WriteString(delYYY)
		c, err := readSnapshotIfAny(r, l, cacheConf)
		Expect(err).To(BeNil())
		Expect(c).NotTo(BeNil())
		Expect(ioutil.ReadAll(r)).To(BeEquivalentTo(delYYY))
	})

	It("snapshot write and read", func() {
		actualCache := cache.NewLockingLRU(l, cacheConf)
		actualCache.Set(itYYY)
		writeCacheSnapshot(actualCache, data)
		data.WriteString(delYYY)

		c, err := readSnapshotIfAny(r, l, cacheConf)
		Expect(err).To(BeNil())
		Expect(c).NotTo(BeNil())
		Expect(ioutil.ReadAll(r)).To(BeEquivalentTo(delYYY))

		gotIts := c.Get([]byte(itYYY.Key))
		Expect(gotIts).To(HaveLen(1))
		gotIt := gotIts[0]
		Expect(gotIt.ItemMeta).To(Equal(itYYY.ItemMeta))
		actualData, _ := ioutil.ReadAll(itYYY.Data.NewReader())
		Expect(ioutil.ReadAll(gotIt.Reader)).To(Equal(actualData))
	})

	It("read correct command log", func() {
		c := cache.NewLockingLRU(l, cacheConf)
		c.Set(itYYY)
		data.WriteString(delYYY)
		data.WriteString(getXXX)
		data.WriteString(setXXX)
		dataLen := data.Len()
		lastValidPos, err := readCommandLog(cr, c)
		Expect(err).To(BeNil())
		Expect(lastValidPos).To(BeEquivalentTo(dataLen))

		Expect(c.Get([]byte(itYYY.Key))).To(BeEmpty())
		xxxIts := c.Get([]byte(xxxMeta.Key))
		Expect(xxxIts).To(HaveLen(1))
		gotIt := xxxIts[0]
		gotIt.Exptime = xxxMeta.Exptime // Exptime will be different.
		Expect(gotIt.ItemMeta).To(Equal(xxxMeta))
		Expect(ioutil.ReadAll(gotIt.Reader)).To(BeEquivalentTo(xxxData))
	})

	It("read incorrect command log", func() {
		c := cache.NewLockingLRU(l, cacheConf)
		c.Set(itYYY)
		data.WriteString(delYYY)
		data.WriteString(getXXX)
		expectedLastValidPos := data.Len()
		data.WriteString(setXXX[:len(setXXX)-3])

		lastValidPos, err := readCommandLog(cr, c)
		Expect(err).NotTo(BeNil())
		Expect(lastValidPos).To(BeEquivalentTo(expectedLastValidPos))

		Expect(c.Get([]byte(itYYY.Key))).To(BeEmpty())
		Expect(c.Get([]byte(xxxMeta.Key))).To(BeEmpty())
	})

	It("read incorrect command log", func() {
		c := cache.NewLockingLRU(l, cacheConf)
		c.Set(itYYY)
		data.WriteString(delYYY)
		data.WriteString(getXXX)
		expectedLastValidPos := data.Len()
		data.WriteString(setXXX[:len(setXXX)-3])

		lastValidPos, err := readCommandLog(cr, c)
		Expect(err).NotTo(BeNil())
		Expect(lastValidPos).To(BeEquivalentTo(expectedLastValidPos))

		Expect(c.Get([]byte(itYYY.Key))).To(BeEmpty())
		Expect(c.Get([]byte(xxxMeta.Key))).To(BeEmpty())
	})

	Context("readAOF", func() {
		var (
			filename      string
			err           error
			c             *cache.LockingLRU
			memcachedConf Config
		)
		BeforeEach(func() {
			filename = TmpFileName()
			memcachedConf = Config{
				Cache: cache.Config{
					Size: 1 << 10,
				},
				AOF: aof.Config{
					Name:       filename,
					RotateSize: 1 << 10,
				},
			}

		})
		AfterEach(func() { os.Remove(filename) })
		DoReadAOF := func() {
			c, err = readAOF(p, l, memcachedConf)
			if err != nil {
				Byf("%v", err)
			}
		}
		It("no aof file", func() {
			DoReadAOF()
			Expect(err).To(BeNil())
		})
		Context("invalid file", func() {
			var expectedTruncated []byte
			BeforeEach(func() {
				actualCache := cache.NewLockingLRU(l, cacheConf)
				actualCache.Set(itYYY)
				writeCacheSnapshot(actualCache, data)
				data.WriteString(delYYY)
				data.WriteString(getXXX)
				expectedTruncated = append([]byte(nil), data.Bytes()...)

				data.WriteString(setXXX[:len(setXXX)-3])
				err := ioutil.WriteFile(filename, data.Bytes(), 0600)
				Expect(err).To(BeNil())
			})

			It("no fix corruption", func() {
				DoReadAOF()
				_, ok := err.(*CorruptedError)
				Expect(ok).To(BeTrue())
			})
			It("fix corruption", func() {
				memcachedConf.FixCorruptedAOF = true
				DoReadAOF()
				Expect(ioutil.ReadFile(filename)).To(Equal(expectedTruncated))
			})
		})
	})

})
