package memcached

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"runtime"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
	"github.com/stretchr/testify/mock"

	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/cache/cachemocks"
	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
	. "github.com/skipor/memcached/testutil"
)

const ReadTimeout = 0.2

type Out struct {
	buf *Buffer
}

func NewOut() *Out {
	return &Out{NewBuffer()}
}

var _ BufferProvider = (*Out)(nil)

func (o *Out) Buffer() *Buffer {
	return o.buf
}
func ReadAll(i *cache.Item) []byte {
	ir := i.Data.NewReader()
	defer ir.Close()
	data, _ := ioutil.ReadAll(ir)
	return data
}

func (o *Out) ExpectItem(i *cache.Item) {
	Eventually(o).Should(Say(ValueResponse + " "))
	o.expectChunk([]byte(i.Key))
	Eventually(o).Should(Say(fmt.Sprintf(" %v %v"+SeparatorPattern, i.Flags, i.Bytes)))
	expectedData := ReadAll(i)
	actualData, err := ioutil.ReadAll(io.LimitReader(o.buf, int64(i.Bytes)))
	Expect(err).To(BeNil())
	ExpectBytesEqual(actualData, expectedData)
	Expect(o).To(Say(SeparatorPattern))
}

func (o *Out) expectChunk(ch []byte) {
	actualCh := make([]byte, len(ch))
	_, err := io.ReadFull(o.buf, actualCh)
	Expect(err).To(BeNil())
	ExpectBytesEqual(actualCh, ch)
}

var _ = Describe("Conn", func() {
	var (
		connMeta      *ConnMeta
		mcache        *cachemocks.Cache
		c             *conn
		out           *Out
		in            *io.PipeWriter
		serveFinished chan struct{}
	)
	BeforeEach(func() {
		serveFinished = make(chan struct{})
		out = NewOut()
		mcache = &cachemocks.Cache{}
		var connReader *io.PipeReader
		connReader, in = io.Pipe()
		connMeta = &ConnMeta{
			Cache: mcache,
		}
		connMeta.init()
		rwc := struct {
			io.ReadCloser
			io.Writer
		}{connReader, out.buf}
		l := log.NewLogger(log.DebugLevel, GinkgoWriter)
		c = newConn(l, connMeta, rwc)
		go func() {
			defer GinkgoRecover()
			c.serve()
			close(serveFinished)
		}()
	})

	AfterEach(func() {
		in.Close()
		Eventually(serveFinished).Should(BeClosed())
		Expect(out).NotTo(Say(Anything))
		mcache.AssertExpectations(GinkgoT())
	})

	AssertSay := func(pattern string) {
		It("expected response", func() {
			Eventually(out, ReadTimeout).Should(Say(pattern))
		})
	}

	// Test can use input string, or write to in directly.
	var input string
	JustBeforeEach(func() { io.WriteString(in, input) })
	AfterEach(func() { input = "" })
	Input := func(s string) {
		BeforeEach(func() { input = s })
	}

	Context("server error", func() {
		BeforeEach(func() {
			input = "get \r\n"
			in.CloseWithError(errors.New("test err"))
		})
		AssertSay(ServerErrorPattern)
	})

	Context("client error", func() {
		Input("get \r\n")
		AssertSay(ClientErrorPattern)
	})

	Context("delete", func() {
		var key string
		var noreply bool
		var deleted bool
		AfterEach(func() {
			noreply = false
			deleted = false
		})
		JustBeforeEach(func() {
			key = "test_key"
			mcache.On("Delete", []byte(key)).Return(deleted)
			input = "delete " + key
			if noreply {
				input += " noreply"
			}
			input += Separator
			io.WriteString(in, input)
		})

		Context("no reply", func() {
			BeforeEach(func() { noreply = true })
		})
		Context("not found", func() {
			AssertSay(NotFoundPattern)
		})
		Context("deleted", func() {
			BeforeEach(func() { deleted = true })
			AssertSay(DeletedPattern)
		})
	})

	Context("set", func() {
		var (
			meta    cache.ItemMeta
			data    []byte
			noreply bool
		)
		BeforeEach(func() {
			meta.Key = "test_key"
			meta.Exptime = Rand.Int63n(time.Now().Unix()) + MaxRelativeExptime
			meta.Flags = Rand.Uint32()
			meta.Bytes = Rand.Intn(connMeta.MaxItemSize)
		})
		AfterEach(func() { noreply = false })

		JustBeforeEach(func() {
			data = make([]byte, meta.Bytes)
			io.ReadFull(Rand, data)
			mcache.On("Set", mock.Anything).Run(func(args mock.Arguments) {
				i := args.Get(0).(cache.Item)
				Expect(i.ItemMeta).To(Equal(meta))
				ExpectBytesEqual(ReadAll(&i), data)
			})
			input = fmt.Sprintf("set %s %v %v %v",
				meta.Key, meta.Flags, meta.Exptime, meta.Bytes)
			if noreply {
				input += " noreply"
			}
			input += Separator
			input += string(data) + Separator
			io.WriteString(in, input)
		})

		Context("no reply", func() {
			BeforeEach(func() { noreply = true })
			It("say nothing", func() {})
		})
		Context("stored", func() {
			AssertSay(StoredPattern)
		})
		Context("too large item", func() {
			BeforeEach(func() { meta.Bytes = connMeta.MaxItemSize + 1 })
			JustBeforeEach(func() {
				// cache.Cache.Set should not be called.
				mcache.ExpectedCalls = nil
			})
			AssertSay(ClientErrorPattern)
		})
	})

	Context("get", func() {
		var (
			kn         int
			foundItems = []int{}
			items      []*cache.Item
			keys       [][]byte
			leak       chan *recycle.Data
		)

		BeforeEach(func() {
			leak = make(chan *recycle.Data)
			connMeta.Pool.SetLeakCallback(recycle.NotifyOnLeak(leak))
		})
		AfterEach(func() {
			kn = 0
			foundItems = nil
			for _, it := range items {
				it.Data.Recycle()
			}
			items = nil
			runtime.GC()
			Consistently(leak).ShouldNot(Receive())
		})
		AssertGotExpectedItems := func() {
			It("found expected items", func() {
				for i := range foundItems {
					By(fmt.Sprintf("Expecting value %v", i))
					out.ExpectItem(items[i])
					By(fmt.Sprintf("Got value %v", i))
				}
				Eventually(out, ReadTimeout).Should(Say(EndPattern))
			})
		}

		JustBeforeEach(func() {
			keys = make([][]byte, kn)
			for i := 0; i < kn; i++ {
				keys[i] = []byte(fmt.Sprintf("test_key_%v", i))
			}
			for _, i := range foundItems {
				meta := cache.ItemMeta{
					Key:     string(keys[i]),
					Exptime: Rand.Int63n(DefaultMaxItemSize),
					Flags:   Rand.Uint32(),
					Bytes:   Rand.Intn(connMeta.MaxItemSize),
				}
				data, _ := connMeta.Pool.ReadData(FastRand, meta.Bytes)
				items = append(items, &cache.Item{meta, data})
			}
			mcache.On("Get", mock.Anything).Return(func(actualKeys ...[]byte) (views []cache.ItemView) {
				for i, k := range keys {
					Expect(actualKeys[i]).To(Equal(k))
				}
				for _, it := range items {
					By(fmt.Sprintf("Found item %s.", it.Key))
					views = append(views, it.NewView())
				}
				return
			})
			input = "get"
			for _, k := range keys {
				input += " " + string(k)
			}
			input += Separator
			io.WriteString(in, input)
		})

		Context("no items founded", func() {
			BeforeEach(func() { kn = 5 })
			AssertGotExpectedItems()
		})
		Context("found some", func() {
			BeforeEach(func() {
				kn = 5
				foundItems = []int{0, 2, 4}
			})
			AssertGotExpectedItems()
		})
	})
})
