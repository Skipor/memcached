package memcached

import (
	"bytes"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"

	"github.com/skipor/memcached/aof"
	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/cache/cachemocks"
	"github.com/skipor/memcached/recycle"
	"github.com/skipor/memcached/testutil"
)

var _ = Describe("LoggingTestView", func() {
	var (
		filename string
		v        *loggingCacheView
		AOF      *aof.AOF
		mcache   *cachemocks.Cache
		err      error

		deleteRaw []byte
		getRaw    []byte
		setRaw    []byte
		setData   []byte
	)
	BeforeEach(func() {
		deleteRaw = []byte("delete xxx noreply\r\n")
		getRaw = []byte("get yyy xxx\r\n")
		setRaw = []byte("get key 0 0 1\r\n")
		setData = []byte{'d'}
		filename = testutil.TmpFileName()
		mcache = &cachemocks.Cache{}
		AOF, err = aof.Open(nil, aof.RotatorFunc(nil), aof.Config{
			Name:       filename,
			RotateSize: 1 << 20,
		})
		Expect(err).To(BeNil(), "%s", err)
		v = newLoggingCacheView(mcache, AOF)

	})
	AfterEach(func() {
		os.Remove(filename)
		mcache.AssertExpectations(GinkgoT())
	})

	ExpectRLock := func() {
		mcache.On("RLock").Run(func(mock.Arguments) {
			mcache.On("RUnlock")
		}).Once()
	}
	ExpectLock := func() {
		mcache.On("Lock").Run(func(mock.Arguments) {
			mcache.On("Unlock")
		}).Once()
	}
	ExpectFileEqual := func(b []byte) {
		fileData, err := ioutil.ReadFile(filename)
		Expect(err).To(BeNil())
		testutil.ExpectBytesEqual(fileData, b)
	}

	It("delete", func() {
		key, _, err := parseDeleteFields(bytes.Fields(deleteRaw)[1:])
		Expect(err).To(BeNil())
		mcache.On("Delete", key).Return(true)
		ExpectLock()
		deleted := v.NewDeleter(deleteRaw).Delete(key)
		Expect(deleted).To(BeTrue())
		ExpectFileEqual(deleteRaw)
	})

	It("get", func() {
		keys, err := parseGetFields(bytes.Fields(getRaw)[1:])
		Expect(err).To(BeNil())
		expected := make([]cache.ItemView, 4)
		mcache.On("Get", keys).Return(expected)
		ExpectRLock()
		actual := v.NewGetter(getRaw).Get(keys...)
		Expect(actual).To(Equal(expected))
		ExpectFileEqual(getRaw)
	})

	It("set", func() {
		meta, _, err := parseSetFields(bytes.Fields(setRaw)[1:])
		Expect(err).To(BeNil())
		data, _ := recycle.NewPool().ReadData(bytes.NewReader(setData), len(setData))
		it := cache.Item{
			ItemMeta: meta,
			Data:     data,
		}
		expectedData := bytes.Join([][]byte{setRaw, setData, separatorBytes}, nil)
		mcache.On("Set", it)
		ExpectLock()
		setter := v.NewSetter(setRaw)
		setRaw[1] = 0 // Model raw invalidation
		setter.Set(it)
		ExpectFileEqual(expectedData)
	})

})
