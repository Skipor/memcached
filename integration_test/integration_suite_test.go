package integration

import (
	"fmt"
	"io"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/Skipor/memcached"
	. "github.com/Skipor/memcached/testutil"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/onsi/gomega/gexec"
)

var MemcachedCLI string

var _ = BeforeSuite(func() {
	var err error
	var args []string
	if os.Getenv("MEMCACHED_RACE") != "" {
		args = append(args, "-race")
		println("Building with race detector.")
	}
	if os.Getenv("MEMCACHED_DEBUG") != "" {
		args = append(args, "-tags debug")
	}
	MemcachedCLI, err = gexec.Build("github.com/Skipor/memcached/cmd/memcached", args...)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})

func TestIntegrationTest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var TestKey, ResetTestKeys = func() (k func() string, rk func()) {
	var i int
	k = func() string {
		key := fmt.Sprintf("test_key_%v", i)
		i++
		return key
	}
	rk = func() { i = 0 }
	return
}()

func NewItem(size int) *memcache.Item {
	it := &memcache.Item{
		Key:        TestKey(),
		Expiration: 1000 + Rand.Int31n(memcached.MaxRelativeExptime-1000),
		Flags:      Rand.Uint32(),
	}
	it.Value = make([]byte, size)
	io.ReadFull(Rand, it.Value)
	return it
}

func RandSizeItem() *memcache.Item {
	return NewItem(Rand.Intn(1 << 10))
}

func ExpectItemsEqualWithOffset(off int, a, b *memcache.Item) {
	off++
	ExpectWithOffset(off, a.Key).To(Equal(b.Key))
	ExpectWithOffset(off, a.Flags).To(Equal(b.Flags))
	ExpectBytesEqualWithOffset(1, a.Value, b.Value)
}

func ExpectItemsEqual(a, b *memcache.Item) {
	ExpectItemsEqualWithOffset(1, a, b)
}
