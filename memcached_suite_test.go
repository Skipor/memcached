package memcached

import (
	"math/rand"
	"testing"

	"io"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var Rand *rand.Rand

func TestMemcached(t *testing.T) {
	randSorce := rand.NewSource(GinkgoRandomSeed())
	Rand = rand.New(randSorce)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Memcached Suite")
}

func ChunkWithoutSeparators(size int) []byte {
	ch, _ := ioutil.ReadAll(io.LimitReader(Rand, int64(size)))
	for i, b := range ch {
		for _, sb := range []byte(Separator) {
			if b == sb {
				ch[i] = 'x'
			}
		}
	}
	return ch
}

type fastRandReader struct{}

var fastRand = fastRandReader{}

func (fastRandReader) Read(p []byte) (int, error) {
	if len(p) > 0 {
		p[0] = byte(Rand.Int())
	}
	return len(p), nil
}
