package testutil

import (
	"math/rand"

	"github.com/google/gofuzz"
	. "github.com/onsi/ginkgo"
)

var RandSource = rand.NewSource(GinkgoRandomSeed())
var Rand = rand.New(RandSource)
var Fuzzer = func() *fuzz.Fuzzer {
	f := fuzz.New()
	f.RandSource(RandSource)
	return f
}()
var Fuzz = Fuzzer.Fuzz

var FastRand = fastRandReader{}

type fastRandReader struct{}

func (fastRandReader) Read(p []byte) (int, error) {
	if len(p) > 0 {
		p[0] = byte(Rand.Int())
	}
	return len(p), nil
}
