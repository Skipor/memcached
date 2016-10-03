package recycle

import (
	"math/rand"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var Rand *rand.Rand

func TestRecycle(t *testing.T) {
	randSorce := rand.NewSource(GinkgoRandomSeed())
	Rand = rand.New(randSorce)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Recycle Suite")
}
