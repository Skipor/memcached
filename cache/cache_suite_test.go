package cache

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestCache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cache Suite")
}

func (l *lru) checkInvariants(n *node) {
	//TODO
}

func (c *cache) checkInvariants(n *node) {
	//TODO
}
