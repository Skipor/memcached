package recycle

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRecycle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Recycle Suite")
}
