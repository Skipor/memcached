package aof

import (
	"io"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAof(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Aof Suite")
}

func resetTestHooks() {
	afterFileSnapshotTestHook = func() {}
	afterExtraWriteTestHook = func() {}
	afterFinishTestHook = func() {}
}

var panicRotator = RotatorFunc(func(r ROFile, w io.Writer) error {
	panic("unexpected rotate call")
})
