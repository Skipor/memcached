package aof

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func resetTestHooks() {
	afterFileSnapshotRotationTestHook = func() {}
	afterExtraWriteRotationTestHook = func() {}
	afterFinishTakeRotationTestHook = func() {}
}

func TestAof(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Aof Suite")
}
