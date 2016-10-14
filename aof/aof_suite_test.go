package aof

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAof(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Aof Suite")
}

func testFileName() string {
	f, err := ioutil.TempFile("", "aof_test_")
	Expect(err).To(BeNil())
	filename := f.Name()
	err = f.Close()
	Expect(err).To(BeNil())
	err = os.Remove(filename)
	Expect(err).To(BeNil())
	return filename
}

func resetTestHooks() {
	afterFileSnapshotRotationTestHook = func() {}
	afterExtraWriteRotationTestHook = func() {}
	afterFinishTakeRotationTestHook = func() {}
}

var panicRotator = RotatorFunc(func(r ROFile, w io.Writer) error {
	panic("unexpected rotate call")
})
