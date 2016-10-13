package testutil

import (
	"fmt"

	"github.com/onsi/ginkgo"
)

func Byf(format string, args ...interface{}) {
	ginkgo.By(fmt.Sprintf(format, args...))
	fmt.Fprintln(ginkgo.GinkgoWriter)
}
