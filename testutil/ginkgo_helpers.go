package testutil

import (
	"bytes"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const maxPrintableLen = 1024

func Byf(format string, args ...interface{}) {
	By(fmt.Sprintf(format, args...))
	fmt.Fprintln(GinkgoWriter)
}

// ExpectBytesEqual have much less overhead for large byte chunks but ginkgo.Equal.
func ExpectBytesEqual(a, b []byte) {
	ExpectBytesEqualWithOffset(1, a, b)
}

func ExpectBytesEqualWithOffset(off int, a, b []byte) {
	off++
	if !bytes.Equal(a, b) {
		if len(a)+len(b) <= 2*maxPrintableLen {
			ExpectWithOffset(off, a).To(Equal(b))
		}
		ExpectWithOffset(off, len(a)).To(Equal(len(b)), "Length are unequal and data is too large to print.")
		for i, ab := range a {
			if ab != b[i] {
				var cmpLen int = maxPrintableLen
				if leftChunk := a[i:]; len(leftChunk) < maxPrintableLen {
					cmpLen = len(leftChunk)
				}
				ExpectWithOffset(off, a[i:cmpLen]).To(Equal(b[i:cmpLen]), "Skiped %v equal bytes.", i)
			}
		}
	}
}
