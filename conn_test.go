package memcached

import (
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

type Out struct {
	buf *gbytes.Buffer
}

var _ gbytes.BufferProvider = (*Out)(nil)

func (o *Out) Buffer() *gbytes.Buffer {
	return o.buf
}

var _ = FDescribe("Conn", func() {
	var ()
	BeforeEach(func() {
		io.Pipe()
		Expect(true).To(BeTrue())
	})

})
