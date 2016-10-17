package memcached

import (
	"io"
	"io/ioutil"
	"testing"

	. "github.com/Skipor/memcached/testutil"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMemcached(t *testing.T) {

	RegisterFailHandler(Fail)
	RunSpecs(t, "Memcached Suite")
}

func ChunkWithoutSeparators(size int) []byte {
	ch, _ := ioutil.ReadAll(io.LimitReader(Rand, int64(size)))
	for i, b := range ch {
		for _, sb := range []byte(Separator) {
			if b == sb {
				ch[i] = 'x'
			}
		}
	}
	return ch
}

const (
	Anything           = `.+`
	KeyPattern         = `[\w[:punct:]]+`
	ErrorMsgPattern    = `[ \w[:punct:]]+`
	SeparatorPattern   = `\r\n`
	ErrorPattern       = ErrorResponse + SeparatorPattern
	ClientErrorPattern = ClientErrorResponse + ` ` + ErrorMsgPattern + SeparatorPattern
	ServerErrorPattern = ServerErrorResponse + ` ` + ErrorMsgPattern + SeparatorPattern
	StoredPattern      = StoredResponse + SeparatorPattern
	EndPattern         = EndResponse + SeparatorPattern
	DeletedPattern     = DeletedResponse + SeparatorPattern
	NotFoundPattern    = NotFoundResponse + SeparatorPattern
)
