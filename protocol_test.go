package memcached

import (
	"bytes"
	"errors"
	"io"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/skipor/memcached/mocks"
	"github.com/skipor/memcached/recycle"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("reader", func() {
	var (
		input          *bytes.Buffer
		r              reader
		command        []byte
		fields         [][]byte
		clientErr, err error
	)
	ReadCommand := func() {
		command, fields, clientErr, err = r.readCommand()
	}

	const correctCommand = "get xxx   yyy " + Separator
	var expectedCommand = []byte("get")
	var expectedFields = [][]byte{[]byte("xxx"), []byte("yyy")}

	ExpectNoErrors := func() {
		Expect(clientErr).To(BeNil())
		Expect(err).To(BeNil())
	}
	ExpectCommandReaded := func() {
		ReadCommand()
		ExpectNoErrors()
		Expect(command).To(Equal(expectedCommand))
		Expect(fields).To(Equal(expectedFields))
	}
	ExpectErr := func(expectedErr error) {
		ReadCommand()
		Expect(unwrap(err)).To(Equal(expectedErr))
		Expect(command).To(BeNil())
		Expect(fields).To(BeNil())
	}

	Context("read error", func() {
		var afterInputErr error
		JustBeforeEach(func() {
			afterInputErr = errors.New("some read error")
			mr := &mocks.Reader{}
			mr.On("Read", mock.Anything).Return(0, afterInputErr)
			r = newReader(io.MultiReader(input, mr), nil)
		})

		Context("just after some commands", func() {
			var n int
			BeforeEach(func() {
				n = Rand.Intn(3)
				for i := 0; i < n; i++ {
					input.WriteString(correctCommand)
				}
			})
			It("fails after them", func() {
				for i := 0; i < n; i++ {
					ExpectCommandReaded()
				}
				ExpectErr(afterInputErr)
			})
		})

		Context("before command end", func() {
			BeforeEach(func() {
				input.WriteString("get xxx ")
			})
			It("fails", func() {
				ExpectErr(afterInputErr)
			})
		})

		Context("before large command end", func() {
			BeforeEach(func() {
				input.Write(ChunkWithoutSeparators(5 * MaxCommandSize))
			})
			It("fails", func() {
				ExpectErr(afterInputErr)
			})
		})
	})

	BeforeEach(func() {
		input = &bytes.Buffer{}
		r = newReader(input, recycle.NewPool())
	})
	ExpectEOF := func() {
		ReadCommand()
		Expect(unwrap(err)).To(Equal(io.EOF))
		Expect(clientErr).To(BeNil())
		Expect(command).To(BeNil())
		Expect(fields).To(BeNil())
	}

	Context("empty input", func() {
		It("got EOF", func() {
			ExpectEOF()
		})
	})

	Context("n correct commands", func() {
		var n int
		JustBeforeEach(func() {
			for i := 0; i < n; i++ {
				input.WriteString(correctCommand)
			}
		})
		AssertAllReadedWell := func() {
			It("all of them readed well", func() {
				for i := 0; i < n; i++ {
					ExpectCommandReaded()
				}
				ExpectEOF()
			})
		}

		Context("n = 0 ", func() {
			BeforeEach(func() { n = 0 })
			AssertAllReadedWell()
		})
		Context("n = some ", func() {
			BeforeEach(func() { n = Rand.Intn(50) + 1 })
			AssertAllReadedWell()
		})
		Context("n = really big ", func() {
			BeforeEach(func() {
				n = Rand.Intn(2*MaxCommandSize/len(correctCommand)) + 1
			})
			AssertAllReadedWell()
		})
		Context("n = some ", func() {
			BeforeEach(func() { n = Rand.Intn(50) + 1 })
			AssertAllReadedWell()
		})
	})

	Context("data block", func() {
		var data *recycle.Data
		var dbInput *bytes.Buffer
		BeforeEach(func() {
			dbInput = &bytes.Buffer{}
		})
		ReadDataBlock := func() {
			data, clientErr, err = r.readDataBlock(dbInput.Len())
		}
		ExpectDataBlockReaded := func() {
			ReadDataBlock()
			ExpectNoErrors()
			readed := &bytes.Buffer{}
			dataReader := data.NewReader()
			dataReader.WriteTo(readed)

			dataReader.Close()
			data.Recycle()

			Expect(readed.Bytes()).To(Equal(dbInput.Bytes()))
		}

		Context("empty block", func() {
			BeforeEach(func() {
				input.WriteString(Separator)
			})
			It("readed well", func() {
				ExpectDataBlockReaded()
				ExpectEOF()
			})
		})

		Context("only correct data block", func() {
			BeforeEach(func() {
				dbInput.ReadFrom(io.LimitReader(fastRand, 2*InBufferSize))
				input.Write(dbInput.Bytes())
				input.WriteString(Separator)
			})
			It("readed well", func() {
				ExpectDataBlockReaded()
				ExpectEOF()
			})
		})

		Context("between commands", func() {
			BeforeEach(func() {
				input.WriteString(correctCommand)
				dbInput.ReadFrom(io.LimitReader(fastRand, 2*InBufferSize))
				input.Write(dbInput.Bytes())
				input.WriteString(Separator)
				input.WriteString(correctCommand)
			})
			It("all readed well", func() {
				ExpectCommandReaded()
				ExpectDataBlockReaded()
				ExpectCommandReaded()
				ExpectEOF()
			})
		})

	})

	Context("client error in input ", func() {
		// Test cases input structure: 1)correct command 2) some error input that produce error 3) correct command
		BeforeEach(func() {
			input.WriteString(correctCommand)
		})
		JustBeforeEach(func() {
			input.WriteString(correctCommand)
		})

		AssertClientErrEqual := func(expectedClientErr error) {
			It("client error equal expected", func() {
				ExpectCommandReaded()
				ReadCommand()
				if clientErr != nil {
					By("Got error: " + clientErr.Error())
				}
				Expect(unwrap(clientErr)).To(Equal(expectedClientErr))
				Expect(err).To(BeNil())
				ExpectCommandReaded()
				ExpectEOF()
			})
		}

		Context("illegal separator", func() {
			BeforeEach(func() {
				input.WriteString(strings.TrimSuffix(correctCommand, Separator))
				input.WriteByte('\n')
			})
			AssertClientErrEqual(ErrInvalidLineSeparator)
		})

		Context("too large command", func() {
			BeforeEach(func() {
				// Large command without separators
				noSepBigChunk := ChunkWithoutSeparators(3*InBufferSize + Rand.Intn(InBufferSize))
				n := len(noSepBigChunk)
				noSepBigChunk[n/2+Rand.Intn(n/4)] = '\n'
				input.Write(noSepBigChunk)
				input.WriteString(Separator)
			})
			AssertClientErrEqual(ErrTooLargeCommand)
		})

	})

})

var _ = Describe("parse", func() {
	//TODO

})
