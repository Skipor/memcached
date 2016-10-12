package memcached

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"

	"github.com/skipor/memcached/cache"
	"github.com/skipor/memcached/internal/mocks"
	"github.com/skipor/memcached/recycle"
	. "github.com/skipor/memcached/testutil"
)

var _ = Describe("reader", func() {
	var (
		input          *bytes.Buffer
		r              reader
		command        []byte
		fields         [][]byte
		clientErr, err error
	)
	ReadCmd := func() {
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
		ReadCmd()
		ExpectNoErrors()
		Expect(command).To(Equal(expectedCommand))
		Expect(fields).To(Equal(expectedFields))
	}
	ExpectReadCmdErr := func(expectedErr error) {
		ReadCmd()
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
				ExpectReadCmdErr(afterInputErr)
			})
		})

		Context("before command end", func() {
			BeforeEach(func() {
				input.WriteString("get xxx ")
			})
			It("fails", func() {
				ExpectReadCmdErr(afterInputErr)
			})
		})

		Context("before large command end", func() {
			BeforeEach(func() {
				input.Write(ChunkWithoutSeparators(5 * MaxCommandSize))
			})
			It("fails", func() {
				ExpectReadCmdErr(afterInputErr)
			})
		})
	})

	BeforeEach(func() {
		input = &bytes.Buffer{}
		r = newReader(input, recycle.NewPool())
	})
	ExpectEOF := func() {
		ReadCmd()
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
		var leak chan *recycle.Data
		BeforeEach(func() {
			leak = make(chan *recycle.Data)
			r.pool.SetLeakCallback(recycle.NotifyOnLeak(leak))
			dbInput = &bytes.Buffer{}
		})
		AfterEach(func() {
			data = nil
			runtime.GC()
			Consistently(leak).ShouldNot(Receive())
		})

		ReadDataBlock := func() {
			data, clientErr, err = r.readDataBlock(dbInput.Len())
		}
		Context("error", func() {
			JustBeforeEach(ReadDataBlock)
			Context("unexpected", func() {
				BeforeEach(func() {
					var n int64 = InBufferSize * 3
					dbInput.ReadFrom(io.LimitReader(FastRand, n))
					input.Write(dbInput.Bytes()[:n/2])
					input.WriteString(Separator)
				})
				It("got read err", func() {
					Expect(unwrap(err)).To(Equal(io.ErrUnexpectedEOF))
					Expect(data).To(BeNil())
				})
			})

			Context("no separator after block", func() {
				BeforeEach(func() {
					var n int64 = InBufferSize * 3
					dbInput.ReadFrom(io.LimitReader(FastRand, n))
					input.Write(dbInput.Bytes())
					input.WriteByte('x')
					input.WriteString(Separator)
				})
				It("got client err", func() {
					Expect(unwrap(clientErr)).To(Equal(ErrInvalidLineSeparator))
					Expect(err).To(BeNil())
					Expect(data).To(BeNil())
				})
			})

		})

		ExpectDataBlockReaded := func() {
			ReadDataBlock()
			ExpectNoErrors()
			readed := &bytes.Buffer{}
			dataReader := data.NewReader()
			dataReader.WriteTo(readed)

			dataReader.Close()
			data.Recycle()

			ExpectBytesEqual(readed.Bytes(), dbInput.Bytes())
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
				dbInput.ReadFrom(io.LimitReader(FastRand, MaxItemSize))
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
				dbInput.ReadFrom(io.LimitReader(FastRand, 2*InBufferSize))
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
				ReadCmd()
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

var _ = Describe("parse key fields", func() {
	var (
		input         string
		extraRequired int

		key     []byte
		extra   [][]byte
		noreply bool
		err     error
	)
	JustBeforeEach(func() {
		fields := bytes.Fields([]byte(input))
		key, extra, noreply, err = parseKeyFields(fields, extraRequired)
	})

	Context("correct input", func() {
		BeforeEach(func() {
			input = "xyz x y z"
			extraRequired = 3
		})
		AssertParsedWell := func() {
			It("parsed well", func() {
				Expect(err).To(BeNil())
				Expect(key).To(BeEquivalentTo("xyz"))
				Expect(extra).To(Equal([][]byte{{'x'}, {'y'}, {'z'}}))
			})
		}
		AssertNoreply := func(b bool) {
			It("noreply ok", func() {
				Expect(noreply).To(Equal(b))
			})
		}
		Context("without noreply", func() {
			AssertParsedWell()
			AssertNoreply(false)
		})
		Context("with noreply", func() {
			BeforeEach(func() { input += " noreply" })
			AssertParsedWell()
			AssertNoreply(true)
		})
	})

	AssertErr := func(expectedErr error) {
		It("expected error", func() {
			Expect(unwrap(err)).To(Equal(expectedErr))
		})
	}

	Context("too many fields", func() {
		BeforeEach(func() {
			input = "x y z noreply"
			extraRequired = 1
		})
		AssertErr(ErrTooManyFields)
	})

	Context("too few fields", func() {
		BeforeEach(func() {
			input = "x noreply"
			extraRequired = 2
		})
		AssertErr(ErrMoreFieldsRequired)
	})

	Context("invalid option", func() {
		BeforeEach(func() {
			input = "x wtf"
			extraRequired = 0
		})
		AssertErr(ErrInvalidOption)
	})
})

var _ = Describe("parse set fields", func() {
	var (
		input   string
		m       cache.ItemMeta
		noreply bool
		err     error
	)
	Parse := func() {
		fields := bytes.Fields([]byte(input))
		m, noreply, err = parseSetFields(fields)
	}
	Context("correct input", func() {
		var (
			key             string
			exptime         int64
			bytes           int
			flags           uint32
			expectedNoreply bool
		)
		BeforeEach(func() {
			key = "xx"
			exptime = 10
			bytes = MaxItemSize
			flags = math.MaxUint32
		})
		JustBeforeEach(func() {
			input = fmt.Sprintf("%s %v %v %v", key, flags, exptime, bytes)
			if expectedNoreply {
				input += " noreply"
			}
			Parse()
		})

		AssertParsedWell := func() {
			It("parsed well", func() {
				Expect(err).To(BeNil())
				Expect(m.Key).To(Equal(key))
				Expect(m.Flags).To(Equal(flags))
				Expect(m.Bytes).To(Equal(bytes))
				Expect(noreply).To(Equal(expectedNoreply))
				if exptime < MaxRelativeExptime {
					exptime += time.Now().Unix()
				}
				Expect([]int64{m.Exptime - 1, m.Exptime}).To(ContainElement(exptime))
			})
		}

		AssertParsedWell()
		Context("with non absolute time", func() {
			BeforeEach(func() { exptime = MaxRelativeExptime + 1 })
			AssertParsedWell()
		})
		Context("with noreply", func() {
			BeforeEach(func() { expectedNoreply = true })
			AssertParsedWell()
		})
	})

	JustBeforeEach(Parse)

	AssertErr := func(expectedErr error) {
		It("expected error", func() {
			Expect(unwrap(err)).To(Equal(expectedErr))
		})
	}

	Context("fields err", func() {
		BeforeEach(func() {
			input = "a b c d e c"
		})
		AssertErr(ErrTooManyFields)
	})
	const correctParams = " 1 1 1"

	Context("large key", func() {
		BeforeEach(func() {
			in := make([]byte, MaxKeySize+1)
			for i := range in {
				in[i] = 'x'
			}
			input = string(in) + correctParams
		})
		AssertErr(ErrTooLargeKey)
	})

	Context("invalid char in key", func() {
		BeforeEach(func() {
			input = "x\x00yz" + correctParams
		})
		AssertErr(ErrInvalidCharInKey)
	})

	Context("invalid param", func() {
		const paramsNum = 3
		var params []interface{}
		BeforeEach(func() { params = []interface{}{1, 1, 1} })
		TestInvalidParam := func(invalid interface{}) func() {
			return func() {
				for i := 0; i < paramsNum; i++ {
					paramIndex := i
					Context(fmt.Sprint("param ", i), func() {
						BeforeEach(func() {
							params[paramIndex] = invalid
							input = fmt.Sprintf("x %v %v %v", params...)
						})
						It("parse error", func() {
							Expect(err).NotTo(BeNil())
							Expect(err.Error()).To(ContainSubstring(ErrFieldsParseError.Error()))
						})
					})
				}
			}
		}
		Context("negative", TestInvalidParam(-1))
		Context("overflow", TestInvalidParam(uint64(1<<63)))
		Context("non numeric", TestInvalidParam("xxx"))
	})
})
