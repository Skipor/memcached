package recycle

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"

	"github.com/skipor/memcached/mocks"
)

var _ = Describe("Pool create", func() {
	var p *Pool
	var chunkSizes []int

	Context("nil chunkSizes", func() {
		BeforeEach(func() {
			p = NewPoolSizes(nil)
			chunkSizes = nil
		})
		It("use defaults", func() {
			Expect(p.chunkSizes).To(Equal(DefaultChunkSizes))
		})
	})

	Context("invalid configuration", func() {
		JustBeforeEach(func() {
			Expect(func() {
				NewPoolSizes(chunkSizes)
			}).Should(Panic())
		})
		Context("when chunk sizes are unsorted", func() {
			BeforeEach(func() {
				chunkSizes = []int{1 << 10, 1 << 8}
			})
			It("creation panics", func() {})
		})

		Context("when chunk sizes have duplicates", func() {
			BeforeEach(func() {
				chunkSizes = []int{1 << 8, 1 << 10, 1 << 10}
			})
			It("creation panics", func() {})
		})
	})
})

var _ = Describe("chunk requested", func() {
	var p *Pool
	BeforeEach(func() {
		p = NewPool()
	})

	var chunkSize int
	var chunk, chunkCopy []byte
	JustBeforeEach(func() {
		chunk = p.chunk(chunkSize)
		chunkCopy = make([]byte, len(chunk))
		Rand.Read(chunkCopy)
		copy(chunk, chunkCopy)
	})

	AssertSizeEqualRequested := func() {
		It("size equal requested", func() {
			Expect(chunk).To(HaveLen(chunkSize))
		})
	}
	AssertChunkReturnsAfterRecycle := func() {
		It("it returns after recycle", func() {
			if RaceEnabled {
				Skip("no pooling happens when race detector is on")
			}
			p.recycleChunk(chunk)
			Expect(p.chunk(chunkSize)).To(Equal(chunk))
			Expect(chunk).To(Equal(chunkCopy))
		})
	}

	Context("size is few less than max", func() {
		BeforeEach(func() {
			chunkSize = p.MaxChunkSize() - 4
		})
		AssertSizeEqualRequested()
		AssertChunkReturnsAfterRecycle()
	})

	Context("size is greater than max", func() {
		BeforeEach(func() {
			chunkSize = p.MaxChunkSize() + 4
		})
		It("size equal max", func() {
			Expect(chunk).To(HaveLen(p.MaxChunkSize()))
		})
		AssertChunkReturnsAfterRecycle()
	})

	Context("size is little greater than half of min", func() {
		BeforeEach(func() {
			chunkSize = p.MinChunkSize()/2 + 1
		})
		AssertSizeEqualRequested()
		AssertChunkReturnsAfterRecycle()
	})

	Context("size is half of min", func() {
		BeforeEach(func() {
			chunkSize = p.MinChunkSize() / 2
		})
		AssertSizeEqualRequested()
		It("do not returns after recycle", func() {
			p.recycleChunk(chunk)
			Expect(p.chunk(chunkSize)).NotTo(Equal(chunk))
		})
	})
})

var _ = Describe("data read", func() {
	var p *Pool
	var input []byte
	var inputReader io.Reader
	var data *Data
	var err error

	BeforeEach(func() {
		p = NewPool()
		size := Rand.Intn(p.MaxChunkSize() * 128)
		input = make([]byte, size)
		Rand.Read(input)
		inputReader = bytes.NewReader(input)
	})

	Context("no read error happen", func() {
		JustBeforeEach(func() {
			data, err = p.ReadData(inputReader, len(input))
			Expect(err).To(BeNil())
		})

		It("chunked data equals readed", func() {
			buf := &bytes.Buffer{}
			for _, ch := range data.chunks {
				buf.Write(ch)
			}
			By(fmt.Sprintf("Chunks num: %v", len(data.chunks)))
			Expect(buf.Len()).To(Equal(len(input)))
			Expect(buf.Bytes()).To(Equal(input))
		})

		Context("concurrent reads", func() {
			var k int
			var readResults chan []byte
			JustBeforeEach(func() {
				k = Rand.Intn(10)
				readResults = make(chan []byte)
				for i := 0; i < k; i++ {
					r := data.NewReader()
					go func() {
						defer GinkgoRecover()
						sleep := 50 + Rand.Intn(100)
						time.Sleep(time.Duration(sleep) * time.Millisecond)
						buf := &bytes.Buffer{}
						r.WriteTo(buf)
						r.Close()
						Expect(r.isClosed()).To(BeTrue())
						readResults <- buf.Bytes()
					}()
				}

				It("all readers got correct result", func() {
					for i := 0; i < k; i++ {
						Expect(<-readResults).To(Equal(input))
					}
				})

				It("data recycled after all reads", func() {
					data.Recycle()
					Expect(data.isRecycled()).To(BeFalse())
					for i := 0; i < k; i++ {
						<-readResults
					}
					Expect(data.isRecycled()).To(BeTrue())
				})

			})
		})

		Context("read access after recycle", func() {
			It("panic", func() {
				data.Recycle()
				Expect(func() { data.NewReader() }).To(Panic())
			})
		})

		Context("leak callback set", func() {
			var leak chan *Data
			BeforeEach(func() {
				leak = make(chan *Data)
				p.SetLeakCallback(NotifyOnLeak(leak))
			})

			gcData := func() {
				data = nil
				runtime.GC()
			}

			Context("data was recycled", func() {
				JustBeforeEach(func() {
					data.Recycle()
					gcData()
				})
				It("callback not called", func() {
					Eventually(leak).ShouldNot(Receive())
				})
			})

			Context("data was not recycled", func() {
				JustBeforeEach(gcData)
				It("callback called", func() {
					Eventually(leak).Should(Receive())
				})
			})

			Context("reader was not closed ", func() {
				JustBeforeEach(func() {
					data.NewReader()
					data.Recycle()
					gcData()
				})
				It("callback called", func() {
					Eventually(leak).Should(Receive())
				})
			})
		})
	})

	Context("unexpected EOF", func() {
		BeforeEach(func() {
			data, err = p.ReadData(inputReader, len(input)+1)
			Expect(err).NotTo(BeNil())
		})
		It("io.UnexpectedEOF returned", func() {
			Expect(err).To(Equal(io.ErrUnexpectedEOF))
		})
		It("no data retuned", func() {
			Expect(data).To(BeNil())
		})
	})

	Context("read error happen", func() {
		expectedErr := errors.New("err")
		BeforeEach(func() {
			mr := &mocks.Reader{}
			mr.On("Read", mock.Anything).Return(0, expectedErr)
			data, err = p.ReadData(mr, len(input))
			Expect(err).NotTo(BeNil())
		})

		It("error equals that return underlying reader", func() {
			Expect(err).To(Equal(expectedErr))
		})
		It("no data returned", func() {
			Expect(data).To(BeNil())
		})
	})
})
