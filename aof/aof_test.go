package aof

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/skipor/memcached/log"
	. "github.com/skipor/memcached/testutil"
)

var _ = FDescribe("AOF init", func() {
	const rotateSize = 1 << 30 // Large enough for not starting rotation.
	const oneWriteLimit = 1024
	var (
		aof         *AOF
		filename    string
		conf        Config
		writeNum    int
		dataWriten  *bytes.Buffer
		initialData *bytes.Buffer
	)
	BeforeEach(func() {
		dataWriten = &bytes.Buffer{}
		initialData = &bytes.Buffer{}
		filename = testFileName()
		conf = Config{
			Name:       filename,
			RotateSize: rotateSize,
		}
		writeNum = Rand.Intn(5)
		conf.BuffSize = Rand.Intn(oneWriteLimit * 2)
	})
	AfterEach(func() {
		err := os.Remove(filename)
		Expect(err).To(BeNil())
	})
	JustBeforeEach(func() {
		var err error
		aof, err = Open(log.NewLogger(log.DebugLevel, GinkgoWriter), panicRotator, conf)
		Expect(err).To(BeNil(), "%v", err)
	})
	ExpectFileDataEqualExpected := func() {
		data, err := ioutil.ReadFile(filename)
		Expect(err).To(BeNil())
		ExpectBytesEqual(data, append(initialData.Bytes(), dataWriten.Bytes()...))
	}
	WriteSomeData := func() {
		for i, end := 0, Rand.Intn(5)+1; i < end; i++ {
			p := make([]byte, Rand.Intn(oneWriteLimit))
			io.ReadFull(Rand, p)
			t := aof.NewTransaction()
			_, err := io.MultiWriter(dataWriten, t).Write(p)
			Expect(err).To(BeNil())
			err = t.Close()
			Expect(err).To(BeNil())
		}
	}
	Context("bufferred without close", func() {
		BeforeEach(func() {
			data := make([]byte, Rand.Intn(oneWriteLimit))
			initialData.Write(data)
			err := ioutil.WriteFile(filename, data, Perm)
			Expect(err).To(BeNil())
			conf.BuffSize = 10 * oneWriteLimit
		})
		It("", func() {
			WriteSomeData()
			ExpectFileDataEqualExpected()
		})
	})
	It("create new ", func() {
		WriteSomeData()
		ExpectFileDataEqualExpected()
	})

})

var _ = Describe("AOF write and sync", func() {
	const rotateSize = 1 << 30 // Large enough for not starting rotation.
	const writeNum = 3
	var (
		aof      *AOF
		data     []byte
		mfile    *mockFile
		mflusher *mockFlusher
	)
	BeforeEach(func() {
		Fuzz(&data)
		mfile = &mockFile{}
		mflusher = &mockFlusher{}
		mfile.On("Write", data).Return(len(data), nil)
		mflusher.On("Flush").Return(nil)
		aof = &AOF{
			writer:  mfile,
			flusher: mflusher,
			file:    mfile,
			config: Config{
				RotateSize: rotateSize,
			},
		}
	})
	AfterEach(func() {
		mfile.AssertExpectations(GinkgoT())
		mflusher.AssertExpectations(GinkgoT())
	})
	WriteData := func() {
		t := aof.NewTransaction()
		t.Write(data)
		t.Close()
	}

	It("sync period less than min", func() {
		aof.config.SyncPeriod = MinSyncPeriod - 1
		Expect(aof.isSyncEveryTransaction()).To(BeTrue())
		mfile.On("Sync").Return(func() error {
			return nil
		})
		for i := 0; i < writeNum; i++ {
			WriteData()
			Expect(aof.size).To(BeEquivalentTo(len(data) * (i + 1)))
			mfile.AssertNumberOfCalls(GinkgoT(), "Sync", i+1)
			mflusher.AssertNumberOfCalls(GinkgoT(), "Flush", i+1)
		}
		mfile.On("Close").Return(nil).Once()
		aof.Close()
		mflusher.AssertNumberOfCalls(GinkgoT(), "Flush", writeNum+1)
	})

	It("background sync", func() {
		const syncPeriod = MinSyncPeriod
		aof.config.SyncPeriod = syncPeriod
		Expect(aof.isSyncEveryTransaction()).To(BeFalse())
		onSync := make(chan struct{})
		mfile.On("Sync").Return(func() error {
			onSync <- struct{}{}
			return nil
		})
		aof.startSync()
		for i := 0; i < writeNum; i++ {
			WriteData()
			Eventually(onSync, 2*syncPeriod).Should(Receive())
		}
		Consistently(onSync, 2*syncPeriod).ShouldNot(Receive())

		mfile.AssertNumberOfCalls(GinkgoT(), "Sync", writeNum)
		mflusher.AssertNumberOfCalls(GinkgoT(), "Flush", writeNum)
		mfile.On("Close").Return(nil).Once()
		aof.Close()
		mflusher.AssertNumberOfCalls(GinkgoT(), "Flush", writeNum+1)

		// Test that background routine finished.
		aof.size += 10
		Consistently(onSync, 2*syncPeriod).ShouldNot(Receive())
	})

})
