package aof

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/skipor/memcached/log"
	. "github.com/skipor/memcached/testutil"
)

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
		data = make([]byte, Rand.Intn(1024)+1)
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
		By("data write")
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

var _ = Describe("AOF init", func() {
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
		filename = TmpFileName()
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

var _ = Describe("AOF rotation", func() {
	AfterEach(resetTestHooks)
	var (
		initial            []byte
		beforeFileSnapshot []byte
		rotated            []byte
		afterFileSnapshot  []byte
		afterExtraWrite    []byte
		afterFinish        []byte

		aof *AOF
	)
	mRotator := RotatorFunc(func(r ROFile, w io.Writer) error {
		fileSnapshotData, err := ioutil.ReadAll(r)
		Expect(err).To(BeNil())
		ExpectBytesEqual(fileSnapshotData, append(initial, beforeFileSnapshot...))

		_, err = w.Write(rotated)
		Expect(err).To(BeNil())
		return nil

	})

	Write := func(p []byte) {
		t := aof.NewTransaction()
		t.Write(p)
		t.Close()
	}
	It("rotation ok", func(done Done) {
		const RotationSize = 4 * (1 << 10)
		const BuffSize = (1 << 10)
		initial = make([]byte, Rand.Intn(RotationSize-10))
		io.ReadFull(Rand, initial)
		beforeFileSnapshot = make([]byte, RotationSize+1-len(initial)+Rand.Intn(BuffSize))
		rotated = make([]byte, Rand.Intn(int(RotationSize*(MinRotateCompress*100))/100))
		Fuzz(&afterFileSnapshot)
		Fuzz(&afterExtraWrite)
		Fuzz(&afterFinish)

		afterFileSnapshotTestHook = func() { Write(afterFileSnapshot) }
		afterExtraWriteTestHook = func() { Write(afterExtraWrite) }

		finish := &sync.WaitGroup{}
		finish.Add(1)
		afterFinishTestHook = func() {
			Write(afterFinish)
			finish.Done()
		}
		expectedData := bytes.Join([][]byte{rotated, afterFileSnapshot, afterExtraWrite, afterFinish}, nil)

		filename := TmpFileName()
		defer os.Remove(filename)
		err := ioutil.WriteFile(filename, initial, Perm)
		Expect(err).To(BeNil())
		conf := Config{
			Name:       filename,
			BuffSize:   BuffSize,
			RotateSize: RotationSize,
		}

		aof, err = Open(log.NewLogger(log.DebugLevel, GinkgoWriter), mRotator, conf)
		Expect(err).To(BeNil())

		sep := RotationSize - len(initial) - 1
		Write(beforeFileSnapshot[:sep])
		Expect(aof.rotateInProcess).To(BeFalse())
		Write(beforeFileSnapshot[sep:])

		finish.Wait()

		Expect(aof.size).To(BeEquivalentTo(len(expectedData)))
		Expect(aof.rotateInProcess).To(BeFalse())
		err = aof.Close()
		Expect(err).To(BeNil())
		actual, err := ioutil.ReadFile(filename)
		Expect(err).To(BeNil())
		ExpectBytesEqual(actual, expectedData)
		close(done)
	})

})
