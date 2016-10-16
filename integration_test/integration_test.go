package integration

import (
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"

	"github.com/skipor/memcached"
	"github.com/skipor/memcached/cmd/memcached/config"
	"github.com/skipor/memcached/internal/tag"
	"github.com/skipor/memcached/internal/util"
	"github.com/skipor/memcached/testutil"
)

var _ = Describe("Integration", func() {
	BeforeEach(func() {
		if tag.Race {
			Skip("Integration is not running under race detector.")
		}
	})
	const SessionWaitTime = 3 * time.Second
	var (
		confFile   string
		inConf     config.Config    // App config to run.
		serverConf memcached.Config // Parsed config. Read only.

		session *Session
	)
	BeforeEach(func() {
		confFile = testutil.TmpFileName()
		inConf = *config.Default() // Sometimes we want to know defaults.
		inConf.LogLevel = "debug"
		serverConf = memcached.Config{}

	})
	JustBeforeEach(func() {
		if !util.IsZero(serverConf) {
			Fail("Test should set inConf fields,  not serverConfig fields. ")
		}
		var err error
		serverConf, err = config.Parse(inConf)
		Expect(err).NotTo(HaveOccurred())

		err = ioutil.WriteFile(confFile, config.Marshal(&inConf), 0600)
		Expect(err).NotTo(HaveOccurred())
		command := exec.Command(MemcachedCLI, "-config", confFile)
		session, err = Start(command, GinkgoWriter, GinkgoWriter)
		Expect(err).ToNot(HaveOccurred(), "%v", err)
		time.Sleep(50 * time.Millisecond) // Wait for output.
	})
	AfterEach(func() {
		session.Terminate().Wait()
	})

	Context("simple requests", func() {
		var (
			c   *memcache.Client
			err error
		)
		JustBeforeEach(func() {
			c = memcache.New(serverConf.Addr)
		})
		It("get what set", func() {
			set := RandSizeItem()
			err = c.Set(set)
			Expect(err).To(BeNil())
			get, err := c.Get(set.Key)
			Expect(err).To(BeNil())
			ExpectItemsEqual(get, set)
		})

		It("overwrite", func() {
			set := RandSizeItem()
			overwrite := RandSizeItem()
			overwrite.Key = set.Key
			err = c.Set(set)
			Expect(err).To(BeNil())
			err = c.Set(overwrite)
			Expect(err).To(BeNil())

			get, err := c.Get(set.Key)
			Expect(err).To(BeNil())
			ExpectItemsEqual(get, overwrite)
		})

		It("delete", func() {
			set := RandSizeItem()
			err = c.Set(set)
			Expect(err).To(BeNil())

			err = c.Delete(set.Key)
			_, err = c.Get(set.Key)
			Expect(err).To(Equal(memcache.ErrCacheMiss))
		})

		It("multi get", func() {
			var keys []string
			items := map[string]*memcache.Item{}
			for i := 0; i < 10; i++ {
				i := RandSizeItem()
				keys = append(keys, i.Key)
				items[i.Key] = i
				err = c.Set(i)
				Expect(err).To(BeNil())
			}
			gotItems, err := c.GetMulti(keys)
			Expect(err).To(BeNil())
			Expect(len(gotItems)).To(Equal(len(items)))
			for k, v := range gotItems {
				ExpectItemsEqual(v, items[k])
			}
		})
		Context("load", func() {
			BeforeEach(func() {
				inConf.LogLevel = "info" // Too large debug output.
			})

			It("", func() {
				LoadTest(serverConf.Addr)
			})
		})

	})

	It("not handle termination without persistence", func() {
		session.Terminate().Wait()
		Expect(session).To(Exit(143))
	})

	Context("persistence on", func() {
		var inAOF *config.AOFConfig //shortcut
		BeforeEach(func() {
			inAOF = &inConf.AOF
			inAOF.Name = testutil.TmpFileName()
		})
		AfterEach(func() {
			os.Remove(inAOF.Name)
		})

		It("handle terminate", func() {
			session.Terminate().Wait(SessionWaitTime)
			Expect(session).To(Exit(0))
		})
		It("handle interrupt", func() {
			session.Interrupt().Wait(SessionWaitTime)
			Expect(session).To(Exit(0))
		})
	})
})
