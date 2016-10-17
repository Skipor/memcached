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
		ResetTestKeys()
		confFile = testutil.TmpFileName()
		inConf = *config.Default() // Sometimes we want to know defaults.
		inConf.LogLevel = "debug"
		serverConf = memcached.Config{} // Will be filled in JBE.
	})

	StartMemcached := func() {
		var err error
		command := exec.Command(MemcachedCLI, "-config", confFile)
		session, err = Start(command, GinkgoWriter, GinkgoWriter)
		Expect(err).ToNot(HaveOccurred(), "%v", err)
		time.Sleep(50 * time.Millisecond) // Wait for output.
	}
	JustBeforeEach(func() {
		if !util.IsZero(serverConf) {
			Fail("Test should configure inConf, not serverConfig.")
		}
		var err error
		serverConf, err = config.Parse(inConf)
		Expect(err).NotTo(HaveOccurred())
		err = ioutil.WriteFile(confFile, config.Marshal(&inConf), 0600)
		Expect(err).NotTo(HaveOccurred())
		StartMemcached()
	})
	AfterEach(func() {
		session.Terminate().Wait(SessionWaitTime)
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

	})

	Context("load", func() {
		// TODO make configurable load tester.
		// Print RPS, compare with original memcached implementation.
		BeforeEach(func() {
			inConf.LogLevel = "info" // Too large debug output.
		})

		It("", func() {
			LoadTest(serverConf.Addr)
		})
	})

	It("not handle termination without persistence", func() {
		session.Terminate().Wait(SessionWaitTime)
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

		var (
			c   *memcache.Client
			err error
		)
		Connect := func() { c = memcache.New(serverConf.Addr) }
		JustBeforeEach(Connect)

		It("simple cache recover", func() {
			set := RandSizeItem()
			err = c.Set(set)
			Expect(err).ToNot(HaveOccurred())

			session.Interrupt().Wait(SessionWaitTime)
			Expect(session).To(Exit(0))
			StartMemcached()
			Connect()

			get, err := c.Get(set.Key)
			Expect(err).ToNot(HaveOccurred())
			ExpectItemsEqual(get, set)
		})
		Context("input much larger that chache size", func() {
			var (
				its    []*memcache.Item
				inSize int
			)

			BeforeEach(func() {
				inConf.CacheSize = "64k"
				inConf.LogLevel = "info"
			})
			JustBeforeEach(func() {
				Expect(serverConf.Cache.Size).To(BeEquivalentTo(64 << 10))
				for inSize < int(5*serverConf.Cache.Size) {
					set := RandSizeItem()
					err = c.Set(set)
					Expect(err).ToNot(HaveOccurred())
					its = append(its, set)
					inSize += len(set.Key)
					inSize += len(set.Value)
				}
				Expect(inSize).To(BeNumerically(">", serverConf.AOF.RotateSize))
			})

			It("aof is rotated", func() {
				data, err := ioutil.ReadFile(inAOF.Name)
				Expect(err).ToNot(HaveOccurred())
				time.Sleep(time.Second / 2) // Wait for rotation, if any.
				Expect(len(data)).To(BeNumerically("<", serverConf.AOF.RotateSize))
			})

			It("can recover correctly", func() {
				hitMap := make(map[string]*memcache.Item)
				for _, it := range its {
					_, err := c.Get(it.Key)
					if err == memcache.ErrCacheMiss {
						continue
					}
					Expect(err).To(BeNil())
					hitMap[it.Key] = it
				}

				session.Interrupt().Wait(SessionWaitTime)
				Expect(session).To(Exit(0))
				StartMemcached()
				Connect()

				for k, it := range hitMap {
					gotIt, err := c.Get(k)
					Expect(err).ToNot(HaveOccurred())
					ExpectItemsEqual(gotIt, it)
				}
			})

		})

		CorruptLastAOFCmd := func() {
			aof, err := os.Open(serverConf.AOF.Name)
			Expect(err).ToNot(HaveOccurred())
			stat, err := aof.Stat()
			Expect(err).ToNot(HaveOccurred())
			size := stat.Size()
			aof.Close()
			err = os.Truncate(serverConf.AOF.Name, size-3)
			Expect(err).ToNot(HaveOccurred())
		}

		It("cache do not recover from corrupted without option", func() {
			var its []*memcache.Item
			const k = 3
			for i := 0; i < k; i++ {
				set := RandSizeItem()
				err = c.Set(set)
				Expect(err).ToNot(HaveOccurred())
				its = append(its, set)
			}

			session.Interrupt().Wait(SessionWaitTime)
			Expect(session).To(Exit(0))
			CorruptLastAOFCmd()

			StartMemcached()
			session.Wait(SessionWaitTime)
			Expect(session).ShouldNot(Exit(0))
			testutil.Byf("%s", session.Out.Contents())
			Expect(session.Err.Contents()).ToNot(ContainSubstring("panic"))
			Expect(session.Err.Contents()).To(ContainSubstring("FATAL"))
		})

		Context("sync period", func() {
			BeforeEach(func() { inConf.AOF.BufSize = "4k" })
			Context("long sync period set", func() {
				BeforeEach(func() { inConf.AOF.Sync = 1 * time.Minute })
				It("do not sync in short period", func() {
					// Can't use KILL here - signal can be catched, what cause sync.
					// Just check that AOF is empty.
					set := RandSizeItem()
					err = c.Set(set)
					Expect(err).ToNot(HaveOccurred())
					time.Sleep(time.Second / 2)
					data, err := ioutil.ReadFile(inAOF.Name)
					Expect(err).ToNot(HaveOccurred())
					Expect(data).To(BeEmpty())
				})
			})
			Context("short sync period set", func() {
				BeforeEach(func() { inConf.AOF.Sync = 200 * time.Millisecond })
				It("do sync in short period", func() {
					// Can't use KILL here - signal can be catched, what cause sync.
					// Just check that AOF is NOT empty.
					set := RandSizeItem()
					err = c.Set(set)
					Expect(err).ToNot(HaveOccurred())
					time.Sleep(time.Second / 2)
					data, err := ioutil.ReadFile(inAOF.Name)
					Expect(err).ToNot(HaveOccurred())
					Expect(data).ToNot(BeEmpty())
				})
			})

		})

		Context("fix corrupted", func() {
			BeforeEach(func() { inConf.AOF.FixCorrupted = true })
			It("cache recover from corrupted with option", func() {
				var its []*memcache.Item
				const k = 3
				for i := 0; i < k; i++ {
					set := RandSizeItem()
					err = c.Set(set)
					Expect(err).ToNot(HaveOccurred())
					its = append(its, set)
				}

				session.Interrupt().Wait(SessionWaitTime)
				Expect(session).To(Exit(0))
				CorruptLastAOFCmd()

				StartMemcached()
				Connect()

				_, err := c.Get(its[k-1].Key)
				Expect(err).To(Equal(memcache.ErrCacheMiss))
				for i := 0; i < k-1; i++ {
					it := its[i]
					gotIt, err := c.Get(it.Key)
					Expect(err).ToNot(HaveOccurred())
					ExpectItemsEqual(gotIt, it)
				}
			})
		})
	})
})
