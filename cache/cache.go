package cache

import (
	"sync"

	"github.com/skipor/memcached/log"
)

// Handler implementation must not retain key slices.
type Cache interface {
	Set(i Item)
	Delete(key []byte) (deleted bool)
	// Get returns ItemReaders for keys that was found in cache.
	// Readers can be nil, if no key was found.
	Get(key ...[]byte) (readers []ItemView)
	Touch(key ...[]byte)
}

type Config struct {
	Size int64
}

func NewLRU(l log.Logger, conf Config) *LRU {
	c := &LRU{}
	c.init(l, conf)
	return c
}

func NewLockingLRU(l log.Logger, conf Config) *LockingLRU {
	c := &LockingLRU{}
	c.init(l, conf)
	return c
}

// LRU is Cache with auto locking on Cache operations.
type LRU struct{ lru }

var _ Cache = (*LRU)(nil)

func (c *LRU) Set(i Item) {
	c.lock.Lock()
	c.set(i)
	c.lock.Unlock()
}

func (c *LRU) Delete(key []byte) (deleted bool) {
	c.lock.Lock()
	deleted = c.delete(key)
	c.lock.Unlock()
	return
}

func (c *LRU) Get(keys ...[]byte) (views []ItemView) {
	c.lock.RLock()
	views = c.get(keys...)
	c.lock.RUnlock()
	return
}

func (c *LRU) Touch(keys ...[]byte) {
	c.lock.RLock()
	c.touch(keys...)
	c.lock.RUnlock()
}

type RWCache interface {
	Cache
	sync.Locker
	RLock()
	RUnlock()
}

// LockingLRU is cache that requires explicit lock calls.
type LockingLRU struct{ lru }

var _ RWCache = (*LockingLRU)(nil)

func (c *LockingLRU) Set(i Item)                            { c.set(i) }
func (c *LockingLRU) Delete(key []byte) (deleted bool)      { return c.delete(key) }
func (c *LockingLRU) Get(keys ...[]byte) (views []ItemView) { return c.get(keys...) }
func (c *LockingLRU) Touch(keys ...[]byte)                  { c.touch(keys...) }

func (c *LockingLRU) Lock()    { c.lock.Lock() }
func (c *LockingLRU) Unlock()  { c.lock.Unlock() }
func (c *LockingLRU) RLock()   { c.lock.RLock() }
func (c *LockingLRU) RUnlock() { c.lock.RUnlock() }
