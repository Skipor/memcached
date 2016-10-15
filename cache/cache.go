package cache

import (
	"sync"

	"github.com/skipor/memcached/log"
	"github.com/skipor/memcached/recycle"
)

// Handler implementation must not retain key slices.
type Cache interface {
	Set(i Item)
	Delete(key []byte) (deleted bool)
	// Get returns ItemReaders for keys that was found in cache.
	// views can be nil, if no key was found.
	Get(key ...[]byte) (views []ItemView)
	Touch(key ...[]byte)
}

type Config struct {
	Size int64
}

func NewLRU(l log.Logger, conf Config) *LRU {
	return &LRU{*newLRU(l, conf)}
}

func NewLockingLRU(l log.Logger, conf Config) *LockingLRU {
	return &LockingLRU{*newLRU(l, conf)}
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

func ReadLockingLRUSnapshot(r SnapshotReader, p *recycle.Pool, l log.Logger, conf Config) (c *LockingLRU, err error) {
	var lru *lru
	lru, err = readSnapshot(r, p, l, conf)
	if err != nil {
		return
	}
	c = &LockingLRU{*lru}
	return
}
