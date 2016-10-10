package cache

import (
	"sync"

	"time"

	"github.com/skipor/memcached/internal/tag"
	"github.com/skipor/memcached/log"
)

type Temp uint8

const (
	cold Temp = iota
	warm
	hot
	temps   = 3
	hotCap  = 0.32
	warmCap = 0.32
)

// Handler implementation must not retain key slices.
type Cache interface {
	Set(i Item)
	// Get returns ItemReaders for keys that was found in cache.
	// Readers can be nil, if no key was found.
	Get(key ...[]byte) (readers []ItemView)
	Delete(key []byte) (deleted bool)
}

type Config struct {
	Size int64
}

func NewCache(l log.Logger, conf Config) Cache {
	c := &cache{
		table: make(map[string]*node),
		limits: limits{
			total: conf.Size,
			hot:   conf.Size * (hotCap * 100) / 100,
			warm:  conf.Size * (warmCap * 100) / 100,
		},
	}
	for _, lru := range c.lrus[:] {
		lru.init()
		lru.onExpire = c.onExpire
	}
	c.hot().onEvictActive = moveToTail
	c.warm().onEvictActive = moveToTail
	c.cold().onEvictActive = moveTo(c.warm())

	c.hot().onEvictInactive = moveTo(c.cold())
	c.warm().onEvictInactive = moveTo(c.cold())
	c.cold().onEvictInactive = c.onEvict
	return c
}

// Note: Doc based on ginhub.com/memcached/memcached/doc/new_lru.txt
// * There are HOT, WARM, and COLD LRU's. New items enter the
// HOT LRU.
// * LRU updates only happen as items reach the bottom of an LRU. If active in
// HOT, stay in HOT, if active in WARM, stay in WARM. If active in COLD, move
// to WARM.
// * HOT/WARM each capped at 32% of memory available for that slab class. COLD
// is uncapped (by default, as of this writing).
// * Items flow from HOT/WARM into COLD.
//
// The primary goal is to better protect active items from "scanning". Items
// which are never hit again will flow from HOT, through COLD, and out the
// bottom. Items occasionally active (reaching COLD, but being hit before
// eviction), move to WARM. There they can stay relatively protected.

type cache struct {
	sync.RWMutex
	table  map[string]*node
	lrus   [temps]lru
	limits limits
	log    log.Logger
}

type limits struct {
	total int64
	hot   int64
	warm  int64
}

var _ Cache = (*cache)(nil)

func (c *cache) Set(i Item) {
	c.Lock()
	defer c.Unlock()

	n, ok := c.table[i.Key]
	if ok {
		c.log.Debugf("Override item %s.", i.Key)
		n.Data.Recycle()
		n.Item = i
		n.active = active
	} else {
		n = newNode(i)
		c.table[i.Key] = n
		c.lrus[hot].pushBack(n)
	}

	if n.size() > c.limits.hot {
		// TODO do this check earlier
		panic("too large item")
	}

	if c.hotOverflow() || c.overflow() {
		// TODO do this in backgroud goroutine. That improves latency.
		c.fixOverflows()
	}

}

func (c *cache) Get(keys ...[]byte) (views []ItemView) {
	c.RLock()
	defer c.RUnlock()
	now := time.Now().Unix()
	for _, key := range keys {
		if n, ok := c.table[string(key)]; ok { // No allocation.
			if !n.expired(now) {
				views = append(views, n.NewView())
			}
		}
	}
	return
}

func (c *cache) Delete(key []byte) (deleted bool) {
	c.Lock()
	defer c.Unlock()
	n, ok := c.table[string(key)] // No allocation.
	link(n.prev, n.next)
	c.delete(n)
	return ok
}

func (c *cache) fixOverflows() {
	now := time.Now().Unix()
	if c.hotOverflow() {
		c.hot().shrink(c.limits.hot, now)
	}
	if !c.overflow() {
		return
	}
	// Total overflow. Shrink cold.
	c.cold().shrink(c.coldLimit(), now)

	if !c.warmOverflow() {
		return
	}
	// Some some active cold flow to warm. Need to shrink warm.
	c.cold().shrink(c.coldLimit(), now)

	if !c.overflow() {
		return
	}
	// There was too many active colds, that became warm.
	// There are no active colds now, so we can evict them.
	c.cold().shrink(c.coldLimit(), now)

	if c.overflow() {
		panic("Overflow after cache eviction. Should not happen.")
	}
}

// onEvict is callback for lru.
func (c *cache) onEvict(n *node) {
	c.log.Debugf("Item %s evicted.", n.Key)
	c.delete(n)
}

// onExpire is callback for lru.
func (c *cache) onExpire(n *node) {
	c.log.Debugf("Item %s expired.", n.Key)
	c.delete(n)
}

// delete removes owned but detached from lru list node.
func (c *cache) delete(n *node) {
	n.owner.size -= n.size()
	n.Data.Recycle()
	delete(c.table, string(n.Key))
	if tag.Debug {
		n.next = nil
		n.prev = nil
		n.owner = nil
		n.Data = nil
	}
}

func (c *cache) hot() *lru          { return &c.lrus[hot] }
func (c *cache) warm() *lru         { return &c.lrus[warm] }
func (c *cache) cold() *lru         { return &c.lrus[cold] }
func (c *cache) free() int64        { return c.limits.total - c.size() }
func (c *cache) coldLimit() int64   { return c.cold().size + c.free() }
func (c *cache) hotOverflow() bool  { return c.lrus[hot].size > c.limits.hot }
func (c *cache) warmOverflow() bool { return c.lrus[warm].size > c.limits.warm }
func (c *cache) overflow() bool     { return c.free() < 0 }

func (c *cache) size() int64 {
	var size int64
	for i := range c.lrus[:] {
		size += c.lrus[i].size
	}
	return size
}

// Snapshot requires write lock be acquired.
func (c *cache) snapshot() *cacheSnapshot {
	// TODO after main logic
	panic("NIY")
}

type cacheSnapshot struct {
}

type nopUnlocker struct{}

var _ sync.Locker = nopUnlocker{}

func (nopUnlocker) Lock()   { panic("should not be called") }
func (nopUnlocker) Unlock() {}
