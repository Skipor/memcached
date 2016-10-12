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
	return newCache(l, conf)
}

func newCache(l log.Logger, conf Config) *cache {
	c := &cache{
		log:   l,
		table: make(map[string]*node),
		limits: limits{
			total: conf.Size,
			hot:   conf.Size * (hotCap * 100) / 100,
			warm:  conf.Size * (warmCap * 100) / 100,
		},
	}
	for i := 0; i < temps; i++ {
		lru := newLRU()
		lru.onExpire = c.onExpire
		c.lrus = append(c.lrus, lru)
	}
	c.hot().onActive = attachAsInactive
	c.warm().onActive = attachAsInactive
	c.cold().onActive = moveTo(c.warm())

	c.hot().onInactive = moveTo(c.cold())
	c.warm().onInactive = moveTo(c.cold())
	c.cold().onInactive = c.onEvict
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
	lrus   []*lru
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
	defer c.checkInvariants()
	now := nowUnix()
	expired := i.expired(now)
	if expired {
		c.log.Warn("Set expired item.")
	}
	n, ok := c.table[i.Key]
	var wasActive bool
	if ok {
		c.log.Debugf("Remove old item %s value.", i.Key)
		wasActive = n.isActive()
		n.detach()
		c.deleteDetached(n)
	}
	if expired {
		c.log.Warn("Skip add of expired item.")
		i.Data.Recycle()
		return
	}
	c.log.Debugf("Add item %s.", i.Key)
	n = newNode(i)
	c.table[i.Key] = n
	c.lrus[hot].push(n)
	if wasActive {
		n.active = active
	}

	if n.size() > c.limits.hot {
		// TODO do this check earlier
		c.log.Panic("Too large item. Size %v, limit %v", n.size(), c.limits.hot)
	}

	if c.hotOverflow() || c.totalOverflow() {
		// TODO do this in backgroud goroutine. That improves latency.
		c.fixOverflows()
	}

}

func (c *cache) Get(keys ...[]byte) (views []ItemView) {
	c.RLock()
	defer c.RUnlock()
	defer c.checkInvariants()
	c.log.Debug("get %v", keys)
	now := time.Now().Unix()
	for _, key := range keys {
		if n, ok := c.table[string(key)]; ok { // No allocation.
			if !n.expired(now) {
				n.setActive()
				views = append(views, n.NewView())
			}
		}
	}
	return
}

func (c *cache) Delete(key []byte) (deleted bool) {
	c.Lock()
	defer c.Unlock()
	defer c.checkInvariants()
	n, ok := c.table[string(key)] // No allocation.
	if !ok {
		return false
	}
	n.detach()
	c.deleteDetached(n)
	return true
}

func (c *cache) fixOverflows() {
	c.log.Debug("Fixing overflows")
	now := time.Now().Unix()
	if c.hotOverflow() {
		c.log.Debug("Hot overflow.")
		c.hot().shrink(c.limits.hot, now)
	}
	if !c.totalOverflow() {
		return
	}
	c.log.Debug("Total overflow.")
	c.cold().shrink(c.coldLimit(), now)

	if c.warmOverflow() {
		// Some active cold become warm now.
		c.log.Debug("Warm overflow.")
		c.warm().shrink(c.limits.warm, now)
	}

	if !c.totalOverflow() {
		return
	}
	c.log.Debug("Total overflow not fixed yet. Evict previous warm inactive items.")
	c.cold().shrink(c.coldLimit(), now)

	if c.totalOverflow() {
		panic("Overflow after cache eviction. Should not happen.")
	}
}

func (c *cache) onEvict(n *node) {
	c.log.Debugf("Item %s evicted.", n.Key)
	c.deleteDetached(n)
}

func (c *cache) onExpire(n *node) {
	c.log.Debugf("Item %s expired.", n.Key)
	c.deleteDetached(n)
}

// delete removes owned but detached node.
func (c *cache) deleteDetached(n *node) {
	n.disown()
	n.Data.Recycle()
	delete(c.table, string(n.Key))
	if tag.Debug {
		n.next = nil
		n.prev = nil
		n.owner = nil
		n.Data = nil
	}
}

func (c *cache) hot() *lru           { return c.lrus[hot] }
func (c *cache) warm() *lru          { return c.lrus[warm] }
func (c *cache) cold() *lru          { return c.lrus[cold] }
func (c *cache) free() int64         { return c.limits.total - c.size() }
func (c *cache) coldLimit() int64    { return c.cold().size + c.free() }
func (c *cache) hotOverflow() bool   { return c.hot().size > c.limits.hot }
func (c *cache) warmOverflow() bool  { return c.warm().size > c.limits.warm }
func (c *cache) totalOverflow() bool { return c.free() < 0 }

func (c *cache) itemsNum() int {
	return len(c.table)
}

func (c *cache) size() int64 {
	var size int64
	for i := range c.lrus {
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

func nowUnix() int64 {
	return time.Now().Unix()
}
