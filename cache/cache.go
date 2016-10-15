package cache

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/skipor/memcached/internal/tag"
	"github.com/skipor/memcached/log"
)

type temp uint8

const (
	cold temp = iota
	warm
	hot

	temps = 3

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

type Viewable interface {
	Cache
	View
}

type Config struct {
	Size int64
}

func NewCache(l log.Logger, conf Config) *LRU {
	c := &LRU{
		log:   l,
		table: make(map[string]*node),
		limits: limits{
			total: conf.Size,
			hot:   conf.Size * (hotCap * 100) / 100,
			warm:  conf.Size * (warmCap * 100) / 100,
		},
	}
	for i := 0; i < temps; i++ {
		queue := newQueue()
		queue.onExpire = c.onExpire
		c.queues = append(c.queues, queue)
	}
	c.hot().onActive = attachAsInactive
	c.warm().onActive = attachAsInactive
	c.cold().onActive = moveTo(c.warm())

	c.hot().onInactive = moveTo(c.cold())
	c.warm().onInactive = moveTo(c.cold())
	c.cold().onInactive = c.onEvict
	return c
}

type LRU struct {
	sync.RWMutex
	table  map[string]*node
	queues []*queue
	limits limits
	log    log.Logger
}

type limits struct {
	total int64
	hot   int64
	warm  int64
}

var _ Cache = (*LRU)(nil)

func (c *LRU) Set(i Item) {
	c.Lock()
	defer c.Unlock()
	c.set(i)
}

func (c *LRU) set(i Item) {
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
	c.queues[hot].push(n)
	if wasActive {
		n.active = active
	}

	if n.size() > c.limits.hot {
		c.log.Panic("Too large item. Size %v, limit %v", n.size(), c.limits.hot)
	}

	if c.hotOverflow() || c.totalOverflow() {
		// TODO do this in background goroutine. That improves latency.
		c.fixOverflows()
	}

}

func (c *LRU) Get(keys ...[]byte) (views []ItemView) {
	c.RLock()
	defer c.RUnlock()
	return c.get(keys...)
}

func (c *LRU) get(keys ...[]byte) (views []ItemView) {
	c.log.Debugf("get %s", keysPrinter{keys})
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

func (c *LRU) Touch(keys ...[]byte) {
	c.RLock()
	defer c.RUnlock()
	c.touch(keys...)
}

func (c *LRU) touch(keys ...[]byte) {
	c.log.Debugf("touch %s", keysPrinter{keys})
	for _, key := range keys {
		if n, ok := c.table[string(key)]; ok { // No allocation.
			n.setActive()
		}
	}
	return
}

func (c *LRU) Delete(key []byte) (deleted bool) {
	c.Lock()
	defer c.Unlock()
	return c.delete(key)
}

func (c *LRU) delete(key []byte) (deleted bool) {
	defer c.checkInvariants()
	n, ok := c.table[string(key)] // No allocation.
	if !ok {
		return false
	}
	n.detach()
	c.deleteDetached(n)
	return true
}

func (c *LRU) fixOverflows() {
	c.log.Debug("Fixing overflows")
	now := time.Now().Unix()
	if c.hotOverflow() {
		c.log.Debug("Hot overflow.")
		c.hot().shrinkWhile(c.hotOverflow, now)
	}
	if !c.totalOverflow() {
		return
	}
	c.log.Debug("Total overflow.")
	c.cold().shrinkWhile(func() bool {
		return !c.cold().empty() && c.totalOverflow()
	}, now)

	if c.warmOverflow() {
		// Some active cold become warm now.
		c.log.Debug("Warm overflow.")
		c.warm().shrinkWhile(c.warmOverflow, now)
	}

	if !c.totalOverflow() {
		return
	}
	c.log.Debug("Total overflow not fixed yet. Evict previous warm inactive items.")
	c.cold().shrinkWhile(c.totalOverflow, now)

	if c.totalOverflow() {
		panic("Overflow after cache eviction. Should not happen.")
	}
}

func (c *LRU) onEvict(n *node) {
	c.log.Debugf("Item %s evicted.", n.Key)
	c.deleteDetached(n)
}

func (c *LRU) onExpire(n *node) {
	c.log.Debugf("Item %s expired.", n.Key)
	c.deleteDetached(n)
}

// delete removes owned but detached node.
func (c *LRU) deleteDetached(n *node) {
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

func (c *LRU) hot() *queue  { return c.queues[hot] }
func (c *LRU) warm() *queue { return c.queues[warm] }
func (c *LRU) cold() *queue { return c.queues[cold] }
func (c *LRU) free() int64  { return c.limits.total - c.size() }

func (c *LRU) hotOverflow() bool   { return c.hot().size > c.limits.hot }
func (c *LRU) warmOverflow() bool  { return c.warm().size > c.limits.warm }
func (c *LRU) totalOverflow() bool { return c.free() < 0 }

func (c *LRU) itemsNum() int {
	return len(c.table)
}

func (c *LRU) size() int64 {
	var size int64
	for i := range c.queues {
		size += c.queues[i].size
	}
	return size
}

type keysPrinter struct{ keys [][]byte }

func (p keysPrinter) String() string {
	buf := &bytes.Buffer{}
	for _, k := range p.keys {
		buf.WriteString(fmt.Sprintf(" %q", k))
	}
	return buf.String()
}

func nowUnix() int64 {
	return time.Now().Unix()
}
