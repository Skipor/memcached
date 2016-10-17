package memcached

import (
	"github.com/Skipor/memcached/aof"
	"github.com/Skipor/memcached/cache"
)

type logginCacheViewFabric struct {
	c   cache.RWCache
	aof *aof.AOF
}

func (f logginCacheViewFabric) New() cache.View {
	return newLoggingCacheView(f.c, f.aof)
}

func newLoggingCacheView(c cache.RWCache, aof *aof.AOF) *loggingCacheView {
	return &loggingCacheView{
		cache: c,
		aof:   aof,
	}
}

// LoggingCacheView implement AOF logging for memcached server.
// LoggingView is thread unsafe, but it is very lightweight and can be made for every connection.
// Logging error cause panic. In such case there is no guarantee of locks release,
// and program should finish execution.
//
// General schema of operations:
// 1) Acquire cache lock.
// 2) Make cache operation.
// 3) Acquire log lock.
// 4) Release cache lock.
// 5) Do logging.
// 6) Release log lock.
// Acquiring log lock  with acquired cache lock guarantees that all
// cache modifying operations (operations that requires write lock)
// will be logged in same order as applied to cache.
// Releasing cache lock before logging allows apply another cache operation
// while logging in process.
type loggingCacheView struct {
	cache   cache.RWCache
	aof     *aof.AOF
	rawCopy []byte // rawCopy is buffer for data which should be copied.
}

var _ cache.View = (*loggingCacheView)(nil)

func (v *loggingCacheView) NewGetter(raw []byte) cache.Getter {
	return &lcvOperation{
		loggingCacheView: v,
		raw:              raw,
	}
}

func (v *loggingCacheView) NewSetter(raw []byte) cache.Setter {
	if v.rawCopy == nil {
		v.rawCopy = make([]byte, 0, len(raw))
	}
	v.rawCopy = append(v.rawCopy[:0], raw...)
	o := &lcvOperation{
		loggingCacheView: v,
		raw:              v.rawCopy,
	}
	return o
}

func (v *loggingCacheView) NewDeleter(raw []byte) cache.Deleter {
	return &lcvOperation{
		loggingCacheView: v,
		raw:              raw,
	}
}

type lcvOperation struct {
	*loggingCacheView
	raw []byte
}

func assertNoErr(err error) {
	if err != nil {
		panic(err)
	}
}

func (o *lcvOperation) Get(keys ...[]byte) (views []cache.ItemView) {
	o.cache.RLock()
	views = o.cache.Get(keys...)

	t := o.aof.NewTransaction()
	o.cache.RUnlock()

	_, err := t.Write(o.raw)
	assertNoErr(err)

	err = t.Close()
	assertNoErr(err)

	// One use only.
	o.raw = nil
	o.loggingCacheView = nil
	return
}

func (o *lcvOperation) Set(i cache.Item) {
	itemReader := i.Data.NewReader()

	o.cache.Lock()
	o.cache.Set(i)
	t := o.aof.NewTransaction()
	o.cache.Unlock()

	_, err := t.Write(o.raw)
	assertNoErr(err)

	_, err = itemReader.WriteTo(t)
	assertNoErr(err)

	_, err = t.Write(separatorBytes)
	assertNoErr(err)

	err = t.Close()
	assertNoErr(err)

	itemReader.Close()
	o.raw = nil
	o.loggingCacheView = nil
	return

}
func (o *lcvOperation) Delete(key []byte) (deleted bool) {
	o.cache.Lock()
	deleted = o.cache.Delete(key)
	t := o.aof.NewTransaction()
	o.cache.Unlock()

	_, err := t.Write(o.raw)
	assertNoErr(err)

	err = t.Close()
	assertNoErr(err)

	o.raw = nil
	o.loggingCacheView = nil
	return
}
