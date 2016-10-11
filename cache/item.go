package cache

import "github.com/skipor/memcached/recycle"

type Item struct {
	ItemMeta
	Data *recycle.Data
}

type ItemMeta struct {
	Key     string
	Flags   uint32
	Exptime int64
	Bytes   int
}

func (m ItemMeta) expired(now int64) bool {
	return m.Exptime < now
}

func (i Item) NewView() ItemView {
	return ItemView{
		i.ItemMeta,
		i.Data.NewReader(),
	}
}

type ItemView struct {
	ItemMeta
	Reader *recycle.DataReader
}