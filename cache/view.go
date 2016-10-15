package cache

// View interface that usually wraps Cache with additional logic per operation.
// Operation log, for example. Interfaces returned by View methods must be used only once.
type View interface {
	// NewSetter returns setter.
	// Provided rawCommand CAN be invalidated after call.
	// Implementations should copy it if needed.
	NewSetter(rawCommand []byte) Setter
	// NewGetter returns getter.
	// Provided rawCommand MUST NOT be invalidated Getter.Get call.
	NewGetter(rawCommand []byte) Getter
	// NewDeleter returns deleter.
	// Provided rawCommand MUST NOT be invalidated Deleter.Delete call.
	NewDeleter(rawCommand []byte) Deleter
}

type Viewable interface {
	Cache
	View
}

type Getter interface {
	Get(key ...[]byte) (views []ItemView)
}
type Setter interface {
	Set(i Item)
}
type Deleter interface {
	Delete(key []byte) (deleted bool)
}

func (c *LRU) NewGetter(rawCommand []byte) Getter   { return c }
func (c *LRU) NewSetter(rawCommand []byte) Setter   { return c }
func (c *LRU) NewDeleter(rawCommand []byte) Deleter { return c }

var _ View = (*LRU)(nil)
