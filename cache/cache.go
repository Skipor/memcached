package cache

// Handler implementation must not retain key slices.
type Cache interface {
	Set(i Item)
	// Get returns ItemReaders for keys that was found in cache.
	// Readers can be nil, if no key was found.
	Get(key ...[]byte) (readers []ItemView)
	Delete(key []byte) (deleted bool)
}

func NewHandler() {
	//pool := recycle.NewPool()
	//if pool.MaxChunkSize() < MaxCommandSize {
	//	// Required for zero copy read large enough item data.
	//	panic("max chunk size should not be less than input buffer")
	//}
}
