package aof

type flusher interface {
	Flush() error
}
type nopFlusher struct{}

func (nopFlusher) Flush() error { return nil }
