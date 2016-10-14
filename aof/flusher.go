package aof

//go:generate mockery -name=flusher -inpkg -testonly

type flusher interface {
	Flush() error
}
type nopFlusher struct{}

func (nopFlusher) Flush() error { return nil }
