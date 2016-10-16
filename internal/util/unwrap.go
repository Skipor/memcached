package util

func Unwrap(err error) error {
	type hasUnderlying interface {
		Underlying() error
	}
	if eh, ok := err.(hasUnderlying); ok {
		return eh.Underlying()
	}
	return err
}
