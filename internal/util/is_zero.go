package util

import "reflect"

func IsZero(i interface{}) bool {
	return IsZeroVal(reflect.ValueOf(i))
}

func IsZeroVal(v reflect.Value) bool {
	return v.Interface() == reflect.Zero(v.Type()).Interface()
}
