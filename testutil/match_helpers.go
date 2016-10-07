package testutil

import (
	"bytes"

	. "github.com/onsi/gomega"
)

func ExpectBytesEqual(a, b []byte) {
	if !bytes.Equal(a, b) {
		Expect(a).To(Equal(b))
	}
}

// TODO: that is experiment. Remove before release if unused.
//func Match(m types.GomegaMatcher) butNot {
//	return butNotMatcher{GomegaMatcher: m}
//}
//
//func (m butNotMatcher) ButNot(butNot types.GomegaMatcher) types.GomegaMatcher {
//	m.butNot = butNot
//	return m
//}
//
//type butNot interface {
//	ButNot(types.GomegaMatcher) types.GomegaMatcher
//}
//
//type butNotMatcher struct {
//	types.GomegaMatcher
//	// butNot match means that there is no way to match main mather.
//	butNot types.GomegaMatcher
//}
//
//func (m butNotMatcher) MatchMayChangeInTheFuture(actual interface{}) bool {
//	butNotMatch, _ := m.butNot.Match(actual)
//	println(fmt.Sprint("butnot match", butNotMatch))
//	match, _ := m.Match(actual) // If match, than match already changed.
//	return match || !butNotMatch
//}
//
//var _ OracleMatcher = (butNotMatcher{})
//
//type OracleMatcher interface {
//	MatchMayChangeInTheFuture(actual interface{}) bool
//}
