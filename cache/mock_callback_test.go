package cache

import (
	. "github.com/onsi/ginkgo"
	"github.com/stretchr/testify/mock"
)

type MockCallback struct {
	mock.Mock
}

func (m *MockCallback) Expire(n *node) {
	By("Expire " + n.Key)
	n.disown()
	m.Called(n)
}

func (m *MockCallback) Evict(n *node) {
	By("Evict " + n.Key)
	n.disown()
	m.Called(n)
}

func (m *MockCallback) AttachAsInactive(n *node) {
	By("AttachAsInactive " + n.Key)
	attachAsInactive(n)
	m.Called(n)
}

func (m *MockCallback) MoveTo(q *queue) func(*node) {
	return func(n *node) {
		moveTo(q)(n)
		m.Moved(n)
	}
}

func (m *MockCallback) Moved(n *node) {
	By("Moved " + n.Key)
	m.Called(n)
}
