package loges

import (
	"time"
)

// Counter provides hooks to count documents going through bubbles.
type Counter interface {
	// Counts the numbers of individual actions that were sent
	// successfully, retried or dropped due to error.
	Actions(send, retry, error int)

	// SendTotal counts a bulk post of total size l. This includes
	// action metadata in contrast to Send.
	SendTotal(l int)

	// Trouble counts that a batch post had problems.
	Trouble()

	// BatchTime records the length of a batch request.
	BatchTime(t time.Duration)
}

// DefaultCounter implements Counter, not counting anything.
type DefaultCounter struct{}

// Actions is a default trivial implementation.
func (DefaultCounter) Actions(int, int, int) {
}

// SendTotal is a default trivial implementation.
func (DefaultCounter) SendTotal(int) {
}

// Trouble is a default trivial implementation.
func (DefaultCounter) Trouble() {
}

// BatchTime is a default trivial implementation.
func (DefaultCounter) BatchTime(time.Duration) {
}

var _ Counter = DefaultCounter{}
