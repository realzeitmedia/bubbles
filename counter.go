package bubbles

import (
	"time"
)

// RetryType distinguishes between reasons for retrying an action.
type RetryType int

const (
	// RetryUnlikely is if the action is retried for reason that should not occur.
	RetryUnlikely = iota

	// RetryTransient is if the action is retried for a transient error, e.g. full queue.
	RetryTransient
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

type val struct{ C, T int }

type count struct {
	Sends      int
	Retries    int
	Errors     int
	SendTotals val
	Troubles   int
}

func (c *count) Actions(s, r, e int) {
	c.Sends += s
	c.Retries += r
	c.Errors += e
}

func (c *count) SendTotal(l int) {
	c.SendTotals.C++
	c.SendTotals.T += l
}

func (c *count) Trouble() {
	c.Troubles++
}

func (c *count) BatchTime(time.Duration) {
}

var _ Counter = &count{}
