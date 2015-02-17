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
	// Retry counts a retried action of type a with payload length l.
	Retry(t RetryType, a ActionType, l int)

	// Send counts a sent action of type a with payload length l.
	Send(a ActionType, l int)

	// SendTotal counts a bulk post of total size l. This includes
	// action metadata in contrast to Send.
	SendTotal(l int)

	// Timeout counts that a batch post timed out.
	Timeout()

	// BatchTime records the length of a batch request.
	BatchTime(t time.Duration)
}

// DefaultCounter implements Counter, not counting anything.
type DefaultCounter struct{}

// Retry is a default trivial implementation.
func (DefaultCounter) Retry(RetryType, ActionType, int) {
}

// Send is a default trivial implementation.
func (DefaultCounter) Send(ActionType, int) {
}

// SendTotal is a default trivial implementation.
func (DefaultCounter) SendTotal(int) {
}

// Timeout is a default trivial implementation.
func (DefaultCounter) Timeout() {
}

// BatchTime is a default trivial implementation.
func (DefaultCounter) BatchTime(time.Duration) {
}

var _ Counter = DefaultCounter{}

type val struct{ C, T int }

type count struct {
	Retries    val
	Sends      val
	SendTotals val
	Timeouts   int
}

func (c *count) Retry(_ RetryType, _ ActionType, l int) {
	c.Retries.C++
	c.Retries.T += l
}

func (c *count) Send(_ ActionType, l int) {
	c.Sends.C++
	c.Sends.T += l
}

func (c *count) SendTotal(l int) {
	c.SendTotals.C++
	c.SendTotals.T += l
}

func (c *count) Timeout() {
	c.Timeouts++
}

func (c *count) BatchTime(time.Duration) {
}

var _ Counter = &count{}
