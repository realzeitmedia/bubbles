package bubbles

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

var _ Counter = DefaultCounter{}

// Count implements Counter, just counting to exported fields.
type Count struct {
	Retries    int
	Sends      int
	SendTotals int
	Timeouts   int
}

// Retry increments c.Retries.
func (c *Count) Retry(RetryType, ActionType, int) {
	c.Retries++
}

// Send increments c.Sends.
func (c *Count) Send(ActionType, int) {
	c.Sends++
}

// SendTotal increments c.SendTotals.
func (c *Count) SendTotal(int) {
	c.SendTotals++
}

// Timeout increments c.Timeouts.
func (c *Count) Timeout() {
	c.Timeouts++
}

var _ Counter = &Count{}