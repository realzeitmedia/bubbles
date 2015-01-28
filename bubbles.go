package bubbles

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	// DefaultMaxDocumentsPerBatch is the number of documents a batch needs to
	// have before it is send. This is per connection.
	DefaultMaxDocumentsPerBatch = 1000

	// DefaultFlushTimeout is the maximum time we batch something before we try
	// to send it to a server.
	DefaultFlushTimeout = 10 * time.Second

	// DefaultServerTimeout is the time we give ElasticSearch to respond. This
	// is also the maximum time Stop() will take.
	DefaultServerTimeout = 10 * time.Second

	// DefaultConnCount is the number of connections per hosts.
	DefaultConnCount = 2

	serverErrorWait    = 500 * time.Millisecond
	serverErrorWaitMax = 16 * time.Second

	defaultElasticSearchPort = "9200"
)

var (
	errInvalidResponse = errors.New("invalid response")
)

// Bubbles is the main struct to control a queue of Actions going to the
// ElasticSearch servers.
type Bubbles struct {
	q                chan Action
	retryQ           chan Action
	quit             chan struct{}
	wg               sync.WaitGroup
	maxDocumentCount int
	connCount        int
	flushTimeout     time.Duration
	serverTimeout    time.Duration
	c                Counter
	e                Errer
}

// Opt is any option to New().
type Opt func(*Bubbles)

// OptConnCount is an option to New() to specify the number of connections per
// host. The default is DefaultConnCount.
func OptConnCount(n int) Opt {
	return func(b *Bubbles) {
		b.connCount = n
	}
}

// OptFlush is an option to New() to specify the flush timeout of a batch. The
// default is DefaultFlushTimeout.
func OptFlush(d time.Duration) Opt {
	return func(b *Bubbles) {
		b.flushTimeout = d
	}
}

// OptServerTimeout is an option to New() to specify the timeout of a single
// batch POST to ElasticSearch. This value is also the maximum time Stop() will
// take. All actions in a bulk which is timed out will be retried. The default
// is DefaultServerTimeout.
func OptServerTimeout(d time.Duration) Opt {
	return func(b *Bubbles) {
		b.serverTimeout = d
	}
}

// OptMaxDocs is an option to New() to specify maximum number of documents in a
// single batch. The default is DefaultMaxDocumentsPerBatch.
func OptMaxDocs(n int) Opt {
	return func(b *Bubbles) {
		b.maxDocumentCount = n
	}
}

// OptCounter is an option to New() to specify something that counts documents.
func OptCounter(c Counter) Opt {
	return func(b *Bubbles) {
		b.c = c
	}
}

// OptErrer is an option to New() to specify an error handler. The default
// handler uses the log module.
func OptErrer(e Errer) Opt {
	return func(b *Bubbles) {
		b.e = e
	}
}

// New makes a new ElasticSearch bulk inserter. It needs a list with 'ip' or
// 'ip:port' addresses, options are added via the Opt* functions.
func New(addrs []string, opts ...Opt) *Bubbles {
	b := Bubbles{
		q:                make(chan Action),
		quit:             make(chan struct{}),
		maxDocumentCount: DefaultMaxDocumentsPerBatch,
		connCount:        DefaultConnCount,
		flushTimeout:     DefaultFlushTimeout,
		serverTimeout:    DefaultServerTimeout,
		c:                DefaultCounter{},
		e:                DefaultErrer{},
	}
	for _, o := range opts {
		o(&b)
	}
	b.retryQ = make(chan Action, len(addrs)*b.connCount*b.maxDocumentCount)

	// Start a go routine per connection per host
	for _, a := range addrs {
		addr := withPort(a, defaultElasticSearchPort)
		for i := 0; i < b.connCount; i++ {
			b.wg.Add(1)
			go func(a string) {
				client(&b, a)
				b.wg.Done()
			}(addr)
		}
	}
	return &b
}

// Enqueue returns the queue to add Actions in a routine. It will block if all bulk
// processors are busy.
func (b *Bubbles) Enqueue() chan<- Action {
	return b.q
}

// Stop shuts down all ElasticSearch clients. It'll return all Action entries
// which were not yet processed, or were up for a retry. It can take
// OptServerTimeout to complete.
func (b *Bubbles) Stop() []Action {
	close(b.quit)
	// There is no explicit timeout, we rely on b.serverTimeout to shut down
	// everything.
	b.wg.Wait()

	// Collect and return elements which are in flight.
	close(b.retryQ)
	close(b.q)
	pending := make([]Action, 0, len(b.q)+len(b.retryQ))
	for a := range b.q {
		pending = append(pending, a)
	}
	for a := range b.retryQ {
		pending = append(pending, a)
	}
	return pending
}

// client talks to ElasticSearch. This runs in a go routine in a loop and deals
// with a single ElasticSearch address.
func client(b *Bubbles, addr string) {
	url := fmt.Sprintf("http://%s/_bulk", addr)

	cl := http.Client{
		Timeout: b.serverTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return errors.New("no redirect")
		},
	}

	wait := 0 * time.Second
	batchSize := b.maxDocumentCount
	for {
		select {
		case <-b.quit:
			return
		case <-time.After(wait):
		}
		switch runBatch(b, cl, url, batchSize) {
		case noTrouble:
			wait = 0 * time.Second
			batchSize = b.maxDocumentCount
			continue
		case littleTrouble:
		case bigTrouble:
			batchSize /= 2
			if batchSize < 1 {
				batchSize = 1
			}
		}
		if wait == 0 {
			wait = serverErrorWait
		} else {
			wait *= 2
			if wait >= serverErrorWaitMax {
				wait = serverErrorWaitMax
			}
		}
		b.c.Timeout()
	}
}

type trouble int

const (
	noTrouble trouble = iota
	littleTrouble
	bigTrouble
)

// runBatch gathers and deals with a batch of actions. It returns
// whether there was trouble.
func runBatch(b *Bubbles, cl http.Client, url string, batchSize int) trouble {
	actions := make([]Action, 0, b.maxDocumentCount)
	// First use all retry actions.
retry:
	for len(actions) < batchSize {
		select {
		case a := <-b.retryQ:
			actions = append(actions, a)
		default:
			// no more retry actions queued
			break retry
		}
	}

	var t <-chan time.Time
gather:
	for len(actions) < batchSize {
		if t == nil && len(actions) > 0 {
			// Set timeout on the first element we read
			t = time.After(b.flushTimeout)
		}
		select {
		case <-b.quit:
			for _, a := range actions {
				b.retryQ <- a
			}
			return noTrouble
		case <-t:
			// this case is not enabled until we've got an action
			break gather
		case a := <-b.retryQ:
			actions = append(actions, a)
		case a := <-b.q:
			actions = append(actions, a)
		}
	}

	res, err := postActions(b.c, cl, url, actions)
	if err != nil {
		// A server error. Retry these actions later.
		b.e.Error(err)
		for _, a := range actions {
			b.c.Retry(RetryUnlikely, a.Type, len(a.Document))
			b.retryQ <- a
		}
		return bigTrouble
	}

	// Server has accepted the request an sich, but there can be errors in the
	// individual actions.
	if !res.Errors {
		// Simple case, no errors present.
		return noTrouble
	}

	// Invalid response from ElasticSearch.
	if len(actions) != len(res.Items) {
		b.e.Error(errInvalidResponse)
		for _, a := range actions {
			b.c.Retry(RetryUnlikely, a.Type, len(a.Document))
			b.retryQ <- a
		}
		return bigTrouble
	}
	// Figure out which actions have errors.
	for i, e := range res.Items {
		a := actions[i]
		el, ok := e[string(a.Type)]
		if !ok {
			// Unexpected reply from ElasticSearch.
			b.e.Error(errInvalidResponse)
			b.c.Retry(RetryUnlikely, a.Type, len(a.Document))
			b.retryQ <- a
			continue
		}

		c := el.Status
		switch {
		case c >= 200 && c < 300:
			// Document accepted by ElasticSearch.
		case c == 429 || (c >= 500 && c < 600):
			// Server error. Retry it.
			// We get a 429 when the bulk queue is full, which we just retry as
			// well.
			b.e.Warning(ActionError{
				Action:     a,
				StatusCode: c,
				Msg:        fmt.Sprintf("transient error %d: %s", c, el.Error),
				Server:     url,
			})
			b.c.Retry(RetryTransient, a.Type, len(a.Document))
			b.retryQ <- a
		case c >= 400 && c < 500:
			// Some error. Nothing we can do with it.
			b.e.Error(ActionError{
				Action:     a,
				StatusCode: c,
				Msg:        fmt.Sprintf("error %d: %s", c, el.Error),
				Server:     url,
			})
		default:
			// No idea.
			b.e.Error(fmt.Errorf("unwelcome response %d: %s", c, el.Error))
		}
	}
	return littleTrouble
}

type bulkRes struct {
	Took   int  `json:"took"`
	Errors bool `json:"errors"`
	Items  []map[string]struct {
		Index   string `json:"_index"`
		Type    string `json:"_type"`
		ID      string `json:"_id"`
		Version int    `json:"_version"`
		Status  int    `json:"status"`
		Error   string `json:"error"`
	} `json:"items"`
}

func postActions(c Counter, cl http.Client, url string, actions []Action) (*bulkRes, error) {
	buf := bytes.Buffer{}
	for _, a := range actions {
		c.Send(a.Type, len(a.Document))
		buf.Write(a.Buf())
	}
	c.SendTotal(buf.Len())

	// This doesn't Chunk.
	resp, err := cl.Post(url, "application/json", &buf)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var bulk bulkRes
	if err := json.Unmarshal(body, &bulk); err != nil {
		return nil, err
	}
	return &bulk, nil
}

// ActionError wraps an Action we won't retry. It implements the error interface.
type ActionError struct {
	Action     Action
	StatusCode int
	Msg        string
	Server     string
}

func (e ActionError) Error() string {
	return fmt.Sprintf("%s: %s %s", e.Server, e.Action.Type, e.Msg)
}

// withPort adds a default port to an address string.
func withPort(a, port string) string {
	if _, _, err := net.SplitHostPort(a); err != nil {
		// no port found.
		return net.JoinHostPort(a, port)
	}
	return a
}
