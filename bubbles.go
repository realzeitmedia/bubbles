package bubbles

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

type bubbles struct {
	q      chan *Action
	retryQ chan *Action
	error  chan *Action
	quit   chan struct{}
	wg     sync.WaitGroup
}

type MetaData struct {
	Index string `json:"_index"`
	Type  string `json:"_type"`
	ID    string `json:"_id"`
	// TODO: the rest of 'm
}

type BulkRes struct {
	Took   int `json:"took"`
	Items  []map[string]BulkResStatus
	Errors bool
}
type BulkResStatus struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Status  int    `json:"status"`
	Error   string `json:"error"`
}

func New(addrs []string, cpc int, flushTimeout time.Duration) (*bubbles, error) {
	b := bubbles{
		q:      make(chan *Action),
		retryQ: make(chan *Action, len(addrs)*cpc),
		error:  make(chan *Action, 1),
		quit:   make(chan struct{}),
	}
	for _, a := range addrs {
		for i := 0; i < cpc; i++ {
			b.wg.Add(1)
			go func(a string) {
				client(&b, a)
				b.wg.Done()
			}(a)
		}
	}
	return &b, nil
}

// Errors returns the channel for actions which errored.
func (b *bubbles) Errors() <-chan *Action {
	return b.error
}

// Enqueue queues a single action in a routine. It will block if all bulk
// processers are busy.
func (b bubbles) Enqueue(a *Action) {
	b.q <- a
}

// Stop shuts down all ES clients. It'll return all Action entries which were
// not yet processed.
func (b *bubbles) Stop() []*Action {
	close(b.quit)
	// TODO: timeout
	b.wg.Wait()
	// Return elements which gave errors.
	close(b.retryQ) // after Wait()!
	close(b.q)      // after Wait()!
	pending := make([]*Action, 0, len(b.retryQ)+len(b.q))
	for a := range b.q {
		pending = append(pending, a)
	}
	for a := range b.retryQ {
		pending = append(pending, a)
	}
	return pending
}

// client talks to ES. This runs in a go routine in a loop and deals with a
// single ES address.
func client(b *bubbles, addr string) {
	url := fmt.Sprintf("http://%s/_bulk", addr) // TODO: https?
	// fmt.Printf("starting client to %s\n", addr)
	// defer fmt.Printf("stopping client to %s\n", addr)

	cl := http.Client{
		Transport: &http.Transport{
			DisableCompression: true, // TODO: sure?
		},
		CheckRedirect: noRedirect,
	}

	for {
		select {
		case <-b.quit:
			return
		default:
		}
		runBatch(b, cl, url)
	}

}

func runBatch(b *bubbles, cl http.Client, url string) {
	maxDocumentCount := 10
	actions := make([]*Action, 0, maxDocumentCount)
	var t <-chan time.Time
	// First try the ones to retry.
retry:
	for len(actions) < maxDocumentCount {
		select {
		case a := <-b.retryQ:
			actions = append(actions, a)
			if t == nil {
				t = time.After(10 * time.Millisecond)
			}
		default:
			// no more retry ones
			break retry
		}
	}

gather:
	for len(actions) < maxDocumentCount {
		select {
		case <-b.quit:
			for _, a := range actions {
				b.retryQ <- a
			}
			return
		case <-t:
			// case not enabled until we've read an action.
			break gather
		case a := <-b.retryQ:
			actions = append(actions, a)
			if t == nil {
				t = time.After(10 * time.Millisecond)
			}
		case a := <-b.q:
			actions = append(actions, a)
			// Set timeout on the first element we read
			if t == nil {
				t = time.After(10 * time.Millisecond)
			}
		}
	}
	if len(actions) == 0 {
		// no actions. Weird.
		return
	}

	for retry := 3; retry > 0; retry-- {
		res, err := postActions(cl, url, actions)
		if err != nil {
			select {
			case <-b.quit:
				for _, a := range actions {
					b.retryQ <- a
				}
				return
			default:
			}
			// Go again
			// fmt.Printf("major error: %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		// fmt.Printf("ES res: %v\n", res)
		// Server accepted this.
		if !res.Errors {
			// Simple case, no errors.
			return
		}
		// Figure out which ones have errors.
		for i, e := range res.Items {
			a := actions[i] // TODO: sanity check
			el, ok := e[string(a.Type)]
			if !ok {
				// TODO: this
				fmt.Printf("Non matching action!\n")
				continue
			}

			c := el.Status
			switch {
			case c >= 200 && c < 300:
				// Document accepted by ES.
			case c >= 400 && c < 500:
				// Some error. Nothing we can do with it.
				b.error <- a
			case c >= 500 && c < 600:
				// Server error. Retry it.
				b.retryQ <- a
			default:
				// No idea.
				fmt.Printf("unexpected status: %d. Ignoring document.\n", c)
			}
		}
		return
	}
	// Giving up on the batch. All actions should be re-tried.
	for _, a := range actions {
		b.retryQ <- a
	}
}

func postActions(cl http.Client, url string, actions []*Action) (*BulkRes, error) {
	// TODO: bytestring as argument
	// TODO: don't chunk.
	// TODO: timeout
	buf := bytes.Buffer{}
	for _, a := range actions {
		buf.Write(a.Buf())
	}
	// fmt.Printf("buffer to post to %s:\n<<<\n%s\n<<<\n", url, buf.String())
	resp, err := cl.Post(url, "application/x-www-form-urlencoded", &buf)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// fmt.Printf("all accepted by ES\n")
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var bulk BulkRes
	if err := json.Unmarshal(body, &bulk); err != nil {
		return nil, err
	}
	return &bulk, nil
}

func noRedirect(req *http.Request, via []*http.Request) error {
	return errors.New("no redirect")
}
