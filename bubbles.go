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

type ActionType int

const (
	Index ActionType = iota
	Create
	Delete
	Update
)

type Action struct {
	Type     ActionType
	Retry    int
	MetaData MetaData
	Document string // without any \n! // TODO: []byte ?
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

// Buf returns the command as ES buffer
func (a *Action) Buf() []byte {
	switch a.Type {
	default:
		panic("what's this?")
	case Index:
		md, err := json.Marshal(a.MetaData)
		if err != nil {
			panic(err.Error())
		}
		return []byte(fmt.Sprintf("{\"%s\": %s}\n%s\n", a.Type.String(), md, a.Document))
	}
}

// String returns the ES version of the action.
func (a ActionType) String() string {
	switch a {
	default:
		panic("what's this?")
	case Index:
		return "index"
	}
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

// Errors returns the channel for actions which errored, but have no retries
// left.
func (b *bubbles) Errors() <-chan *Action {
	return b.error
}

// Enqueue queues a single action in a routine. It will block if all bulk
// processers are busy.
func (b bubbles) Enqueue(a *Action) {
	b.q <- a
}

// retry is called by client() if an action didn't arrive at the server.
func (b *bubbles) retry(a *Action) {
	if a.Retry < 0 {
		// fmt.Printf("erroring an action\n")
		b.error <- a
		return
	}
	// fmt.Printf("requeueing an action\n")
	b.retryQ <- a // never blocks.
}

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

// client talks to ES. This runs in a go routine and deals with a single ES
// address.
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
		toRetry := runBatch(b, cl, url)
		for _, a := range toRetry {
			// fmt.Printf("do retry: %v\n", a)
			b.retry(a)
		}
	}

}

func runBatch(b *bubbles, cl http.Client, url string) []*Action {
	maxDocumentCount := 10
	actions := make([]*Action, 0, maxDocumentCount)
	var t <-chan time.Time
	// First try the ones to retry.
prio:
	for len(actions) < maxDocumentCount {
		select {
		case a := <-b.retryQ:
			actions = append(actions, a)
			if t == nil {
				t = time.After(10 * time.Millisecond)
			}
		default:
			// no more prio ones
			break prio
		}
	}

gather:
	for len(actions) < maxDocumentCount {
		select {
		case <-b.quit:
			return actions
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
		// fmt.Printf("no actions. Weird.\n")
		return nil
	}

	for retry := 3; retry > 0; retry-- {
		res, err := postActions(cl, url, actions)
		if err != nil {
			// TODO: on a global 400 decr retry counts for all actions.
			select {
			case <-b.quit:
				// fmt.Printf("quit post\n")
				return actions
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
			return nil
		}
		// Figure out which ones have errors.
		goAgain := []*Action{}
		for i, e := range res.Items {
			a := actions[i] // TODO: sanity check
			el, ok := e[a.Type.String()]
			if !ok {
				// TODO: this
				fmt.Printf("Non matching action!\n")
				continue
			}
			if el.Status == 200 {
				continue
			}
			// fmt.Printf("action error: %v\n", el.Error)
			// TODO: do something with el.Error
			a.Retry--
			goAgain = append(goAgain, a)
		}
		return goAgain
	}
	fmt.Printf("giving up on a batch\n")
	// All actions should be tried.
	return actions
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

	// fmt.Printf("res body: %s\n", string(body))
	var bulk BulkRes
	if err := json.Unmarshal(body, &bulk); err != nil {
		return nil, err
	}
	return &bulk, nil
}

func noRedirect(req *http.Request, via []*http.Request) error {
	return errors.New("no redirect")
}
