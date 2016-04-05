// +build live

package bubbles

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	addr  = "localhost" // no port, default is :9200
	index = "bubbles"
)

func cleanES(t *testing.T) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s:9200/%s", addr, index), nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if !(res.StatusCode == 200 || res.StatusCode == 404) {
		t.Fatal(fmt.Sprintf("DELETE err: %d", res.StatusCode))
	}
	res.Body.Close()
}

func TestLiveIndex(t *testing.T) {
	cleanES(t)
	b := New([]string{addr}, OptFlush(10*time.Millisecond))

	ins := Action{
		Type: Index,
		MetaData: MetaData{
			Index: index,
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}

	b.Enqueue() <- ins
	time.Sleep(500 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}

func TestLiveIndexError(t *testing.T) {
	cleanES(t)
	// Index with errors.

	errs := NewTestErrs(t)
	b := New([]string{addr},
		OptFlush(100*time.Millisecond),
		OptServerTimeout(3*time.Second),
		OptErrer(errs),
	)

	ins1 := Action{
		Type: Index,
		MetaData: MetaData{
			Index: index,
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}
	ins2 := Action{
		Type: Index,
		MetaData: MetaData{
			Index: index,
			Type:  "type1",
			ID:    "2",
		},
		Document: `{"field1": `, // <-- invalid!
	}

	b.Enqueue() <- ins1
	b.Enqueue() <- ins2
	time.Sleep(500 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}

	// ins2 has a fatal error and should be reported via the error cb.
	errored := <-errs.Errors
	if have, want := errored.Action, ins2; have != want {
		t.Fatalf("have %v, want %v", have, want)
	}
	if have, want := errored.Elasticsearch.Type, "mapper_parsing_exception"; have != want {
		t.Fatalf("have %q, want %q", have, want)
	}
	if have, want := errored.Error(), "http://localhost:9200/_bulk index status 400: mapper_parsing_exception failed to parse"; have != want {
		t.Fatalf("have %q, want %q", have, want)
	}
	close(errs.Errors)
	// That should have been our only error.
	if _, ok := <-errs.Errors; ok {
		t.Fatalf("error channel should have been closed")
	}
}

func TestLiveMany(t *testing.T) {
	cleanES(t)

	var (
		b = New([]string{addr},
			OptFlush(10*time.Millisecond),
		)
		clients   = 10
		documents = 10000
	)

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < documents/clients; j++ {
				b.Enqueue() <- Action{
					Type: Create,
					MetaData: MetaData{
						Index: index,
						Type:  "type1",
					},
					Document: `{"field1": "value1"}`,
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	time.Sleep(200 * time.Millisecond)

	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}

	// Wait for ES to index our documents.
	done := make(chan struct{}, 1)
	go func() {
		for {
			if getDocCount() >= documents {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for documents")
	}
	if have, want := getDocCount(), documents; have != want {
		t.Fatalf("have %d, want %d", have, want)
	}
}

func getDocCount() int {
	// _cat/indices gives a line such as:
	// 'yellow open bubbles 5 1 10001 0 314.8kb 314.8kb\n'
	res, err := http.Get(fmt.Sprintf("http://%s:9200/_cat/indices/%s", addr, index))
	if err != nil {
		panic(err)
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("res: %d - %s\n", res.StatusCode, string(body))
	cols := strings.Fields(string(body))
	docCount, err := strconv.Atoi(cols[5])
	if err != nil {
		panic(err)
	}
	return docCount
}
