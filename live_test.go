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
	addr  = "localhost:9200"
	index = "bubbles"
)

func TestLiveIndex(t *testing.T) {
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
	time.Sleep(100 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}

func TestLiveIndexError(t *testing.T) {
	// Index with errors.

	errs := make(chan ActionError)
	b := New([]string{addr},
		OptFlush(10*time.Millisecond),
		OptError(func(e ActionError) { errs <- e }),
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
	time.Sleep(100 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}

	// ins2 has a fatal error and should be reported via the error cb.
	errored := <-errs
	if have, want := errored.Action, ins2; have != want {
		t.Fatalf("have %v, want %v", have, want)
	}
	// Check the error message. The last part has some pointers in there so we
	// can't check that.
	want := `http://` + addr + `/_bulk: error 400: MapperParsingException[failed to parse]; nested: JsonParseException` // &c.
	have := errored.Error()
	if !strings.HasPrefix(have, want) {
		t.Fatalf("have %s, want %s", have, want)
	}
	// That should have been our only error.
	if _, ok := <-errs; ok {
		t.Fatalf("error channel should have been closed")
	}
}

func TestLiveMany(t *testing.T) {
	b := New([]string{addr},
		OptFlush(10*time.Millisecond),
		OptError(func(e ActionError) { t.Fatal(e) }),
	)

	var (
		clients   = 10
		documents = 10000
	)
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/%s", addr, index), nil)
	if err != nil {
		panic(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	if res.StatusCode != 200 {
		panic("DELETE err")
	}
	res.Body.Close()

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

	// One more to flush things.
	b.Enqueue() <- Action{
		Type: Create,
		MetaData: MetaData{
			Index:   index,
			Type:    "type1",
			Refresh: true, // <--
		},
		Document: `{"field1": "value1"}`,
	}
	time.Sleep(50 * time.Millisecond)

	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}

	// Wait for ES to index our documents.
	done := make(chan struct{}, 1)
	go func() {
		for {
			if getDocCount() >= 10001 {
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
	if have, want := getDocCount(), 10001; have != want {
		t.Fatalf("have %d, want %d", have, want)
	}
}

func getDocCount() int {
	// _cat/indices gives a line such as:
	// 'yellow open bubbles 5 1 10001 0 314.8kb 314.8kb\n'
	res, err := http.Get(fmt.Sprintf("http://%s/_cat/indices/%s", addr, index))
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
