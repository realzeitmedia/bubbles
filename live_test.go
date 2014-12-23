// +build live

package bubbles

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

const (
	addr  = "localhost:9200"
	index = "bubbles"
)

func TestLiveIndex(t *testing.T) {
	b, err := New([]string{addr}, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}

	ins := Action{
		Type: Index,
		MetaData: MetaData{
			Index: index,
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}

	b.Enqueue(&ins)
	time.Sleep(1000 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}

func TestLiveIndexError(t *testing.T) {
	// Index with errors.

	b, err := New([]string{addr}, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}

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

	b.Enqueue(&ins1)
	b.Enqueue(&ins2)
	time.Sleep(1000 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}

func TestLiveMany(t *testing.T) {
	b, err := New([]string{addr}, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	go func() {
		for e := range b.Errors() {
			fmt.Printf("error: %v\n", e)
		}
	}()

	var (
		clients = 10
		// documents = 1000
	)
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/%s", addr, index), nil)
	if err != nil {
		panic(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("res: %d - %s\n", res.StatusCode, string(body))

	for i := 0; i < clients; i++ {
		ins := Action{
			Type: Create,
			MetaData: MetaData{
				Index: index,
				Type:  "type1",
				ID:    "1",
			},
			Document: `{"field1": "value1"}`,
		}
		b.Enqueue(&ins)
	}

	time.Sleep(1000 * time.Millisecond)
	fmt.Printf("go stop\n")
	pending := b.Stop()
	fmt.Printf("done stop\n")
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}
