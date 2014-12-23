// +build live

package bubbles

import (
	"testing"
	"time"
)

func TestLiveIndex(t *testing.T) {
	addr := "192.168.2.20:9200"
	index := "bubbles"

	b, err := New([]string{addr}, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}

	ins := Action{
		Type:  Index,
		Retry: 1,
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
	addr := "192.168.2.20:9200"
	index := "bubbles"

	b, err := New([]string{addr}, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}

	ins1 := Action{
		Type:  Index,
		Retry: 1,
		MetaData: MetaData{
			Index: index,
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}
	ins2 := Action{
		Type:  Index,
		Retry: 1,
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
