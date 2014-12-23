package bubbles

import (
	"testing"
	"time"
)

func TestIndex(t *testing.T) {
	es := newMockES(t, `{"took":7,"items":[{"create":{"_index":"test","_type":"type1","_id":"1","_version":1}}]}`)
	defer es.Stop()

	b, err := New([]string{es.Addr()}, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}

	ins := Action{
		Type: Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}

	b.Enqueue(&ins)
	time.Sleep(15 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}

func TestIndexNoES(t *testing.T) {
	// Index without an ES
	b, err := New([]string{"localhost:4321"}, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}

	ins := Action{
		Type: Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}

	b.Enqueue(&ins)
	time.Sleep(15 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 1; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
	if pending[0] != &ins {
		t.Errorf("Wrong pending object returned")
	}
}

func TestIndexErr(t *testing.T) {
	es := newMockES(t,
		`{"took":8,"errors":true,"items":[{"index":{"_index":"index","_type":"type1","_id":"1","_version":5,"status":200}},{"index":{"_index":"index","_type":"type1","_id":"2","status":400,"error":"MapperParsingException[failed to parse]; nested: JsonParseException[Unexpected end-of-input within/between OBJECT entries\n at [Source: [B@5f72a900; line: 1, column: 160]]; "}}]}`)
	defer es.Stop()

	b, err := New([]string{es.Addr()}, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}

	ins1 := Action{
		Type: Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}
	ins2 := Action{
		Type: Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "2",
		},
		Document: `{"field1": `, // fake an error
	}

	b.Enqueue(&ins1)
	b.Enqueue(&ins2)
	var action *Action
	select {
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout")
	case action = <-b.Errors():
	}
	if have, want := action, &ins2; have != want {
		t.Fatalf("wrong err. have %v, want %v", have, want)
	}
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}
