package bubbles

import (
	"io/ioutil"
	"log"
	"testing"
	"time"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func TestIndex(t *testing.T) {
	es := newMockES(t, func() string {
		return `{"took":7,"items":[{"create":{"_index":"test","_type":"type1","_id":"1","_version":1}}]}`
	})
	defer es.Stop()

	b := New([]string{es.Addr()}, OptConnCount(2), OptFlush(10*time.Millisecond))

	ins := Action{
		Type: Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}

	b.Enqueue() <- ins
	time.Sleep(15 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}

func TestIndexNoES(t *testing.T) {
	// Index without an ES
	b := New([]string{"localhost:4321"}, OptConnCount(2), OptFlush(10*time.Millisecond))

	ins := Action{
		Type: Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}

	b.Enqueue() <- ins
	time.Sleep(20 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 1; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
	if pending[0] != ins {
		t.Errorf("Wrong pending object returned")
	}
}

func TestIndexErr(t *testing.T) {
	es := newMockES(
		t,
		func() string {
			return `{"took":8,"errors":true,"items":[{"index":{"_index":"index","_type":"type1","_id":"1","_version":5,"status":200}},{"index":{"_index":"index","_type":"type1","_id":"2","status":400,"error":"MapperParsingException[failed to parse]; nested: JsonParseException[Unexpected end-of-input within/between OBJECT entries\n at [Source: [B@5f72a900; line: 1, column: 160]]; "}}]}`
		},
	)
	defer es.Stop()

	errs := make(chan ActionError)
	b := New([]string{es.Addr()},
		OptConnCount(2),
		OptFlush(10*time.Millisecond),
		OptError(func(e ActionError) { errs <- e }),
	)

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

	b.Enqueue() <- ins1
	b.Enqueue() <- ins2
	var aerr ActionError
	select {
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout")
	case aerr = <-errs:
	}
	if have, want := aerr.Action, ins2; have != want {
		t.Fatalf("wrong err. have %v, want %v", have, want)
	}
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
}

func TestShutdownTimeout(t *testing.T) {
	es := newMockES(t, func() string {
		time.Sleep(10 * time.Second)
		return "{}"
	})
	defer es.Stop()

	b := New([]string{es.Addr()},
		OptConnCount(1),
		OptFlush(10*time.Millisecond),
		OptServerTimeout(100*time.Millisecond),
	)

	ins := Action{
		Type: Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}
	b.Enqueue() <- ins

	time.Sleep(20 * time.Millisecond)
	now := time.Now()
	pending := b.Stop()
	if time.Since(now) > 1*time.Second {
		t.Fatalf("Stop() took too long")
	}
	if have, want := len(pending), 1; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
	if pending[0] != ins {
		t.Errorf("Wrong pending object returned")
	}
}
