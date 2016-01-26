package bubbles

import (
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/realzeitmedia/bubbles/loges"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func TestIndex(t *testing.T) {
	es := newMockES(t, func() string {
		return `{"took":7,"items":[{"create":{"_index":"test","_type":"type1","_id":"1","_version":1}}]}`
	})
	defer es.Stop()

	c := &count{}
	b := New([]string{es.Addr()},
		OptConnCount(2),
		OptFlush(10*time.Millisecond),
		OptCounter(c),
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
	time.Sleep(100 * time.Millisecond)
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
	if have, want := *c, (count{
		Retries:    0,
		Sends:      1,
		SendTotals: val{1, len(ins.Buf())},
		Troubles:   0,
	}); have != want {
		t.Fatalf("counts: have %v, want %v", have, want)
	}
}

func TestIndexNoES(t *testing.T) {
	// Index without an ES
	c := &count{}
	b := New([]string{"localhost:4321"},
		OptConnCount(2),
		OptFlush(10*time.Millisecond),
		OptCounter(c),
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
	pending := b.Stop()
	if have, want := len(pending), 1; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
	if pending[0] != ins {
		t.Errorf("Wrong pending object returned")
	}
	if have, want := c.Retries, 1; have < want {
		t.Fatalf("retries: have %v, want at least %v", have, want)
	}
}

type TestErrs struct {
	Errors   chan ActionError
	Warnings chan ActionError
	t        *testing.T
}

func NewTestErrs(t *testing.T) *TestErrs {
	return &TestErrs{
		Errors:   make(chan ActionError, 1),
		Warnings: make(chan ActionError),
		t:        t,
	}
}

func (e *TestErrs) Error(err error) {
	switch t := err.(type) {
	case ActionError:
		e.Errors <- t
	default:
		e.t.Fatal(err)
	}
}

func (e *TestErrs) Warning(err error) {
	switch t := err.(type) {
	case ActionError:
		e.Warnings <- t
	default:
		e.t.Fatal(err)
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

	errs := NewTestErrs(t)
	c := &count{}
	b := New([]string{es.Addr()},
		OptConnCount(1),
		OptFlush(10*time.Millisecond),
		OptErrer(errs),
		OptCounter(c),
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
	case aerr = <-errs.Errors:
	}
	if have, want := aerr.Action, ins2; have != want {
		t.Fatalf("wrong err. have %v, want %v", have, want)
	}
	pending := b.Stop()
	if have, want := len(pending), 0; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
	if have, want := *c, (count{
		Sends:      1,
		Retries:    0,
		Errors:     1,
		SendTotals: val{1, len(ins1.Buf()) + len(ins2.Buf())},
		Troubles:   0,
	}); have != want {
		t.Fatalf("counts: have %v, want %v", have, want)
	}
}

func TestIndexCreate(t *testing.T) {
	es := newMockES(
		t,
		func() string {
			return `{"took":8,"errors":true,"items":[{"index":{"_index":"index","_type":"type1","_id":"1","_version":5,"status":200}},{"create":{"_index":"index","_type":"type1","_id":"2","status":429,"error":"RemoteTransportException[[rz-es6-go][inet[/1.2.3.4:9300]][indices:data/write/bulk[s]]]; nested: EsRejectedExecutionException[rejected execution (queue capacity 300) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$PrimaryPhase$1@1204a018];"}}]}`
		},
	)
	defer es.Stop()

	errs := NewTestErrs(t)
	c := &count{}
	b := New([]string{es.Addr()},
		OptConnCount(1),
		OptFlush(10*time.Millisecond),
		OptErrer(errs),
		OptCounter(c),
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
		Document: `{"field1": "value1"}`,
	}

	b.Enqueue() <- ins1
	b.Enqueue() <- ins2
	var awarning ActionError
	select {
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout")
	case awarning = <-errs.Warnings:
	}
	if have, want := awarning.Action, ins2; have != want {
		t.Fatalf("wrong err. have %v, want %v", have, want)
	}
	pending := b.Stop()
	if have, want := len(pending), 1; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
	if have, want := *c, (count{
		Sends:      1,
		Retries:    1,
		Errors:     0,
		SendTotals: val{1, len(ins1.Buf()) + len(ins2.Buf())},
		Troubles:   1,
	}); have != want {
		t.Fatalf("counts: have %v, want %v", have, want)
	}
	if have, want := len(errs.Errors), 0; have != want {
		t.Fatalf("have %d, want %d", have, want)
	}
}

func TestShutdownTimeout(t *testing.T) {
	es := newMockES(t, func() string {
		time.Sleep(10 * time.Second)
		return "{}"
	})
	defer es.Stop()

	maxDocs := 5
	b := New([]string{es.Addr()},
		OptConnCount(1),
		OptFlush(10*time.Millisecond),
		OptServerTimeout(5*time.Second),
		OptMaxDocs(5),
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
	docs := maxDocs
	for i := 0; i < docs; i++ {
		b.Enqueue() <- ins
	}

	time.Sleep(20 * time.Millisecond)
	p := make(chan []Action)
	go func() {
		p <- b.Stop()
	}()
	var pending []Action
	select {
	case pending = <-p:
	case <-time.After(1 * time.Second):
		t.Fatalf("Stop() took too long")
	}
	if have, want := len(pending), docs; have != want {
		t.Fatalf("have %d, want %d: %v", have, want, pending)
	}
	if pending[0] != ins {
		t.Errorf("Wrong pending object returned")
	}
}

type val struct{ C, T int }

type count struct {
	Sends      int
	Retries    int
	Errors     int
	SendTotals val
	Troubles   int
}

func (c *count) Actions(s, r, e int) {
	c.Sends += s
	c.Retries += r
	c.Errors += e
}

func (c *count) SendTotal(l int) {
	c.SendTotals.C++
	c.SendTotals.T += l
}

func (c *count) Trouble() {
	c.Troubles++
}

func (c *count) BatchTime(time.Duration) {
}

var _ loges.Counter = &count{}
