package bubbles_test

import (
	"fmt"
	"time"

	"github.com/realzeitmedia/bubbles"
)

func Example() {
	b := bubbles.New([]string{"127.0.0.1:9200"},
		bubbles.OptConnCount(2),
		bubbles.OptFlush(1*time.Second),
		bubbles.OptError(func(e bubbles.ActionError) {
			fmt.Printf("Err: %s\n", e)
		}),
	)
	defer func() {
		// Stop() returns all in-flight actions.
		for _, a := range b.Stop() {
			fmt.Printf("Discarding action %v\n", a)
		}
	}()

	b.Enqueue() <- bubbles.Action{
		Type: bubbles.Index,
		MetaData: bubbles.MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}
}
