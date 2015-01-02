package bubbles_test

import (
	"fmt"
	"time"

	"github.com/realzeitmedia/bubbles"
)

func Example() {
	b, err := bubbles.New([]string{"127.0.0.1:9200"}, OptConnCount(2), OptFlush(1*time.second))
	if err != nil {
		panic(err.String())
	}
	defer func() {
		// Stop() returns all in-flight actions.
		for _, a := range b.Stop() {
			fmt.Printf("Discarding action %v\n", a)
		}
	}()

	go func() {
		// Errors must be read.
		for err := range b.Errors() {
			fmt.Printf("Err: %s\n", err)
		}
	}()

	b.Enqueue() <- Action{
		Type: bubbles.Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	}
}
