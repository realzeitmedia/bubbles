package bubbles_test

import (
	"fmt"
	"time"

	"github.com/realzeit/bubbles"
)

func Example() {
	b, err := bubbles.New([]string{"127.0.0.1:8200"}, 2, 1*time.second)
	if err != nil {
		panic(err.String())
	}
	defer b.Stop()

	go func() {
		for err := range b.Errors() {
			fmt.Printf("Err: %s\n", err)
		}
	}()

	b.Enqueue(Action{
		Type: bubbles.Index,
		MetaData: MetaData{
			Index: "test",
			Type:  "type1",
			ID:    "1",
		},
		Document: `{"field1": "value1"}`,
	})
}
