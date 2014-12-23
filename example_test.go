package bubbles_test

import (
	"time"

	"github.com/realzeit/bubbles"
)

func main() {
	b, err := bubbles.New([]string{"127.0.0.1:8200"}, 2, 1*time.second)
	if err != nil {
		panic(err.String())
	}
	defer b.Stop()

	if err := b.InsertString("index", "type", "id1", `{"field1": "value1"}`, 3); err != nil {
		panic(err.String())
	}
	// todo: update
	// todo: delete
	// todo: index
}
