package bubbles

import (
	"encoding/json"
	"fmt"
)

type ActionType string

const (
	Index  ActionType = "index"
	Create            = "create"
	Delete            = "delete"
	Update            = "update"
)

type Action struct {
	Type     ActionType
	MetaData MetaData
	Document string // without any \n! // TODO: []byte ?
}

// Buf returns the command ready for the ES bulk buffer
func (a *Action) Buf() []byte {
	switch a.Type {
	default:
		panic("what's this?")
	case Index, Create, Update:
		md, err := json.Marshal(a.MetaData)
		if err != nil {
			panic(err.Error())
		}
		return []byte(fmt.Sprintf("{\"%s\": %s}\n%s\n", a.Type, md, a.Document))
	case Delete:
		md, err := json.Marshal(a.MetaData)
		if err != nil {
			panic(err.Error())
		}
		return []byte(fmt.Sprintf("{\"%s\": %s}\n", a.Type, md))
	}
}
