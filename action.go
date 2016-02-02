package bubbles

import (
	"encoding/json"
	"fmt"
)

// ActionType is the type of the action.
type ActionType string

// The actiontypes and what they are called by ElasticSearch.
const (
	Index  ActionType = "index"
	Create            = "create"
	Delete            = "delete"
	Update            = "update"
)

// Action is a single entry in a Bulk document. It might be re-sent to different
// servers while there are errors. The 'Document' needs to be a valid JSON
// insert/update/&c document, but can't contain any newline. 'Document' is
// ignored for 'delete' actions.
type Action struct {
	Type     ActionType
	MetaData MetaData
	Document string // without any \n!
}

// MetaData tells ElasticSearch how to deal with the document. Index and Type
// are required, the rest is not. See the ElasticSearch documentation for what
// they mean.
type MetaData struct {
	Index           string `json:"_index"`
	Type            string `json:"_type"`
	ID              string `json:"_id,omitempty"`
	RetryOnConflict int    `json:"_retry_on_conflict,omitempty"`
	// All those are not checked:
	Timestamp   int    `json:"_timestamp,omitempty"`
	TTL         int    `json:"ttl,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

// Buf returns the command ready for the ElasticSearch bulk buffer.
func (a *Action) Buf() string {
	switch a.Type {
	default:
		panic("what's this?")
	case Index, Create, Update:
		md, err := json.Marshal(a.MetaData)
		if err != nil {
			panic(err.Error())
		}
		return fmt.Sprintf("{\"%s\": %s}\n%s\n", a.Type, md, a.Document)
	case Delete:
		md, err := json.Marshal(a.MetaData)
		if err != nil {
			panic(err.Error())
		}
		return fmt.Sprintf("{\"%s\": %s}\n", a.Type, md)
	}
}
