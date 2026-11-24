package archive

import (
	"encoding/json"
	"fmt"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// Prefixes of the logical messages the archive reads.
const (
	actorPrefix  = "protobase_actor"
	schemaPrefix = "protobase_schema"
)

// rename is a column rename that a protobase_schema message announces.
type rename struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	From   string `json:"from"`
	To     string `json:"to"`
}

// schemaMessage is the content of a protobase_schema message. Other kinds
// of schema change need no message: the WAL's table definitions show them.
type schemaMessage struct {
	Rename *rename `json:"rename"`
}

// parseRename reads a protobase_schema message, returning nil for one that
// announces no rename.
func parseRename(content []byte) (*rename, error) {
	var m schemaMessage
	if err := json.Unmarshal(content, &m); err != nil {
		return nil, fmt.Errorf("protobase_schema message: %w", err)
	}
	r := m.Rename
	if r == nil {
		return nil, nil
	}
	if r.Schema == "" || r.Table == "" || r.From == "" || r.To == "" {
		return nil, fmt.Errorf("protobase_schema rename needs a schema, table, from and to")
	}
	return r, nil
}

func (r *rename) table() change.Table {
	return change.Table{Schema: r.Schema, Name: r.Table}
}
