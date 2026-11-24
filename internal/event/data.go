package event

import (
	"encoding/json"
	"strings"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

// Data is a transaction event's body: the changes the token may read, in
// commit order.
type Data struct {
	Changes []Change `json:"changes"`
}

// Change is one row change. A thin payload holds the resource, operation and
// key; a raw one adds what the WAL states: the rows, the truncate options and
// who made the change.
type Change struct {
	Resource  string           `json:"resource"`
	Operation change.Operation `json:"operation"`
	Key       map[string]any   `json:"key,omitempty"`
	// OldRow is only the key unless the table has REPLICA IDENTITY FULL.
	OldRow          map[string]any  `json:"old,omitempty"`
	NewRow          map[string]any  `json:"new,omitempty"`
	Cascade         bool            `json:"cascade,omitempty"`
	RestartIdentity bool            `json:"restart_identity,omitempty"`
	Actor           json.RawMessage `json:"actor,omitempty"`
}

// Resource names a table as tokens list it: by name in the public schema,
// otherwise as schema.name. ok is false when the schema or the name holds a
// dot, since its resource could then name another table too, such as public
// "a.b" and schema "a" table "b": no token can read such a table.
func Resource(t change.Table) (resource string, ok bool) {
	if strings.Contains(t.Schema, ".") || strings.Contains(t.Name, ".") {
		return "", false
	}
	if t.Schema == "public" {
		return t.Name, true
	}
	return t.Schema + "." + t.Name, true
}

func data(tx *change.Transaction, tok token.Token) Data {
	actors := actors(tx)
	d := Data{Changes: []Change{}}
	for i, c := range tx.Changes {
		resource, ok := Resource(c.Table)
		if !ok || !tok.Resources[resource] {
			continue
		}
		out := Change{Resource: resource, Operation: c.Operation, Key: values(c.Key)}
		if tok.Payload == token.Raw {
			out.OldRow = values(c.OldRow)
			out.NewRow = values(c.NewRow)
			out.Cascade = c.Cascade
			out.RestartIdentity = c.RestartIdentity
			out.Actor = actors[i]
		}
		d.Changes = append(d.Changes, out)
	}
	return d
}

// values renders a row in Postgres' text format, a null as null, and leaves
// out an unchanged large value the WAL did not repeat.
func values(row change.Row) map[string]any {
	if len(row) == 0 {
		return nil
	}
	out := make(map[string]any, len(row))
	for column, v := range row {
		switch v.Kind {
		case change.Text:
			out[column] = v.Text
		case change.Null:
			out[column] = nil
		}
	}
	return out
}
