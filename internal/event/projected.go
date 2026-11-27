package event

import (
	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/match"
)

// Projected returns the changes of tx that subs concern, in commit order,
// each with the key and, of the rows its subscriptions may read, only the
// columns fields names. With no fields, a change holds no rows, as a thin
// payload does.
func Projected(tx *change.Transaction, subs *match.Index, fields []string) []Change {
	var out []Change
	for i, c := range tx.Changes {
		resource, ok := Resource(c.Table)
		if !ok {
			continue
		}
		tags := subs.Match(resource, &tx.Changes[i])
		if len(tags) == 0 {
			continue
		}
		key, oldRow, newRow := visible(&tx.Changes[i], tags)
		out = append(out, Change{
			Resource: resource, Operation: c.Operation, Key: values(key),
			OldRow: values(project(oldRow, fields)), NewRow: values(project(newRow, fields)),
			Subscriptions: tags,
		})
	}
	return out
}

// project keeps the columns of row that fields names.
func project(row change.Row, fields []string) change.Row {
	out := change.Row{}
	for _, field := range fields {
		if v, ok := row[field]; ok {
			out[field] = v
		}
	}
	return out
}
