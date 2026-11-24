package match

import "github.com/PrototeamHQ/protobase-changes/internal/change"

// view is a row as far as the WAL states it. A column is missing when the
// row lacks it, or when it is a large value the WAL did not repeat and no
// old copy of it came.
type view struct {
	row change.Row
	// fallback is the full old row an unchanged value is read from.
	fallback change.Row
}

func (v view) get(column string) (change.Value, bool) {
	value, ok := v.row[column]
	if ok && value.Kind == change.Unchanged {
		value, ok = v.fallback[column]
	}
	return value, ok && value.Kind != change.Unchanged
}

// oldView is the row before the change: nil for an insert. An update that
// kept the key under the default replica identity sends no old row, but its
// key is the old key too.
func oldView(c *change.Change) *view {
	switch {
	case c.Operation == change.Update && c.OldRowKind == change.NoOldRow:
		return &view{row: c.Key}
	case c.Operation == change.Update || c.Operation == change.Delete:
		return &view{row: c.OldRow}
	}
	return nil
}

// newView is the row after the change: nil for a delete.
func newView(c *change.Change) *view {
	if c.Operation != change.Insert && c.Operation != change.Update {
		return nil
	}
	v := &view{row: c.NewRow}
	if c.OldRowKind == change.OldFull {
		v.fallback = c.OldRow
	}
	return v
}
