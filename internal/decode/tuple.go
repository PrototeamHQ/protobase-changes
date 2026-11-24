package decode

import (
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// Replica identity settings of a relation, as pgoutput sends them.
const (
	identityDefault = 'd'
	identityIndex   = 'i'
)

// keyColumnFlag marks a column of the replica identity key.
const keyColumnFlag = 1

func table(rel *pglogrepl.RelationMessage) change.Table {
	return change.Table{Schema: rel.Namespace, Name: rel.RelationName}
}

// row reads a tuple; keyOnly keeps only the key columns, as a key-only old
// tuple sends the other columns as nulls it never read.
func row(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData, keyOnly bool) change.Row {
	if tuple == nil {
		return nil
	}
	if len(tuple.Columns) != len(rel.Columns) {
		// pgoutput always sends a Relation message matching the tuples after it.
		panic(fmt.Sprintf("tuple of %d columns for relation %s.%s of %d", len(tuple.Columns), rel.Namespace, rel.RelationName, len(rel.Columns)))
	}
	r := change.Row{}
	for i, col := range tuple.Columns {
		meta := rel.Columns[i]
		if keyOnly && meta.Flags&keyColumnFlag == 0 {
			continue
		}
		r[meta.Name] = value(col)
	}
	return r
}

func value(col *pglogrepl.TupleDataColumn) change.Value {
	switch col.DataType {
	case pglogrepl.TupleDataTypeNull:
		return change.Value{Kind: change.Null}
	case pglogrepl.TupleDataTypeToast:
		return change.Value{Kind: change.Unchanged}
	}
	return change.Value{Kind: change.Text, Text: string(col.Data)}
}

func oldRow(rel *pglogrepl.RelationMessage, tupleType uint8, tuple *pglogrepl.TupleData) (change.OldRowKind, change.Row) {
	switch tupleType {
	case pglogrepl.UpdateMessageTupleTypeKey:
		return change.OldKeyOnly, row(rel, tuple, true)
	case pglogrepl.UpdateMessageTupleTypeOld:
		return change.OldFull, row(rel, tuple, false)
	}
	return change.NoOldRow, nil
}

// key picks the replica identity key out of a row. Under REPLICA IDENTITY FULL
// pgoutput flags every column, so the WAL names no key and it stays empty.
func key(rel *pglogrepl.RelationMessage, r change.Row) change.Row {
	if rel.ReplicaIdentity != identityDefault && rel.ReplicaIdentity != identityIndex {
		return nil
	}
	k := change.Row{}
	for _, meta := range rel.Columns {
		if meta.Flags&keyColumnFlag == 0 {
			continue
		}
		if v, ok := r[meta.Name]; ok {
			k[meta.Name] = v
		}
	}
	return k
}
