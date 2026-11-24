// Package change holds the committed transactions the slot reader produces.
package change

import (
	"time"

	"github.com/jackc/pglogrepl"
)

// Operation is what a row change did.
type Operation string

const (
	Insert   Operation = "insert"
	Update   Operation = "update"
	Delete   Operation = "delete"
	Truncate Operation = "truncate"
)

// ValueKind tells a stated value apart from a null and from a TOASTed value
// that did not change and so was not sent.
type ValueKind uint8

const (
	Text ValueKind = iota
	Null
	Unchanged
)

// Value is one column's value in Postgres' text format.
type Value struct {
	Kind ValueKind
	Text string
}

// Row maps column names to the values the WAL stated for them.
type Row map[string]Value

// OldRowKind says how much of the old row the WAL carried.
type OldRowKind string

const (
	// NoOldRow: the WAL sent no old row, as for an insert, or an update that
	// kept the key under the default replica identity.
	NoOldRow OldRowKind = ""
	// OldKeyOnly: the old row holds only the replica identity key columns.
	OldKeyOnly OldRowKind = "key"
	// OldFull: the old row holds every column, under REPLICA IDENTITY FULL.
	OldFull OldRowKind = "full"
)

// Table names a table by schema and name.
type Table struct {
	Schema string
	Name   string
}

// Column describes one column of a table as the WAL stated it at a change.
type Column struct {
	Name string
	// Type is the column's type OID, and Modifier its type modifier, such as
	// the length of a varchar, or -1 for none.
	Type     uint32
	Modifier int32
	// Key marks a column of the replica identity key.
	Key bool
}

// Change is one row change, or one truncate of one table.
type Change struct {
	Table     Table
	Operation Operation
	// Columns are the table's columns at this change, in table order. Changes
	// under the same table definition share the slice.
	Columns []Column
	// Key holds the replica identity key columns. It is empty for a truncate
	// and for a table under REPLICA IDENTITY FULL, whose WAL names no key.
	Key        Row
	OldRowKind OldRowKind
	OldRow     Row
	NewRow     Row
	// Cascade and RestartIdentity are the options of a truncate.
	Cascade         bool
	RestartIdentity bool
}

// Message is a transactional logical message the app emitted with a known
// prefix, such as the actor or a schema change.
type Message struct {
	Prefix  string
	Content []byte
	// Position is the number of the transaction's changes before it.
	Position int
}

// Transaction is one committed transaction.
type Transaction struct {
	Xid        uint32
	CommitLSN  pglogrepl.LSN
	EndLSN     pglogrepl.LSN
	CommitTime time.Time
	Changes    []Change
	Messages   []Message
}
