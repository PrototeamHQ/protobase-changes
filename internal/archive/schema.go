package archive

import (
	"github.com/apache/iceberg-go"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// Top-level fields of every archive table: one row per change.
const (
	fieldCommitLSN  = "commit_lsn"
	fieldXid        = "xid"
	fieldCommitTime = "commit_time"
	// fieldSeq is the change's position in its transaction.
	fieldSeq       = "seq"
	fieldOperation = "operation"
	// fieldActor is the content of the transaction's last protobase_actor
	// message before the change.
	fieldActor = "actor"
	// fieldUnchanged lists the columns whose large values an update left
	// unchanged, which the WAL does not repeat; they are null in new.
	fieldUnchanged = "unchanged"
)

// Row structs, each with one field per Postgres column. key holds only the
// replica identity key columns.
const (
	structKey = "key"
	structOld = "old"
	structNew = "new"
)

var rowStructs = []string{structKey, structOld, structNew}

// baseSchema is the schema of a new table: the fields of every change. The
// row structs join as columns need them, as Parquet stores no empty struct.
// The catalog assigns the final field ids.
func baseSchema() *iceberg.Schema {
	p := iceberg.PrimitiveTypes
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: fieldCommitLSN, Type: p.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: fieldXid, Type: p.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: fieldCommitTime, Type: p.TimestampTz, Required: true},
		iceberg.NestedField{ID: 4, Name: fieldSeq, Type: p.Int32, Required: true},
		iceberg.NestedField{ID: 5, Name: fieldOperation, Type: p.String, Required: true},
		iceberg.NestedField{ID: 6, Name: fieldActor, Type: p.String},
		iceberg.NestedField{ID: 7, Name: fieldUnchanged, Type: &iceberg.ListType{ElementID: 8, Element: p.String}},
	)
}

// structColumns are the columns of cols that struct s holds.
func structColumns(s string, cols []change.Column) []change.Column {
	if s != structKey {
		return cols
	}
	var key []change.Column
	for _, c := range cols {
		if c.Key {
			key = append(key, c)
		}
	}
	return key
}

// partitionSpec partitions a table by the hour of commit_time, so retention
// drops whole files and compaction merges an hour's segments.
func partitionSpec(schema *iceberg.Schema) (iceberg.PartitionSpec, error) {
	return iceberg.NewPartitionSpecOpts(
		iceberg.AddPartitionFieldByName(fieldCommitTime, "commit_hour", iceberg.HourTransform{}, schema, nil))
}
