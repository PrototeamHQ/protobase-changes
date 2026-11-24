package archive

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// evolve commits a shape's schema operations to tbl, one catalog commit
// each: one schema update resolves every name against the schema it starts
// from, and a transaction takes one schema update, while an operation may
// name a field an earlier one renamed or added. Renames and promotions keep
// each field's id, so data written before reads under today's names and
// types. A segment read again after a crash finds its operations done: a
// renamed column is gone, and an added or promoted field already holds.
func evolve(ctx context.Context, tbl *table.Table, ops []schemaOp) (*table.Table, error) {
	for _, op := range ops {
		txn := tbl.NewTransaction()
		u := txn.UpdateSchema(true, false)
		switch op.kind {
		case opAddStruct:
			u.AddColumn(op.path, op.typ, "", false, nil)
		case opAddField:
			u.AddColumn([]string{op.path[0], op.name}, op.typ, "", false, nil)
		case opRename:
			u.RenameColumn(op.path, op.name)
		case opPromote:
			u.UpdateColumn(op.path, table.ColumnUpdate{FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: op.typ}})
		default:
			panic(fmt.Sprintf("unknown schema operation %d", op.kind))
		}
		if err := u.Commit(); err != nil {
			return nil, fmt.Errorf("schema update of %v: %w", op.path, err)
		}
		var err error
		if tbl, err = txn.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit schema update of %v: %w", op.path, err)
		}
	}
	return tbl, nil
}
