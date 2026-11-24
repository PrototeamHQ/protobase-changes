package archive

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// archivedProperty is the table property holding the commit LSN of the last
// transaction archived in the table. It commits with the rows, so a segment
// read again after a crash skips what the table already holds.
const archivedProperty = "protobase.archived-commit-lsn"

// identifier names the archive table of a Postgres table: a child namespace
// per Postgres schema under the app's namespace.
func identifier(namespace string, t change.Table) table.Identifier {
	return table.Identifier{namespace, t.Schema, t.Name}
}

// commitTable archives one table's events of a segment: the schema updates
// they need, then the rows and the archived position in one catalog commit.
func commitTable(ctx context.Context, cat catalog.Catalog, ident table.Identifier, events []event) error {
	tbl, err := loadOrCreate(ctx, cat, ident)
	if err != nil {
		return err
	}
	archived, err := archivedLSN(tbl)
	if err != nil {
		return err
	}
	events = after(events, archived)
	if len(events) == 0 {
		return nil
	}

	s := shapeOf(tbl.Schema())
	rows := s.apply(events)
	if tbl, err = evolve(ctx, tbl, s.ops); err != nil {
		return err
	}
	txn := tbl.NewTransaction()
	if len(rows) > 0 {
		if err := appendRows(ctx, txn, rows); err != nil {
			return err
		}
	}
	last := events[len(events)-1].tx.CommitLSN
	if err := txn.SetProperties(iceberg.Properties{archivedProperty: strconv.FormatUint(uint64(last), 10)}); err != nil {
		return err
	}
	if _, err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("commit %v: %w", ident, err)
	}
	return nil
}

func appendRows(ctx context.Context, txn *table.Transaction, rows []archivedRow) error {
	staged, err := txn.StagedTable()
	if err != nil {
		return err
	}
	rec, err := record(staged.Schema(), rows)
	if err != nil {
		return err
	}
	defer rec.Release()
	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		return err
	}
	defer rdr.Release()
	return txn.Append(ctx, rdr, nil)
}

// after drops the events of transactions the table already holds.
func after(events []event, archived pglogrepl.LSN) []event {
	for i, ev := range events {
		if ev.tx.CommitLSN > archived {
			return events[i:]
		}
	}
	return nil
}

func archivedLSN(tbl *table.Table) (pglogrepl.LSN, error) {
	v, ok := tbl.Properties()[archivedProperty]
	if !ok {
		return 0, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("table %v property %s: %w", tbl.Identifier(), archivedProperty, err)
	}
	return pglogrepl.LSN(n), nil
}

func loadOrCreate(ctx context.Context, cat catalog.Catalog, ident table.Identifier) (*table.Table, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err == nil {
		return tbl, nil
	}
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, fmt.Errorf("load %v: %w", ident, err)
	}
	ns := catalog.NamespaceFromIdent(ident)
	for i := 1; i <= len(ns); i++ {
		if err := ensureNamespace(ctx, cat, ns[:i]); err != nil {
			return nil, err
		}
	}
	schema := baseSchema()
	spec, err := partitionSpec(schema)
	if err != nil {
		return nil, err
	}
	tbl, err = cat.CreateTable(ctx, ident, schema, catalog.WithPartitionSpec(&spec))
	if err != nil {
		return nil, fmt.Errorf("create %v: %w", ident, err)
	}
	return tbl, nil
}

func ensureNamespace(ctx context.Context, cat catalog.Catalog, ns table.Identifier) error {
	err := cat.CreateNamespace(ctx, ns, nil)
	if err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return fmt.Errorf("create namespace %v: %w", ns, err)
	}
	return nil
}
