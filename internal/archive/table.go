package archive

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// archivedProperty is the table property holding the position of the last
// event archived in the table, as "<commit LSN>/<step>". It commits with the
// rows, so a segment read again after a crash skips what the table already holds.
const archivedProperty = "protobase.archived-position"

// position is where an event stands in the WAL: its transaction's commit,
// then its step in it.
type position struct {
	lsn  pglogrepl.LSN
	step int
}

func (p position) after(o position) bool {
	return p.lsn > o.lsn || p.lsn == o.lsn && p.step > o.step
}

func (p position) String() string { return fmt.Sprintf("%d/%d", uint64(p.lsn), p.step) }

func (ev event) position() position { return position{ev.tx.CommitLSN, ev.step} }

// identifier names the archive table of a Postgres table: a child namespace
// per Postgres schema under the app's namespace.
func identifier(namespace string, t change.Table) table.Identifier {
	return table.Identifier{namespace, t.Schema, t.Name}
}

// commitTable archives one table's events of a segment, in runs that each
// end before an event that changes the schema: a run's rows and the archived
// position commit together, then the event's schema updates, then the next
// run. A schema update commits on its own, as a catalog transaction takes one,
// so a crash can leave it done while the rows before it are archived and the
// rows after it are not: a segment read again finds every event before it
// skipped and every event after it applying to the schema it changed.
func commitTable(ctx context.Context, cat catalog.Catalog, ident table.Identifier, events []event) error {
	tbl, err := loadOrCreate(ctx, cat, ident)
	if err != nil {
		return err
	}
	archived, err := archivedPosition(tbl)
	if err != nil {
		return err
	}
	events = after(events, archived)
	evolved := false
	for len(events) > 0 {
		s := shapeOf(tbl.Schema())
		n, rows := s.apply(events)
		if n == 0 {
			if evolved {
				return fmt.Errorf("%v: the schema still changes for the change at %s after its updates", ident, events[0].position())
			}
			if tbl, err = evolve(ctx, tbl, s.ops); err != nil {
				return err
			}
			evolved = true
			continue
		}
		if tbl, err = commitRun(ctx, tbl, rows, events[n-1].position()); err != nil {
			return fmt.Errorf("commit %v: %w", ident, err)
		}
		events = events[n:]
		evolved = false
	}
	return nil
}

// commitRun appends rows and sets the archived position to last in one commit.
func commitRun(ctx context.Context, tbl *table.Table, rows []archivedRow, last position) (*table.Table, error) {
	txn := tbl.NewTransaction()
	if len(rows) > 0 {
		if err := appendRows(ctx, txn, rows); err != nil {
			return nil, err
		}
	}
	if err := txn.SetProperties(iceberg.Properties{archivedProperty: last.String()}); err != nil {
		return nil, err
	}
	return txn.Commit(ctx)
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

// after drops the events the table already holds.
func after(events []event, archived position) []event {
	for i, ev := range events {
		if ev.position().after(archived) {
			return events[i:]
		}
	}
	return nil
}

// archivedPosition reads the archived position, or the start of the WAL when
// nothing is archived yet, so every event comes after it.
func archivedPosition(tbl *table.Table) (position, error) {
	v, ok := tbl.Properties()[archivedProperty]
	if !ok {
		return position{step: -1}, nil
	}
	lsnText, stepText, _ := strings.Cut(v, "/")
	lsn, err := strconv.ParseUint(lsnText, 10, 64)
	if err != nil {
		return position{}, fmt.Errorf("table %v property %s %q: %w", tbl.Identifier(), archivedProperty, v, err)
	}
	step, err := strconv.Atoi(stepText)
	if err != nil {
		return position{}, fmt.Errorf("table %v property %s %q: %w", tbl.Identifier(), archivedProperty, v, err)
	}
	return position{pglogrepl.LSN(lsn), step}, nil
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
