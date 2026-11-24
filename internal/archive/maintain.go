package archive

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
)

// maintenanceInterval is how often every archive table of an app gets its
// retention and compaction.
const maintenanceInterval = time.Hour

// maintain applies retention and compaction to every table under the app's
// namespace, including tables that no longer get changes.
func maintain(ctx context.Context, cat catalog.Catalog, namespace string, retention time.Duration, now time.Time) error {
	app := table.Identifier{namespace}
	exists, err := cat.CheckNamespaceExists(ctx, app)
	if err != nil {
		return fmt.Errorf("check namespace %s: %w", namespace, err)
	}
	if !exists {
		return nil
	}
	schemas, err := cat.ListNamespaces(ctx, app)
	if err != nil {
		return fmt.Errorf("list namespaces of %s: %w", namespace, err)
	}
	for _, ns := range schemas {
		for ident, err := range cat.ListTables(ctx, ns) {
			if err != nil {
				return fmt.Errorf("list tables of %v: %w", ns, err)
			}
			if err := maintainTable(ctx, cat, ident, retention, now); err != nil {
				return fmt.Errorf("maintain %v: %w", ident, err)
			}
		}
	}
	return nil
}

func maintainTable(ctx context.Context, cat catalog.Catalog, ident table.Identifier, retention time.Duration, now time.Time) error {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return err
	}
	if tbl, err = expire(ctx, tbl, retention, now); err != nil {
		return err
	}
	return compact(ctx, tbl, now)
}

// expire deletes the changes of every hour that ended before the retention
// began, whole files at a time as the table is partitioned by hour, and
// expires the snapshots older than the retention, deleting the files only
// they still reference.
func expire(ctx context.Context, tbl *table.Table, retention time.Duration, now time.Time) (*table.Table, error) {
	cutoff := now.Add(-retention).Truncate(time.Hour)
	old := before(cutoff)
	tasks, err := tbl.Scan(table.WithRowFilter(old)).PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	txn := tbl.NewTransaction()
	if len(tasks) > 0 {
		if err := txn.Delete(ctx, old, nil); err != nil {
			return nil, fmt.Errorf("delete changes before %s: %w", cutoff.Format(time.RFC3339), err)
		}
	}
	if err := txn.ExpireSnapshots(table.WithOlderThan(retention), table.WithRetainLast(1)); err != nil {
		return nil, err
	}
	return txn.Commit(ctx)
}

// compact merges the small segment files of each hour that has ended into
// files of the target size. The current hour still gets segments.
func compact(ctx context.Context, tbl *table.Table, now time.Time) error {
	tasks, err := tbl.Scan(table.WithRowFilter(before(now.Truncate(time.Hour)))).PlanFiles(ctx)
	if err != nil {
		return err
	}
	cfg := compaction.DefaultConfig()
	// An hour holds a few segments, fewer than the default minimum of five.
	cfg.MinInputFiles = 2
	plan, err := cfg.PlanCompaction(tasks)
	if err != nil {
		return err
	}
	if len(plan.Groups) == 0 {
		return nil
	}
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	if _, err := tbl.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{}); err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	return nil
}

func before(t time.Time) iceberg.BooleanExpression {
	return iceberg.LessThan(iceberg.Reference(fieldCommitTime), iceberg.Timestamp(t.UnixMicro()))
}
