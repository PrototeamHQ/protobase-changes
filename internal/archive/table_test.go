package archive

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
)

// devCatalog opens the development SQL catalog on a local folder.
func devCatalog(t *testing.T) catalog.Catalog {
	t.Helper()
	dir := t.TempDir()
	cat, err := Open(context.Background(), "shop", config.Catalog{
		Type: config.SQLCatalog, Path: filepath.Join(dir, "catalog.db"), Warehouse: "file://" + filepath.Join(dir, "warehouse"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

var messages = change.Table{Schema: "public", Name: "messages"}

func text(s string) change.Value { return change.Value{Kind: change.Text, Text: s} }

func insert(cols []change.Column, id, body string) change.Change {
	return change.Change{
		Table: messages, Operation: change.Insert, Columns: cols,
		Key: change.Row{"id": text(id)}, NewRow: change.Row{"id": text(id), "body": text(body)},
	}
}

func tx(lsn uint64, at time.Time, changes []change.Change, msgs ...change.Message) *change.Transaction {
	return &change.Transaction{Xid: uint32(lsn), CommitLSN: pglogrepl.LSN(lsn), EndLSN: pglogrepl.LSN(lsn + 1), CommitTime: at, Changes: changes, Messages: msgs}
}

func commitSegment(t *testing.T, cat catalog.Catalog, txs ...*change.Transaction) {
	t.Helper()
	var seg segment
	for _, x := range txs {
		seg.add(x, time.Now())
	}
	byTable, err := seg.tables()
	if err != nil {
		t.Fatal(err)
	}
	for tbl, events := range byTable {
		if err := commitTable(context.Background(), cat, identifier("shop", tbl), events); err != nil {
			t.Fatal(err)
		}
	}
}

// archivedRows reads the table back as JSON rows, oldest first.
func archivedRows(t *testing.T, cat catalog.Catalog) []map[string]any {
	t.Helper()
	ctx := context.Background()
	tbl, err := cat.LoadTable(ctx, identifier("shop", messages))
	if err != nil {
		t.Fatal(err)
	}
	at, err := tbl.Scan(table.WithSelectedFields("commit_lsn", "seq", "operation", "actor", "key", "new", "unchanged")).ToArrowTable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer at.Release()
	rdr := array.NewTableReader(at, -1)
	defer rdr.Release()
	var rows []map[string]any
	for rdr.Next() {
		out, err := rdr.RecordBatch().MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		var batch []map[string]any
		if err := json.Unmarshal(out, &batch); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, batch...)
	}
	sortRows(rows)
	return rows
}

func sortRows(rows []map[string]any) {
	for i := range rows {
		for j := i + 1; j < len(rows); j++ {
			if rows[j]["commit_lsn"].(float64) < rows[i]["commit_lsn"].(float64) {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
}

func TestCommitTableArchivesChanges(t *testing.T) {
	cat := devCatalog(t)
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	now := time.Now().UTC()
	actor := change.Message{Prefix: actorPrefix, Content: []byte(`{"sub":"u42"}`), Position: 1}
	update := change.Change{
		Table: messages, Operation: change.Update, Columns: cols,
		Key: change.Row{"id": text("1")}, NewRow: change.Row{"id": text("1"), "body": {Kind: change.Unchanged}},
	}
	commitSegment(t, cat, tx(100, now, []change.Change{insert(cols, "1", "hi"), update}, actor))

	rows := archivedRows(t, cat)
	if len(rows) != 2 {
		t.Fatalf("got %d rows: %v", len(rows), rows)
	}
	if rows[0]["operation"] != "insert" || rows[0]["actor"] != nil || rows[0]["new"].(map[string]any)["body"] != "hi" {
		t.Errorf("insert row %v", rows[0])
	}
	if rows[0]["key"].(map[string]any)["id"] != float64(1) {
		t.Errorf("insert key %v", rows[0]["key"])
	}
	if rows[1]["actor"] != `{"sub":"u42"}` || rows[1]["new"].(map[string]any)["body"] != nil {
		t.Errorf("update row %v", rows[1])
	}
	if u := rows[1]["unchanged"].([]any); len(u) != 1 || u[0] != "body" {
		t.Errorf("update unchanged %v", rows[1]["unchanged"])
	}
}

func TestCommitTableSkipsWhatItHolds(t *testing.T) {
	cat := devCatalog(t)
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	now := time.Now().UTC()
	first := tx(100, now, []change.Change{insert(cols, "1", "a")})
	second := tx(200, now, []change.Change{insert(cols, "2", "b")})
	commitSegment(t, cat, first)
	// A crash after the commit but before the slot was confirmed reads both again.
	commitSegment(t, cat, first, second)
	commitSegment(t, cat, first, second)

	rows := archivedRows(t, cat)
	if len(rows) != 2 || rows[0]["commit_lsn"] != float64(100) || rows[1]["commit_lsn"] != float64(200) {
		t.Fatalf("got %v", rows)
	}
}

func TestCommitTableRenamesInPlace(t *testing.T) {
	cat := devCatalog(t)
	before := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	after := []change.Column{col("id", oidInt8, true), col("content", oidText, false)}
	now := time.Now().UTC()
	commitSegment(t, cat, tx(100, now, []change.Change{insert(before, "1", "old")}))
	rename := change.Message{Prefix: schemaPrefix, Content: []byte(`{"rename":{"schema":"public","table":"messages","from":"body","to":"content"}}`)}
	renamed := change.Change{
		Table: messages, Operation: change.Insert, Columns: after,
		Key: change.Row{"id": text("2")}, NewRow: change.Row{"id": text("2"), "content": text("new")},
	}
	commitSegment(t, cat, tx(200, now, []change.Change{renamed}, rename))

	rows := archivedRows(t, cat)
	if len(rows) != 2 {
		t.Fatalf("got %v", rows)
	}
	for i, want := range []string{"old", "new"} {
		if got := rows[i]["new"].(map[string]any)["content"]; got != want {
			t.Errorf("row %d content %v, want %s: %v", i, got, want, rows[i])
		}
	}
}

func TestMaintainExpiresAndCompacts(t *testing.T) {
	cat := devCatalog(t)
	ctx := context.Background()
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	now := time.Now().UTC()
	old := now.Add(-50 * time.Hour)
	recent := now.Add(-2 * time.Hour).Truncate(time.Hour)
	commitSegment(t, cat, tx(100, old, []change.Change{insert(cols, "1", "gone")}))
	commitSegment(t, cat, tx(200, recent, []change.Change{insert(cols, "2", "kept")}))
	commitSegment(t, cat, tx(300, recent.Add(time.Minute), []change.Change{insert(cols, "3", "kept")}))

	if err := maintain(ctx, cat, "shop", 24*time.Hour, now); err != nil {
		t.Fatal(err)
	}

	rows := archivedRows(t, cat)
	if len(rows) != 2 || rows[0]["commit_lsn"] != float64(200) || rows[1]["commit_lsn"] != float64(300) {
		t.Fatalf("got %v", rows)
	}
	tbl, err := cat.LoadTable(ctx, identifier("shop", messages))
	if err != nil {
		t.Fatal(err)
	}
	files, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Errorf("the hour's two segments are %d files after compaction, want 1", len(files))
	}
}
