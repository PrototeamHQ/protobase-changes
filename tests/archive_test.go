//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/PrototeamHQ/protobase-changes/internal/archive"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
)

// catalogConfig reads the REST catalog the tests archive to, as
// tests/catalog/start.sh prints it.
func catalogConfig(t *testing.T) config.Catalog {
	t.Helper()
	raw := os.Getenv("PCS_TEST_CATALOG")
	if raw == "" {
		t.Fatal("PCS_TEST_CATALOG is not set: run tests/catalog/start.sh")
	}
	var c config.Catalog
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatalf("PCS_TEST_CATALOG: %v", err)
	}
	return c
}

// archiving follows the app and archives it until the returned stop is called.
func archiving(t *testing.T, ctx context.Context, app config.App) (stop func()) {
	t.Helper()
	cat, err := archive.Open(ctx, app.Name, app.Archive.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	f := newFeed(t, app)
	r := reader.New(app, f, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	stopReader := start(t, ctx, r)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := archive.New(*app.Archive, cat, f, r).Run(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("archiver stopped: %v", err)
		}
	}()
	waitActive(t, ctx, app.Slot)
	return func() {
		cancel()
		<-done
		stopReader()
	}
}

// waitActive waits until the reader holds its slot and the slot has a
// position, so the reader reads every change committed after.
func waitActive(t *testing.T, ctx context.Context, slot string) {
	t.Helper()
	db := connect(t, ctx, databaseURL(t))
	for {
		var active bool
		err := db.QueryRow(ctx, "SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&active)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		if active {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the reader to hold its slot")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func archiveApp(t *testing.T, ctx context.Context, url, name string, segmentSeconds int) config.App {
	t.Helper()
	cat := catalogConfig(t)
	t.Cleanup(func() { dropNamespace(t, cat, name) })
	return config.App{
		Name: "test", DatabaseURL: url, Slot: name, Publication: name, TailTransactions: 100,
		Archive: &config.Archive{Catalog: cat, Namespace: name, SegmentBytes: 1 << 20, SegmentSeconds: segmentSeconds, RetentionHours: 24},
	}
}

func dropNamespace(t *testing.T, c config.Catalog, name string) {
	ctx := context.Background()
	cat, err := archive.Open(ctx, "test", c)
	if err != nil {
		t.Fatal(err)
	}
	for ident, err := range cat.ListTables(ctx, table.Identifier{name, "public"}) {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := cat.DropTable(ctx, ident); err != nil {
			t.Fatal(err)
		}
	}
	for _, ns := range []table.Identifier{{name, "public"}, {name}} {
		if err := cat.DropNamespace(ctx, ns); err != nil && !errors.Is(err, catalog.ErrNoSuchNamespace) {
			t.Fatal(err)
		}
	}
}

// currentLSN is the server's WAL position: a transaction committed after it
// ends past it.
func currentLSN(t *testing.T, ctx context.Context, db *pgx.Conn) pglogrepl.LSN {
	t.Helper()
	var s string
	if err := db.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&s); err != nil {
		t.Fatal(err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatal(err)
	}
	return lsn
}

func waitConfirmed(t *testing.T, ctx context.Context, db *pgx.Conn, name string, past pglogrepl.LSN) {
	t.Helper()
	for confirmedFlush(t, ctx, db, name) <= past {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the slot to be confirmed")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// archived reads an archive table back as JSON rows in commit order.
func archived(t *testing.T, ctx context.Context, c config.Catalog, ident table.Identifier) (*table.Table, []map[string]any) {
	t.Helper()
	cat, err := archive.Open(ctx, "test", c)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	at, err := tbl.Scan().ToArrowTable(ctx)
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
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a["commit_lsn"] != b["commit_lsn"] {
			return a["commit_lsn"].(float64) < b["commit_lsn"].(float64)
		}
		return a["seq"].(float64) < b["seq"].(float64)
	})
	return tbl, rows
}

func TestArchiveEvolvesTheSchema(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)
	exec(t, ctx, db, fmt.Sprintf("ALTER TABLE %s ADD COLUMN n int, ADD COLUMN at timestamptz, ADD COLUMN data bytea", name))
	app := archiveApp(t, ctx, url, name, 1)
	stop := archiving(t, ctx, app)
	defer stop()

	run := func(statements ...string) {
		t.Helper()
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range statements {
			exec(t, ctx, tx, strings.ReplaceAll(s, "$t", name))
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	run(`SELECT pg_logical_emit_message(true, 'protobase_actor', '{"sub":"u42"}')`,
		`INSERT INTO $t VALUES (1, 'hi', 7, '2026-11-24 05:00:00.25+00', '\x00ff')`,
		`UPDATE $t SET body = 'hey' WHERE id = 1`)
	run(`ALTER TABLE $t RENAME COLUMN body TO content`,
		`SELECT pg_logical_emit_message(true, 'protobase_schema', '{"rename":{"schema":"public","table":"$t","from":"body","to":"content"}}')`,
		`ALTER TABLE $t ALTER COLUMN n TYPE bigint`,
		`INSERT INTO $t VALUES (2, 'second', 9000000000)`)
	run(`ALTER TABLE $t ALTER COLUMN n TYPE text USING n::text`,
		`INSERT INTO $t VALUES (3, 'third', 'three')`)
	last := currentLSN(t, ctx, db)
	run(`DELETE FROM $t WHERE id = 1`)
	waitConfirmed(t, ctx, db, name, last)

	tbl, rows := archived(t, ctx, app.Archive.Catalog, table.Identifier{name, "public", name})
	ops := []string{}
	for _, r := range rows {
		ops = append(ops, r["operation"].(string))
	}
	if strings.Join(ops, " ") != "insert update insert insert delete" {
		t.Fatalf("operations %v, rows %v", ops, rows)
	}
	newRow := func(i int) map[string]any { return rows[i]["new"].(map[string]any) }
	if rows[0]["actor"] != `{"sub":"u42"}` || rows[1]["actor"] != `{"sub":"u42"}` || rows[2]["actor"] != nil {
		t.Errorf("actors %v %v %v", rows[0]["actor"], rows[1]["actor"], rows[2]["actor"])
	}
	if newRow(0)["content"] != "hi" || newRow(1)["content"] != "hey" || newRow(2)["content"] != "second" {
		t.Errorf("the renamed column does not carry its values under its new name: %v", rows)
	}
	if newRow(0)["n__1"] != float64(7) || newRow(2)["n__1"] != float64(9000000000) || newRow(3)["n"] != "three" {
		t.Errorf("widened and replaced n: %v %v %v", newRow(0), newRow(2), newRow(3))
	}
	if newRow(0)["at"] != "2026-11-24T05:00:00.25Z" || newRow(0)["data"] != "AP8=" {
		t.Errorf("typed values %v %v", newRow(0)["at"], newRow(0)["data"])
	}
	deleted := rows[4]
	if deleted["new"] != nil || deleted["old"].(map[string]any)["id"] != float64(1) || deleted["key"].(map[string]any)["id"] != float64(1) {
		t.Errorf("delete row %v", deleted)
	}

	schema := tbl.Schema()
	content, _ := schema.FindFieldByName("new.content")
	n, _ := schema.FindFieldByName("new.n__1")
	if n.Type.String() != "long" {
		t.Errorf("new.n__1 is %s, want the promoted long", n.Type)
	}
	for _, old := range tbl.Metadata().Schemas() {
		if body, ok := old.FindFieldByName("new.body"); ok && body.ID != content.ID {
			t.Errorf("new.content has field id %d, new.body had %d", content.ID, body.ID)
		}
	}
}

func TestArchiveConfirmsOnlyCommittedSegments(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)

	// A segment that waits 15 minutes holds the change past a crash.
	app := archiveApp(t, ctx, url, name, config.MaxSegmentSeconds)
	stop := archiving(t, ctx, app)
	before := confirmedFlush(t, ctx, db, name)
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s VALUES (1, 'kept')", name))
	waitReported(t, ctx, db, name, currentLSN(t, ctx, db))
	if confirmed := confirmedFlush(t, ctx, db, name); confirmed > before {
		t.Fatalf("confirmed %s before the segment was committed", confirmed)
	}
	stop()

	// The next run reads the change again from the slot and archives it once.
	app = archiveApp(t, ctx, url, name, 1)
	stop = archiving(t, ctx, app)
	defer stop()
	waitConfirmed(t, ctx, db, name, before)
	_, rows := archived(t, ctx, app.Archive.Catalog, table.Identifier{name, "public", name})
	if len(rows) != 1 || rows[0]["new"].(map[string]any)["body"] != "kept" {
		t.Fatalf("archived %v", rows)
	}
}
