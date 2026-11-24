//go:build integration

// Package tests runs protobase-changes against a real Postgres, named by
// PCS_TEST_DATABASE_URL: a superuser on a server with wal_level = logical,
// and a real Iceberg REST catalog, PCS_TEST_CATALOG, that
// tests/catalog/start.sh starts and prints.
// Run with: go test -tags integration ./tests/
package tests

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/slot"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

func TestSlotReader(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	// A short wal_sender_timeout makes the server ask for status updates often.
	app := config.App{Name: "test", DatabaseURL: withOption(url, "wal_sender_timeout=1s"), Slot: name, Publication: name, TailTransactions: 100}
	setUp(t, ctx, db, name)

	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'before')", name))
	type snapshot struct {
		lsn  pglogrepl.LSN
		rows []string
	}
	snapshots := make(chan snapshot, 1)
	readSnapshot := func(ctx context.Context, s reader.Snapshot) error {
		conn := connect(t, ctx, url)
		tx, err := slot.BeginSnapshot(ctx, conn, s.Name)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT body FROM %s ORDER BY id", name))
		if err != nil {
			return err
		}
		bodies, err := pgx.CollectRows(rows, pgx.RowTo[string])
		snapshots <- snapshot{lsn: s.LSN, rows: bodies}
		return err
	}
	tl := tail.New(app.TailTransactions)
	r := reader.New(app, tl, slog.New(slog.NewTextHandler(io.Discard, nil)), readSnapshot)
	stop := start(t, ctx, r)

	var snap snapshot
	select {
	case snap = <-snapshots:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the snapshot")
	}
	if len(snap.rows) != 1 || snap.rows[0] != "before" {
		t.Fatalf("snapshot saw %v, want [before]", snap.rows)
	}
	snapshotLSN := snap.lsn

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, ctx, tx, `SELECT pg_logical_emit_message(true, 'protobase_actor', '{"sub":"u42"}')`)
	exec(t, ctx, tx, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'hi')", name))
	exec(t, ctx, tx, fmt.Sprintf("UPDATE %s SET body = 'hey' WHERE id = 2", name))
	exec(t, ctx, tx, fmt.Sprintf("DELETE FROM %s WHERE id = 1", name))
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// A rolled back transaction never reaches the stream.
	exec(t, ctx, db, fmt.Sprintf("BEGIN; INSERT INTO %s (id, body) VALUES (9, 'gone'); ROLLBACK", name))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (3, 'later')", name))

	ours := func() []*change.Transaction { return mine(tl, name) }
	waitFor(t, ctx, tl, func() bool { return len(ours()) == 2 })
	got := ours()
	first := got[0]
	if first.CommitLSN <= snapshotLSN || first.CommitTime.IsZero() || first.Xid == 0 {
		t.Fatalf("transaction header %+v, snapshot at %s", first, snapshotLSN)
	}
	if len(first.Messages) != 1 || first.Messages[0].Prefix != "protobase_actor" ||
		string(first.Messages[0].Content) != `{"sub":"u42"}` || first.Messages[0].Position != 0 {
		t.Fatalf("messages %+v", first.Messages)
	}
	table := change.Table{Schema: "public", Name: name}
	id := func(s string) change.Row { return change.Row{"id": text(s)} }
	want := []change.Change{
		{Table: table, Operation: change.Insert, Key: id("2"), NewRow: change.Row{"id": text("2"), "body": text("hi")}},
		{Table: table, Operation: change.Update, Key: id("2"), NewRow: change.Row{"id": text("2"), "body": text("hey")}},
		{Table: table, Operation: change.Delete, Key: id("1"), OldRowKind: change.OldKeyOnly, OldRow: id("1")},
	}
	assertChanges(t, first.Changes, want)
	assertChanges(t, got[1].Changes, []change.Change{
		{Table: table, Operation: change.Insert, Key: id("3"), NewRow: change.Row{"id": text("3"), "body": text("later")}},
	})

	waitReported(t, ctx, db, name, got[1].EndLSN)
	if confirmed := confirmedFlush(t, ctx, db, name); confirmed != snapshotLSN {
		t.Fatalf("slot confirmed up to %s, want it left at %s", confirmed, snapshotLSN)
	}

	// Running again resumes after the tail's newest transaction, without duplicates.
	stop()
	waitInactive(t, ctx, db, name)
	stop = start(t, ctx, r)
	defer stop()
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (4, 'resumed')", name))
	waitFor(t, ctx, tl, func() bool { return len(ours()) >= 3 })
	resumed := ours()
	if len(resumed) != 3 {
		t.Fatalf("tail holds %d of our transactions after resuming, want 3", len(resumed))
	}
	assertChanges(t, resumed[2].Changes, []change.Change{
		{Table: table, Operation: change.Insert, Key: id("4"), NewRow: change.Row{"id": text("4"), "body": text("resumed")}},
	})
}

func text(s string) change.Value { return change.Value{Kind: change.Text, Text: s} }

func mine(tl *tail.Tail, table string) []*change.Transaction {
	txs, _ := tl.Since(0)
	var out []*change.Transaction
	for _, tx := range txs {
		if len(tx.Changes) > 0 && tx.Changes[0].Table.Name == table {
			out = append(out, tx)
		}
	}
	return out
}
