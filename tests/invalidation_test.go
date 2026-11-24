//go:build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/stream"
	"github.com/PrototeamHQ/protobase-changes/internal/supervise"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

// TestInvalidatedSlotIsCreatedAgain lets Postgres invalidate the slot at
// max_slot_wal_keep_size while the reader streams from it.
func TestInvalidatedSlotIsCreatedAgain(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)
	app := config.App{Name: "test", DatabaseURL: url, Slot: name, Publication: name, TailTransactions: 1000}
	f := newFeed(t, app)
	r := reader.New(app, f, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		supervise.Run(runCtx, supervise.Backoff{First: 50 * time.Millisecond, Max: time.Second}, r.Run, func(error) {})
	}()
	t.Cleanup(func() {
		stop()
		<-done
	})

	waitFor(t, ctx, f, func() bool { _, err := f.Head(); return err == nil })
	oldSlot := confirmedFlush(t, ctx, db, name)
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'old')", name))
	waitFor(t, ctx, f, func() bool { return len(mine(t, f, name, oldSlot)) == 1 })
	old, _ := f.Since(oldSlot)
	oldCursor := old[len(old)-1].Cursor

	exec(t, ctx, db, "ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
	exec(t, ctx, db, "SELECT pg_reload_conf()")
	keepAll := func(ctx context.Context) {
		exec(t, ctx, db, "ALTER SYSTEM RESET max_slot_wal_keep_size")
		exec(t, ctx, db, "SELECT pg_reload_conf()")
	}
	t.Cleanup(func() { keepAll(context.Background()) })
	// Each round fills a WAL segment of its own, which the next checkpoint
	// removes past the 1MB the slot may keep.
	tok := token.Token{Expiry: time.Now().Add(time.Hour)}
	for {
		_, err := stream.Open(f, tok, oldCursor, time.Now)
		if errors.Is(err, stream.ErrGone) {
			break
		}
		for range 3 {
			exec(t, ctx, db, "SELECT pg_logical_emit_message(false, 'filler', 'x')")
			exec(t, ctx, db, "SELECT pg_switch_wal()")
		}
		exec(t, ctx, db, "CHECKPOINT")
		select {
		case <-ctx.Done():
			t.Fatalf("the old cursor still opens: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// The rounds may have invalidated the new slot too, so it may be created
	// once more: wait for one that holds its WAL.
	keepAll(ctx)
	newSlot := keptSlot(t, ctx, db, name)
	if newSlot <= oldSlot {
		t.Fatalf("the slot starts at %s, want past the invalidated one at %s", newSlot, oldSlot)
	}
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'new')", name))
	waitFor(t, ctx, f, func() bool { return len(mine(t, f, name, newSlot)) == 1 })
	got := mine(t, f, name, newSlot)[0].Changes[0].NewRow["body"]
	if got != text("new") {
		t.Fatalf("the new slot streamed %+v, want the new row", got)
	}
}

// keptSlot waits until the slot exists and Postgres keeps its WAL, then
// returns where it starts.
func keptSlot(t *testing.T, ctx context.Context, db *pgx.Conn, name string) pglogrepl.LSN {
	t.Helper()
	for {
		var lsn string
		err := db.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1 AND wal_status <> 'lost'", name).Scan(&lsn)
		if err == nil {
			parsed, err := pglogrepl.ParseLSN(lsn)
			if err != nil {
				t.Fatal(err)
			}
			return parsed
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the slot to be created again")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
