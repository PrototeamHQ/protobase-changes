//go:build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/serve"
	"github.com/PrototeamHQ/protobase-changes/internal/supervise"
)

// TestInvalidatedSlotIsCreatedAgain lets Postgres invalidate the slot at
// max_slot_wal_keep_size, past a revocation committed while the service was
// down, so the WAL that held it is gone. The restarted service creates the
// slot again and still refuses the revoked session, from the table the app
// wrote it to, while a fresh session streams.
func TestInvalidatedSlotIsCreatedAgain(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)
	app := config.App{Name: "shop", DatabaseURL: url, Slot: name, Publication: name, TailTransactions: 1000, Issuer: issuer}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	old := newFeed(t, app)

	stop := supervised(ctx, reader.New(app, old, log, nil), nil)
	waitFor(t, ctx, old, func() bool { _, err := old.Head(); return err == nil })
	oldSlot := confirmedFlush(t, ctx, db, name)
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'old')", name))
	waitFor(t, ctx, old, func() bool { return len(mine(t, old, name, oldSlot)) == 1 })
	held, _ := old.Since(oldSlot)
	oldCursor := held[len(held)-1].Cursor
	stop()
	waitInactive(t, ctx, db, name)

	revoked, fresh := name+"_revoked", name+"_fresh"
	exec(t, ctx, db, revocation(t, db, revoked))
	revokedAt := currentLSN(t, ctx, db)
	exec(t, ctx, db, "ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
	exec(t, ctx, db, "SELECT pg_reload_conf()")
	keepAll := func(ctx context.Context) {
		exec(t, ctx, db, "ALTER SYSTEM RESET max_slot_wal_keep_size")
		exec(t, ctx, db, "SELECT pg_reload_conf()")
	}
	t.Cleanup(func() { keepAll(context.Background()) })
	// Each round fills a WAL segment of its own, which the next checkpoint
	// removes past the 1MB the slot may keep.
	fillUntil(t, ctx, db, "Postgres to invalidate the slot", func() bool {
		var lost bool
		if err := db.QueryRow(ctx, "SELECT wal_status = 'lost' FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&lost); err != nil {
			t.Fatal(err)
		}
		return lost
	})
	keepAll(ctx)

	// A restart: the new feed knows no revocation from memory.
	f := newFeed(t, app)
	stop = supervised(ctx, reader.New(app, f, log, nil), nil)
	t.Cleanup(func() { stop() })
	newSlot := keptSlot(t, ctx, db, name)
	if newSlot <= revokedAt {
		t.Fatalf("the slot starts at %s, want past the revocation at %s", newSlot, revokedAt)
	}
	waitFor(t, ctx, f, func() bool { _, err := f.Head(); return err == nil })

	srv := httptest.NewServer(serve.New(map[string]*feed.Feed{"shop": f}, nil).Handler())
	defer srv.Close()
	token := func(sid string) string {
		return seal(t, map[string]any{
			"iss": issuer, "sub": "u42", "sid": sid, "exp": time.Now().Add(time.Hour).Unix(),
			"payload": "raw", "subs": []map[string]any{{"resource": name}},
		})
	}
	if _, status := pull(t, ctx, srv.URL, token(revoked), "", time.Now().Add(5*time.Second)); status != http.StatusUnauthorized {
		t.Fatalf("the revoked session got %d on the new slot, want 401", status)
	}
	if _, status := pull(t, ctx, srv.URL, token(fresh), oldCursor, time.Now().Add(5*time.Second)); status != http.StatusGone {
		t.Fatalf("a cursor of the invalidated slot got %d, want 410", status)
	}
	head, err := f.Head()
	if err != nil {
		t.Fatal(err)
	}
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'new')", name))
	got := collect(t, ctx, srv.URL, token(fresh), f.Cursors.Seal(head), 1)
	if body := got[0].Data.Changes[0].NewRow["body"]; body != "new" {
		t.Fatalf("the fresh session got %+v, want the new row", body)
	}
}

// supervised runs r again after each failure, which it sends to failures
// while there is room, until the returned stop is called.
func supervised(ctx context.Context, r *reader.Reader, failures chan<- error) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		supervise.Run(ctx, supervise.Backoff{First: 50 * time.Millisecond, Max: time.Second}, r.Run, func(err error) {
			select {
			case failures <- err:
			default:
			}
		})
	}()
	return sync.OnceFunc(func() {
		cancel()
		<-done
	})
}

// fillUntil fills WAL segments, which the next checkpoint removes past what
// the slot may keep, until done.
func fillUntil(t *testing.T, ctx context.Context, db *pgx.Conn, what string, done func() bool) {
	t.Helper()
	for !done() {
		for range 3 {
			exec(t, ctx, db, "SELECT pg_logical_emit_message(false, 'filler', 'x')")
			exec(t, ctx, db, "SELECT pg_switch_wal()")
		}
		exec(t, ctx, db, "CHECKPOINT")
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// keptSlot waits until the slot exists and Postgres keeps its WAL, then
// returns where it starts.
func keptSlot(t *testing.T, ctx context.Context, db *pgx.Conn, name string) pglogrepl.LSN {
	t.Helper()
	for {
		var lsn string
		err := db.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1 AND wal_status <> 'lost' AND confirmed_flush_lsn IS NOT NULL", name).Scan(&lsn)
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
