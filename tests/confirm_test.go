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

	"github.com/jackc/pgx/v5"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/confirm"
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/store"
)

// TestSlotWithoutArchiveStaysReserved follows an app without an archive
// into a destination while another database fills WAL segments under a
// max_slot_wal_keep_size of 1MB. The slot is confirmed past that WAL, so
// each checkpoint keeps it reserved, and after a restart the destination
// goes on from its cursor rather than starting again.
func TestSlotWithoutArchiveStaysReserved(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	other := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	exec(t, ctx, other, "CREATE DATABASE "+name)
	t.Cleanup(func() { exec(t, context.Background(), other, "DROP DATABASE "+name+" WITH (FORCE)") })
	exec(t, ctx, other, fmt.Sprintf("CREATE TABLE %s (body text)", name))
	t.Cleanup(func() { exec(t, context.Background(), other, "DROP TABLE "+name) })
	appURL := inDatabase(t, url, name)
	db := connect(t, ctx, appURL)
	setUp(t, ctx, db, name)
	s := postgresStore(t, ctx, db, appURL)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	// A short wal_sender_timeout brings keepalives and status updates every second.
	app := config.App{Name: "shop", DatabaseURL: withOption(appURL, "wal_sender_timeout=2s"), Slot: name, Publication: name,
		TailTransactions: 100, Issuer: issuer}
	rc := &hookReceiver{}
	srv := httptest.NewServer(rc)
	defer srv.Close()
	set := seal(t, map[string]any{"iss": issuer, "iat": time.Now().Unix(), "destinations": []map[string]any{
		{"name": "every", "url": srv.URL, "secret": webhookSecret, "payload": "raw",
			"subs": []map[string]any{{"resource": name}}, "retry": map[string]any{"at_limit": "pause"}},
	}})

	// Set before the service starts, which reads it then.
	exec(t, ctx, db, "ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
	exec(t, ctx, db, "SELECT pg_reload_conf()")
	t.Cleanup(func() {
		exec(t, context.Background(), db, "ALTER SYSTEM RESET max_slot_wal_keep_size")
		exec(t, context.Background(), db, "SELECT pg_reload_conf()")
	})
	stop := following(t, ctx, app, s, set)
	waitStates(t, ctx, db, 1)
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'one')", name))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'two')", name))
	waitDelivered(t, ctx, rc, 2)

	for range 3 {
		fillPastSlot(t, ctx, other, db, name)
		exec(t, ctx, other, "CHECKPOINT")
		var status string
		if err := db.QueryRow(ctx, "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "reserved" {
			t.Fatalf("the slot is %s after a checkpoint, want reserved", status)
		}
	}

	stop()
	waitInactive(t, ctx, db, name)
	stop = following(t, ctx, app, s, "")
	defer stop()
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (3, 'three')", name))
	waitDelivered(t, ctx, rc, 3)
	states, err := s.States(ctx, "shop")
	if err != nil {
		t.Fatal(err)
	}
	if got := states["every"].Status; got != destination.Active {
		t.Fatalf("the destination is %s after the restart, want active", got)
	}
	if got := rc.types(); len(got) != 3 {
		t.Fatalf("delivered %v, want each row once", got)
	}
}

// following runs the reader, the destinations and the confirmer of an app
// without an archive, putting set first unless it is empty, until the
// returned stop is called.
func following(t *testing.T, ctx context.Context, app config.App, s *store.SQL, set string) (stop func()) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := newFeed(t, app)
	r := reader.New(app, f, log, nil)
	hub := destination.NewHub(f, s, &destination.Sender{Client: http.DefaultClient, Now: time.Now}, log)
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, run := range []func(context.Context) error{r.Run, hub.Run, confirm.New(app.DatabaseURL, f, r, hub.Stored).Run} {
		wg.Go(func() {
			if err := run(ctx); !errors.Is(err, context.Canceled) {
				t.Errorf("stopped: %v", err)
			}
		})
	}
	stop = sync.OnceFunc(func() {
		cancel()
		wg.Wait()
	})
	select {
	case <-hub.Ready():
	case <-ctx.Done():
		stop()
		t.Fatal("timed out waiting for the hub to load")
	}
	if set != "" {
		if err := hub.Put(ctx, set); err != nil {
			stop()
			t.Fatal(err)
		}
	}
	return stop
}

// fillPastSlot starts a new WAL segment, then fills it from the other
// database, with a running-transactions record after each fill so the
// slot's restart position can follow its confirm, until the slot's restart
// position is in the segment the server writes.
func fillPastSlot(t *testing.T, ctx context.Context, other, db *pgx.Conn, name string) {
	t.Helper()
	exec(t, ctx, other, "SELECT pg_switch_wal()")
	for {
		exec(t, ctx, other, fmt.Sprintf("INSERT INTO %s SELECT repeat('x', 1000) FROM generate_series(1, 700)", name))
		exec(t, ctx, other, "SELECT pg_log_standby_snapshot()")
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			var caughtUp bool
			err := db.QueryRow(ctx, `SELECT pg_walfile_name(restart_lsn) = pg_walfile_name(pg_current_wal_lsn())
				FROM pg_replication_slots WHERE slot_name = $1`, name).Scan(&caughtUp)
			if err != nil {
				t.Fatal(err)
			}
			if caughtUp {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("timed out waiting for the slot to follow the WAL")
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

func waitDelivered(t *testing.T, ctx context.Context, rc *hookReceiver, n int) {
	t.Helper()
	for len(rc.types()) < n {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %d events, got %v", n, rc.types())
		case <-time.After(50 * time.Millisecond):
		}
	}
}
