package tests

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func connect(t *testing.T, ctx context.Context, url string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func exec(t *testing.T, ctx context.Context, db execer, sql string) {
	t.Helper()
	if _, err := db.Exec(ctx, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// setUp makes a table and a publication of all tables, both named name, and
// removes them and the slot of that name afterwards.
func setUp(t *testing.T, ctx context.Context, db *pgx.Conn, name string) {
	exec(t, ctx, db, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, body text)", name))
	exec(t, ctx, db, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", name))
	t.Cleanup(func() {
		ctx := context.Background()
		waitInactive(t, ctx, db, name)
		exec(t, ctx, db, fmt.Sprintf("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s'", name))
		exec(t, ctx, db, "DROP PUBLICATION "+name)
		exec(t, ctx, db, "DROP TABLE "+name)
	})
}

// start runs the reader until the returned stop is called, failing the test
// if it stops on its own.
func start(t *testing.T, ctx context.Context, r *reader.Reader) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("reader stopped: %v", err)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// waitFor waits for cond, checking it on every append to the tail and every 50ms.
func waitFor(t *testing.T, ctx context.Context, tl *tail.Tail, cond func() bool) {
	t.Helper()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out")
		case <-tl.Changed():
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitInactive waits until no connection holds the slot.
func waitInactive(t *testing.T, ctx context.Context, db *pgx.Conn, name string) {
	t.Helper()
	for {
		var active bool
		err := db.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&active)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && !active {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the slot to be released")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitReported waits until the reader's status updates report lsn as received.
func waitReported(t *testing.T, ctx context.Context, db *pgx.Conn, name string, lsn pglogrepl.LSN) {
	t.Helper()
	for {
		var reported bool
		err := db.QueryRow(ctx, `SELECT coalesce(r.write_lsn >= $2::pg_lsn, false) FROM pg_stat_replication r
			JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1`, name, lsn.String()).Scan(&reported)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		if reported {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for a status update")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// withOption adds a server setting to a database URL.
func withOption(rawURL, setting string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	q := u.Query()
	q.Set("options", "-c "+setting)
	// The connection string parser reads + literally, not as a space.
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	return u.String()
}

func confirmedFlush(t *testing.T, ctx context.Context, db *pgx.Conn, name string) pglogrepl.LSN {
	t.Helper()
	var lsn string
	if err := db.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&lsn); err != nil {
		t.Fatal(err)
	}
	parsed, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func assertChanges(t *testing.T, got, want []change.Change) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("changes\n got %+v\nwant %+v", got, want)
	}
}
