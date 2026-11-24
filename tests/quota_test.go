//go:build integration

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

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
)

// quotaReader streams a new table at one change per second into a feed that
// has caught up, and returns the table, the feed and where the slot was.
func quotaReader(t *testing.T, ctx context.Context) (db *pgx.Conn, name string, f *feed.Feed, from pglogrepl.LSN) {
	t.Helper()
	url := databaseURL(t)
	db = connect(t, ctx, url)
	name = fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)
	perSecond := 1
	// A wal_sender_timeout shorter than the quota's wait shows the stream
	// still answers the server while it is held back.
	app := config.App{Name: "test", DatabaseURL: withOption(url, "wal_sender_timeout=1s"), Slot: name, Publication: name,
		TailTransactions: 100, Quotas: config.Quotas{ChangesPerSecond: &perSecond}}
	f = newFeed(t, app)
	t.Cleanup(start(t, ctx, reader.New(app, f, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)))
	waitFor(t, ctx, f, func() bool { _, err := f.Head(); return err == nil })
	return db, name, f, confirmedFlush(t, ctx, db, name)
}

func TestChangesPerSecondHoldsTheStreamBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, name, f, from := quotaReader(t, ctx)

	began := time.Now()
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')", name))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (4, 'd')", name))
	waitFor(t, ctx, f, func() bool { return len(mine(t, f, name, from)) == 2 })
	// The first transaction's 3 changes take the 1-change burst and 2s more.
	if took := time.Since(began); took < 2*time.Second {
		t.Fatalf("both transactions came within %v, want the second held back 2s", took)
	}
	// The connection outlived the wait: a timed out one fails the reader here.
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (8, 'h')", name))
	waitFor(t, ctx, f, func() bool { return len(mine(t, f, name, from)) == 3 })
}

func TestChangesPerSecondHoldsATransactionBeforeTakingItIn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, name, f, from := quotaReader(t, ctx)

	began := time.Now()
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'a')", name))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'b')", name))
	// The burst of one change takes the first in at once, and the second
	// waits its turn before the feed holds it: never more than the burst.
	waitFor(t, ctx, f, func() bool { return len(mine(t, f, name, from)) >= 1 })
	if got := len(mine(t, f, name, from)); got != 1 {
		t.Fatalf("the feed took in %d transactions at once, want 1", got)
	}
	waitFor(t, ctx, f, func() bool { return len(mine(t, f, name, from)) == 2 })
	if took := time.Since(began); took < time.Second {
		t.Fatalf("the second transaction came within %v, want it held back 1s", took)
	}
}
