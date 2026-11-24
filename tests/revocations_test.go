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

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/revoke"
	"github.com/PrototeamHQ/protobase-changes/internal/stream"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

// TestRestartPastARevocationStillRefusesIt confirms the slot past a
// revocation, so a restart's stream never reads it from the WAL again: the
// revocation still holds, read from the table.
func TestRestartPastARevocationStillRefusesIt(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)
	app := config.App{Name: "shop", DatabaseURL: url, Slot: name, Publication: name, TailTransactions: 100, Issuer: issuer}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	first := newFeed(t, app)
	r := reader.New(app, first, log, nil)
	stop := start(t, ctx, r)
	waitFor(t, ctx, first, func() bool { _, err := first.Head(); return err == nil })
	sid := name + "_revoked"
	exec(t, ctx, db, fmt.Sprintf(`BEGIN; %s; INSERT INTO %s (id, body) VALUES (1, 'one'); COMMIT`, revocation(t, db, sid), name))
	waitFor(t, ctx, first, func() bool { return first.Revoked.Revoked(sid, time.Now()) })
	head, err := first.Head()
	if err != nil {
		t.Fatal(err)
	}
	r.Confirm(head.LSN)
	waitConfirmed(t, ctx, db, name, head.LSN-1)
	stop()

	f := newFeed(t, app)
	t.Cleanup(start(t, ctx, reader.New(app, f, log, nil)))
	waitFor(t, ctx, f, func() bool { _, err := f.Head(); return err == nil })
	tok := token.Token{Session: sid, Expiry: time.Now().Add(time.Hour)}
	if _, err := stream.Open(f, tok, "", time.Now); !errors.Is(err, stream.ErrRevoked) {
		t.Fatalf("the revoked session opened a stream after a restart: %v", err)
	}
}

// TestUnmigratedAppDatabaseIsNotServed follows an app database that lacks
// the revocations table: the service cannot know its revocations, so the
// feed stays unready until --migrate has run.
func TestUnmigratedAppDatabaseIsNotServed(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	exec(t, ctx, admin, "CREATE DATABASE "+name)
	t.Cleanup(func() { exec(t, context.Background(), admin, "DROP DATABASE "+name+" WITH (FORCE)") })
	appURL := inDatabase(t, url, name)
	db := connect(t, ctx, appURL)
	exec(t, ctx, db, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, body text)", name))
	exec(t, ctx, db, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", name))
	t.Cleanup(func() {
		ctx := context.Background()
		waitInactive(t, ctx, db, name)
		exec(t, ctx, db, fmt.Sprintf("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s'", name))
	})
	app := config.App{Name: "shop", DatabaseURL: appURL, Slot: name, Publication: name, TailTransactions: 100, Issuer: issuer}
	f := newFeed(t, app)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	if err := reader.New(app, f, log, nil).Run(ctx); !errors.Is(err, revoke.ErrSchema) {
		t.Fatalf("ran on an app database without the revocations table: %v", err)
	}
	if _, err := f.Head(); !errors.Is(err, feed.ErrNotReady) {
		t.Fatalf("head without the revocations table: %v, want feed.ErrNotReady", err)
	}
	var slots int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if slots != 0 {
		t.Fatal("the new slot stayed, so the next run would export no snapshot")
	}

	// Migrating again changes nothing.
	migrateApp(t, ctx, db)
	migrateApp(t, ctx, db)
	t.Cleanup(start(t, ctx, reader.New(app, f, log, nil)))
	waitFor(t, ctx, f, func() bool { _, err := f.Head(); return err == nil })
}
