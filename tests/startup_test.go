//go:build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
)

func TestStartupRefuses(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)

	run := func(app config.App) error {
		r := reader.New(app, newFeed(t, app), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
		return r.Run(ctx)
	}
	app := config.App{Name: "test", DatabaseURL: url, Slot: name, Publication: name + "_missing", TailTransactions: 1}
	if err := run(app); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("missing publication: %v", err)
	}

	exec(t, ctx, db, fmt.Sprintf("CREATE PUBLICATION %s_inserts FOR ALL TABLES WITH (publish = 'insert')", name))
	t.Cleanup(func() { exec(t, context.Background(), db, "DROP PUBLICATION "+name+"_inserts") })
	app.Publication = name + "_inserts"
	if err := run(app); err == nil || !strings.Contains(err.Error(), "does not publish update") {
		t.Fatalf("insert-only publication: %v", err)
	}

	exec(t, ctx, db, fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'test_decoding')", name))
	app.Publication = name
	if err := run(app); err == nil || !strings.Contains(err.Error(), "is not a logical pgoutput slot") {
		t.Fatalf("slot of another plugin: %v", err)
	}
	if confirmed := confirmedFlush(t, ctx, db, name); confirmed == 0 {
		t.Fatal("the slot of another plugin was dropped")
	}
}

func TestFailedSnapshotDropsTheNewSlot(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)
	app := config.App{Name: "test", DatabaseURL: url, Slot: name, Publication: name, TailTransactions: 1}

	failing := func(context.Context, reader.Snapshot) error { return errors.New("import failed") }
	r := reader.New(app, newFeed(t, app), slog.New(slog.NewTextHandler(io.Discard, nil)), failing)
	if err := r.Run(ctx); err == nil || !strings.Contains(err.Error(), "import failed") {
		t.Fatalf("failing snapshot hook: %v", err)
	}
	var slots int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if slots != 0 {
		t.Fatal("the slot stayed after its snapshot failed, so the next run would skip the initial state")
	}
}
