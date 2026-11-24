package tests

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

func TestStartupRefuses(t *testing.T) {
	url := os.Getenv("PCS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PCS_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)

	run := func(app config.App) error {
		r := reader.New(app, tail.New(1), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
