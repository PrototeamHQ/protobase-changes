package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/store"
)

// sqliteConfig writes a config of one app with a SQLite store and returns its path.
func sqliteConfig(t *testing.T) (path string, cfg config.Config) {
	t.Helper()
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "key")
	if err := os.WriteFile(keyFile, []byte(base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, keys.Size))), 0o600); err != nil {
		t.Fatal(err)
	}
	text := `{"listen": "127.0.0.1:0", "apps": [{"name": "shop", "database_url": "postgres://localhost/shop",
	  "slot": "protobase_changes", "publication": "protobase_changes", "tail_transactions": 10,
	  "issuer": "https://shop.example", "key_files": ["` + keyFile + `"],
	  "store": {"kind": "sqlite", "path": "` + filepath.Join(dir, "changes.db") + `"}}]}`
	path = filepath.Join(dir, "changes.json")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	return path, cfg
}

func TestAnUnmigratedStoreHoldsBackOnlyItsHub(t *testing.T) {
	ctx := context.Background()
	_, cfg := sqliteConfig(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fs, err := feeds(cfg, log)
	if err != nil {
		t.Fatal(err)
	}

	ss, err := stores(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hs := hubs(cfg, fs, ss, log)
	if err := hs["shop"].Run(ctx); !errors.Is(err, store.ErrSchema) {
		t.Fatalf("ran on a store without its schema: %v", err)
	}
	defer closeStores(ss)

	// The app databases --migrate applies first need Postgres; the
	// integration tests cover them.
	if err := migrateStores(ctx, ss); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	hs = hubs(cfg, fs, ss, log)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- hs["shop"].Run(runCtx) }()
	select {
	case <-hs["shop"].Ready():
	case err := <-done:
		t.Fatalf("run after migrate ended before it loaded: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the hub to load")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run after migrate: %v", err)
	}
}

func TestDeliverWithoutHubsRunsUntilCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- deliver(ctx, nil, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("returned %v before the context ended", err)
		default:
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("returned %v, want context.Canceled", err)
		}
	})
}
