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

func TestStartupWantsMigratedStores(t *testing.T) {
	ctx := context.Background()
	path, cfg := sqliteConfig(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fs, err := feeds(cfg, log)
	if err != nil {
		t.Fatal(err)
	}

	ss, err := stores(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hubs(ctx, cfg, fs, ss, log); !errors.Is(err, store.ErrSchema) {
		t.Fatalf("started on a store without its schema: %v", err)
	}
	closeStores(ss)

	var out, errOut strings.Builder
	if err := run([]string{"--config", path, "--migrate"}, &out, &errOut); err != nil {
		t.Fatalf("migrate: %v %s", err, errOut.String())
	}
	ss, err = stores(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStores(ss)
	hs, err := hubs(ctx, cfg, fs, ss, log)
	if err != nil || hs["shop"] == nil {
		t.Fatalf("hubs after migrate: %v %v", hs, err)
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
