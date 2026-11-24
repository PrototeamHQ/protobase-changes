//go:build integration

package tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	osexec "os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/store"
)

// TestMigrateFlag runs the built command with --migrate, twice, against an
// app whose database also holds its store: both schemas are applied, and the
// second run changes nothing.
func TestMigrateFlag(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	exec(t, ctx, db, "DROP SCHEMA IF EXISTS changes CASCADE")
	t.Cleanup(func() { exec(t, context.Background(), db, "DROP SCHEMA IF EXISTS changes CASCADE") })

	dir := t.TempDir()
	bin := filepath.Join(dir, "protobase-changes")
	if out, err := osexec.CommandContext(ctx, "go", "build", "-o", bin, "../cmd/protobase-changes").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	keyFile := filepath.Join(dir, "key")
	if err := os.WriteFile(keyFile, []byte(base64.RawURLEncoding.EncodeToString(testKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := json.Marshal(map[string]any{
		"listen": "127.0.0.1:0",
		"apps": []map[string]any{{
			"name": "shop", "database_url": url, "slot": "protobase_changes", "publication": "protobase_changes",
			"tail_transactions": 10, "issuer": issuer, "key_files": []string{keyFile},
			"store": map[string]any{"kind": "postgres", "url": url},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfgFile := filepath.Join(dir, "changes.json")
	if err := os.WriteFile(cfgFile, cfg, 0o600); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if out, err := osexec.CommandContext(ctx, bin, "--config", cfgFile, "--migrate").CombinedOutput(); err != nil {
			t.Fatalf("--migrate: %v\n%s", err, out)
		}
	}
	var revocations bool
	if err := db.QueryRow(ctx, "SELECT to_regclass('changes.revocations') IS NOT NULL").Scan(&revocations); err != nil {
		t.Fatal(err)
	}
	if !revocations {
		t.Fatal("--migrate left the app database without its revocations table")
	}
	s, err := store.Open(config.Store{Kind: config.Postgres, URL: url})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Check(ctx); err != nil {
		t.Fatalf("the store after --migrate: %v", err)
	}
}
