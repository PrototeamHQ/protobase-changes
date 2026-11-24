package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/store/storetest"
)

func openSQLite(t *testing.T) *SQL {
	t.Helper()
	s, err := Open(config.Store{Kind: config.SQLite, Path: filepath.Join(t.TempDir(), "changes.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCheckWantsMigrate(t *testing.T) {
	ctx := context.Background()
	s := openSQLite(t)
	if err := s.Check(ctx); !errors.Is(err, ErrSchema) {
		t.Fatalf("check before migrate: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate again: %v", err)
	}
	if err := s.Check(ctx); err != nil {
		t.Fatalf("check after migrate: %v", err)
	}
}

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openSQLite(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	storetest.RoundTrip(t, s)
}

func TestPathsKeepTheirOddCharacters(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, name := range []string{"shop#west.db", "shop#east.db", "a b?c.db"} {
		s, err := Open(config.Store{Kind: config.SQLite, Path: filepath.Join(dir, name)})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := s.PutDefinitions(ctx, "shop", name); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	for _, name := range []string{"shop#west.db", "shop#east.db", "a b?c.db"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		s, _ := Open(config.Store{Kind: config.SQLite, Path: filepath.Join(dir, name)})
		raw, err := s.Definitions(ctx, "shop")
		s.Close()
		if err != nil || raw != name {
			t.Fatalf("%s holds %q %v", name, raw, err)
		}
	}
}
