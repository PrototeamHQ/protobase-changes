package store

import (
	"context"
	"errors"
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
