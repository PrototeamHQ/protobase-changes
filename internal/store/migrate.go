package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrSchema means the store's schema is missing or older than this version
// needs: run protobase-changes with --migrate first.
var ErrSchema = errors.New("the store's schema is not up to date; run protobase-changes --migrate")

// Migrate applies the migrations the store lacks, in one transaction, so a
// failing one leaves the store as it was.
func (s *SQL) Migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s store: %w", s.d.name, err)
	}
	defer tx.Rollback()
	if s.d.prefix != "" {
		if _, err := tx.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS changes"); err != nil {
			return fmt.Errorf("%s store: %w", s.d.name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+s.table("migrations")+" (version integer PRIMARY KEY)"); err != nil {
		return fmt.Errorf("%s store: %w", s.d.name, err)
	}
	applied, err := version(ctx, tx, s.table("migrations"))
	if err != nil {
		return fmt.Errorf("%s store: %w", s.d.name, err)
	}
	for i := applied; i < len(s.d.migrations); i++ {
		if _, err := tx.ExecContext(ctx, s.d.migrations[i]); err != nil {
			return fmt.Errorf("%s store: migration %d: %w", s.d.name, i+1, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.table("migrations")+" (version) VALUES ($1)", i+1); err != nil {
			return fmt.Errorf("%s store: %w", s.d.name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s store: %w", s.d.name, err)
	}
	return nil
}

// Check fails with ErrSchema unless every migration was applied. A schema
// migrated further, by a newer version during its deploy, still serves.
func (s *SQL) Check(ctx context.Context) error {
	var exists bool
	query := "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'migrations')"
	if s.d.prefix != "" {
		query = "SELECT to_regclass('changes.migrations') IS NOT NULL"
	}
	if err := s.db.QueryRowContext(ctx, query).Scan(&exists); err != nil {
		return fmt.Errorf("%s store: %w", s.d.name, err)
	}
	if !exists {
		return ErrSchema
	}
	applied, err := version(ctx, s.db, s.table("migrations"))
	if err != nil {
		return fmt.Errorf("%s store: %w", s.d.name, err)
	}
	if applied < len(s.d.migrations) {
		return ErrSchema
	}
	return nil
}

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func version(ctx context.Context, q querier, table string) (int, error) {
	var v sql.NullInt64
	if err := q.QueryRowContext(ctx, "SELECT max(version) FROM "+table).Scan(&v); err != nil {
		return 0, err
	}
	return int(v.Int64), nil
}
