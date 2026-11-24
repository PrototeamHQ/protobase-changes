package revoke

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrSchema means the app database lacks Table or an index of it: run
// protobase-changes with --migrate first.
var ErrSchema = errors.New("the app database's changes schema is not up to date; run protobase-changes --migrate")

// migrations build the part of the changes schema that lives in the app
// database, apart from a store's, which may live elsewhere. They are versioned
// in changes.app_migrations. The revocations table has no key, as the app
// only inserts, so the whole row identifies it when the app deletes the rows
// that expired: the publication of all tables refuses deletes otherwise.
var migrations = []string{
	`CREATE TABLE changes.revocations (sid text NOT NULL, exp bigint NOT NULL);
	ALTER TABLE changes.revocations REPLICA IDENTITY FULL;
	CREATE INDEX revocations_exp ON changes.revocations (exp)`,
}

// Migrate applies the migrations the app database lacks, in one transaction,
// so a failing one leaves it as it was.
func Migrate(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS changes"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS changes.app_migrations (version integer PRIMARY KEY)"); err != nil {
		return err
	}
	applied, err := version(ctx, tx)
	if err != nil {
		return err
	}
	for i := applied; i < len(migrations); i++ {
		if _, err := tx.Exec(ctx, migrations[i]); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO changes.app_migrations (version) VALUES ($1)", i+1); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// check fails with ErrSchema unless every migration was applied. A schema
// migrated further, by a newer version during its deploy, still serves.
func check(ctx context.Context, tx pgx.Tx) error {
	var exists bool
	if err := tx.QueryRow(ctx, "SELECT to_regclass('changes.app_migrations') IS NOT NULL").Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrSchema
	}
	applied, err := version(ctx, tx)
	if err != nil {
		return err
	}
	if applied < len(migrations) {
		return ErrSchema
	}
	return nil
}

func version(ctx context.Context, tx pgx.Tx) (int, error) {
	var v *int
	if err := tx.QueryRow(ctx, "SELECT max(version) FROM changes.app_migrations").Scan(&v); err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	return *v, nil
}
