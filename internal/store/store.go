// Package store keeps destination sets and state in Postgres, in a changes
// schema, or in a SQLite file, through one set of statements both run.
package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
)

// SQL is a destination store in a SQL database. It is safe for concurrent use.
type SQL struct {
	db *sql.DB
	d  dialect
}

// Open connects to the store the config names. It does not check the
// schema: Check does, and Migrate applies it.
func Open(cfg config.Store) (*SQL, error) {
	d, dsn, err := dialectOf(cfg)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(d.driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open the %s store: %w", cfg.Kind, err)
	}
	if cfg.Kind == config.SQLite {
		// One writer at a time is all SQLite takes; waiting in the pool is
		// plainer than retrying a busy database.
		db.SetMaxOpenConns(1)
	}
	return &SQL{db: db, d: d}, nil
}

func (s *SQL) Close() error { return s.db.Close() }

// table names a table of the store, in its schema where the dialect has one.
func (s *SQL) table(name string) string { return s.d.prefix + name }

// exec runs one statement, naming the store in its error.
func (s *SQL) exec(ctx context.Context, query string, args ...any) error {
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s store: %w", s.d.name, err)
	}
	return nil
}
