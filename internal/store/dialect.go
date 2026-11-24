package store

import (
	"fmt"
	"net/url"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
)

// dialect is what differs between the databases: the driver, where the
// tables live, and the schema's migrations. Statements are the same.
type dialect struct {
	name   string
	driver string
	// prefix qualifies table names with their schema.
	prefix string
	// migrations are applied in order, each once.
	migrations []string
}

// tables makes the store's tables under prefix. Times are milliseconds
// since the epoch and positions Postgres' LSN text, so both databases read
// and write them alike.
func tables(prefix string) string {
	return `
CREATE TABLE ` + prefix + `definitions (
  app text PRIMARY KEY,
  token text NOT NULL
);
CREATE TABLE ` + prefix + `states (
  app text NOT NULL,
  destination text NOT NULL,
  system_id text NOT NULL,
  timeline bigint NOT NULL,
  lsn text NOT NULL,
  status text NOT NULL,
  batch_end text,
  batch_id text,
  batch_tried integer,
  batch_next_ms bigint,
  batch_error text,
  PRIMARY KEY (app, destination)
);
CREATE TABLE ` + prefix + `dead_letters (
  app text NOT NULL,
  destination text NOT NULL,
  batch_id text NOT NULL,
  events text NOT NULL,
  error text NOT NULL,
  tried integer NOT NULL,
  failed_at_ms bigint NOT NULL,
  PRIMARY KEY (app, destination, batch_id)
);`
}

var (
	postgres = dialect{
		name: "postgres", driver: "pgx", prefix: "changes.",
		migrations: []string{tables("changes.")},
	}
	sqlite = dialect{
		name: "sqlite", driver: "sqlite",
		migrations: []string{tables("")},
	}
)

func dialectOf(cfg config.Store) (dialect, string, error) {
	switch cfg.Kind {
	case config.Postgres:
		return postgres, cfg.URL, nil
	case config.SQLite:
		dsn := "file:" + cfg.Path + "?" + url.Values{"_pragma": {"busy_timeout(5000)", "journal_mode(WAL)"}}.Encode()
		return sqlite, dsn, nil
	}
	return dialect{}, "", fmt.Errorf("store kind %q is not postgres or sqlite", cfg.Kind)
}
