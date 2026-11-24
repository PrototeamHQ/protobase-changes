// Package config reads the apps protobase-changes follows.
package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
)

// App is one app database to follow.
type App struct {
	Name        string `json:"name"`
	DatabaseURL string `json:"database_url"`
	// Slot is the logical replication slot, created on first start.
	Slot string `json:"slot"`
	// Publication must exist and publish every change of all tables.
	Publication string `json:"publication"`
	// TailTransactions bounds how many committed transactions stay in memory.
	TailTransactions int `json:"tail_transactions"`
	// Archive is optional: without it, nothing confirms the slot.
	Archive *Archive `json:"archive"`
	// Issuer is the iss every token of the app carries, and the source of its events.
	Issuer string `json:"issuer"`
	// KeyFiles hold the keys shared with the app: the first encrypts, every one decrypts.
	KeyFiles []string `json:"key_files"`
	// Store keeps the app's destinations and their cursors; without one the
	// app has no destinations.
	Store *Store `json:"store"`
	// FunctionsURL is where the app's API functions are served, which
	// function destinations call by name.
	FunctionsURL string `json:"functions_url"`
}

// StoreKind names the database a store keeps its state in.
type StoreKind string

const (
	Postgres StoreKind = "postgres"
	SQLite   StoreKind = "sqlite"
)

// Store is where destination definitions and state are kept: a Postgres
// database, in a changes schema, or a SQLite file.
type Store struct {
	Kind StoreKind `json:"kind"`
	// URL is the Postgres database, for kind postgres.
	URL string `json:"url"`
	// Path is the SQLite file, for kind sqlite.
	Path string `json:"path"`
}

type Config struct {
	// Listen is the address the pull endpoints serve on, such as ":8080".
	Listen string `json:"listen"`
	Apps   []App  `json:"apps"`
}

// identifier matches a slot or publication name that needs no quoting in a
// replication command: it must not start with a digit, which reads as a number.
var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Parse reads a JSON config, refusing unknown fields and invalid apps.
func Parse(r io.Reader) (Config, error) {
	var c Config
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("config: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return c, fmt.Errorf("config: unexpected data after the JSON object")
	}
	return c, c.validate()
}

func (c Config) validate() error {
	if c.Listen == "" {
		return fmt.Errorf("config: listen is empty")
	}
	if len(c.Apps) == 0 {
		return fmt.Errorf("config: apps lists no app")
	}
	names := map[string]bool{}
	for i, app := range c.Apps {
		if app.Name == "" {
			return fmt.Errorf("config: apps[%d] has no name", i)
		}
		if names[app.Name] {
			return fmt.Errorf("config: app %q is listed twice", app.Name)
		}
		names[app.Name] = true
		if app.DatabaseURL == "" {
			return fmt.Errorf("config: app %q has no database_url", app.Name)
		}
		if !identifier.MatchString(app.Slot) {
			return fmt.Errorf("config: app %q slot must be 1 to 63 lowercase letters, digits or underscores, not starting with a digit", app.Name)
		}
		if !identifier.MatchString(app.Publication) {
			return fmt.Errorf("config: app %q publication must be 1 to 63 lowercase letters, digits or underscores, not starting with a digit", app.Name)
		}
		if app.TailTransactions < 1 {
			return fmt.Errorf("config: app %q tail_transactions must be at least 1", app.Name)
		}
		if app.Issuer == "" {
			return fmt.Errorf("config: app %q has no issuer", app.Name)
		}
		if len(app.KeyFiles) == 0 {
			return fmt.Errorf("config: app %q lists no key_files", app.Name)
		}
		if app.Archive != nil {
			if err := app.Archive.validate(app.Name); err != nil {
				return err
			}
		}
		if err := app.Store.validate(); err != nil {
			return fmt.Errorf("config: app %q %w", app.Name, err)
		}
		if app.FunctionsURL != "" && !absoluteHTTP(app.FunctionsURL) {
			return fmt.Errorf("config: app %q functions_url must be an absolute http or https URL", app.Name)
		}
	}
	return nil
}

func (s *Store) validate() error {
	if s == nil {
		return nil
	}
	switch {
	case s.Kind == Postgres && s.URL != "" && s.Path == "":
		return nil
	case s.Kind == SQLite && s.Path != "" && s.URL == "":
		return nil
	}
	return fmt.Errorf("store must be {\"kind\": \"postgres\", \"url\": ...} or {\"kind\": \"sqlite\", \"path\": ...}")
}

func absoluteHTTP(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
