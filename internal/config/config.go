// Package config reads the apps protobase-changes follows.
package config

import (
	"encoding/json"
	"fmt"
	"io"
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
}

type Config struct {
	Apps []App `json:"apps"`
}

// identifier matches a slot or publication name that needs no quoting.
var identifier = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

// Parse reads a JSON config, refusing unknown fields and invalid apps.
func Parse(r io.Reader) (Config, error) {
	var c Config
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("config: %w", err)
	}
	if dec.More() {
		return c, fmt.Errorf("config: unexpected data after the JSON object")
	}
	return c, c.validate()
}

func (c Config) validate() error {
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
			return fmt.Errorf("config: app %q slot must be 1 to 63 lowercase letters, digits or underscores", app.Name)
		}
		if !identifier.MatchString(app.Publication) {
			return fmt.Errorf("config: app %q publication must be 1 to 63 lowercase letters, digits or underscores", app.Name)
		}
		if app.TailTransactions < 1 {
			return fmt.Errorf("config: app %q tail_transactions must be at least 1", app.Name)
		}
	}
	return nil
}
