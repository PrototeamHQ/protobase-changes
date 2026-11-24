package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/revoke"
	"github.com/PrototeamHQ/protobase-changes/internal/store"
)

// migrate applies each app database's schema, then each store's.
func migrate(ctx context.Context, cfg config.Config, ss map[string]*store.SQL) error {
	for _, app := range cfg.Apps {
		if err := migrateApp(ctx, app.DatabaseURL); err != nil {
			return fmt.Errorf("app %s database: %w", app.Name, err)
		}
	}
	return migrateStores(ctx, ss)
}

// migrateApp applies the part of the changes schema an app database holds.
func migrateApp(ctx context.Context, url string) error {
	db, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())
	return revoke.Migrate(ctx, db)
}

func migrateStores(ctx context.Context, ss map[string]*store.SQL) error {
	for app, s := range ss {
		if err := s.Migrate(ctx); err != nil {
			return fmt.Errorf("app %s: %w", app, err)
		}
	}
	return nil
}
