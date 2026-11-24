package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/store"
)

// stores opens the store of each app that keeps one, keyed by app name.
func stores(cfg config.Config) (map[string]*store.SQL, error) {
	out := map[string]*store.SQL{}
	for _, app := range cfg.Apps {
		if app.Store == nil {
			continue
		}
		s, err := store.Open(*app.Store)
		if err != nil {
			closeStores(out)
			return nil, fmt.Errorf("app %s: %w", app.Name, err)
		}
		out[app.Name] = s
	}
	return out, nil
}

func closeStores(ss map[string]*store.SQL) {
	for _, s := range ss {
		s.Close()
	}
}

// migrate applies each store's schema.
func migrate(ctx context.Context, ss map[string]*store.SQL) error {
	for app, s := range ss {
		if err := s.Migrate(ctx); err != nil {
			return fmt.Errorf("app %s: %w", app, err)
		}
	}
	return nil
}

// hubs checks each store's schema and loads its app's destinations.
func hubs(ctx context.Context, cfg config.Config, fs map[string]*feed.Feed, ss map[string]*store.SQL, log *slog.Logger) (map[string]*destination.Hub, error) {
	client := &http.Client{}
	out := map[string]*destination.Hub{}
	for _, app := range cfg.Apps {
		s, ok := ss[app.Name]
		if !ok {
			continue
		}
		if err := s.Check(ctx); err != nil {
			return nil, fmt.Errorf("app %s: %w", app.Name, err)
		}
		sender := &destination.Sender{Client: client, FunctionsURL: app.FunctionsURL, Now: time.Now}
		h, err := destination.NewHub(ctx, fs[app.Name], s, sender, log)
		if err != nil {
			return nil, err
		}
		out[app.Name] = h
	}
	return out, nil
}

// deliver runs every hub until ctx ends or the first fails, which stops the others.
func deliver(ctx context.Context, hs map[string]*destination.Hub) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(hs))
	for _, h := range hs {
		go func() { errs <- h.Run(ctx) }()
	}
	var first error
	for range hs {
		if err := <-errs; err != nil && !errors.Is(err, context.Canceled) && first == nil {
			first = err
			cancel()
		}
	}
	if first != nil {
		return first
	}
	<-ctx.Done()
	return ctx.Err()
}
