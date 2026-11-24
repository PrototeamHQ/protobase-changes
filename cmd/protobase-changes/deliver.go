package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/store"
	"github.com/PrototeamHQ/protobase-changes/internal/supervise"
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

// deliver runs every hub until ctx ends, also when there are none. A hub that
// fails runs again after a wait, on its own, so one app's store holds back no
// other app.
func deliver(ctx context.Context, hs map[string]*destination.Hub, log *slog.Logger) error {
	var wg sync.WaitGroup
	for app, h := range hs {
		wg.Go(func() {
			supervise.Run(ctx, supervise.Default, h.Run, func(err error) {
				log.Error("delivery stopped; delivering again after a wait", "app", app, "error", err.Error())
			})
		})
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}
