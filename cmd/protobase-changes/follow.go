package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
)

// feeds loads each app's keys and makes its feed, keyed by app name.
func feeds(cfg config.Config, log *slog.Logger) (map[string]*feed.Feed, error) {
	out := map[string]*feed.Feed{}
	for _, app := range cfg.Apps {
		ring, err := keys.Load(app.KeyFiles)
		if err != nil {
			return nil, fmt.Errorf("app %s: %w", app.Name, err)
		}
		f, err := feed.New(app, ring, log)
		if err != nil {
			return nil, err
		}
		out[app.Name] = f
	}
	return out, nil
}

// follow runs one reader per app into its feed until ctx ends or the first
// reader fails, which stops the others.
func follow(ctx context.Context, cfg config.Config, fs map[string]*feed.Feed, log *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, len(cfg.Apps))
	for _, app := range cfg.Apps {
		r := reader.New(app, fs[app.Name], log, nil)
		go func() {
			if err := r.Run(ctx); err != nil {
				errs <- fmt.Errorf("app %s: %w", app.Name, err)
				return
			}
			errs <- nil
		}()
	}

	var first error
	for range cfg.Apps {
		if err := <-errs; err != nil && first == nil {
			first = err
			cancel()
		}
	}
	return first
}
