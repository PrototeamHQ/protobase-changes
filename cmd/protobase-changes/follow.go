package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/supervise"
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

// follow runs one reader per app into its feed until ctx ends. A reader that
// fails reads again after a wait, on its own, so one app's database holds
// back no other app.
func follow(ctx context.Context, cfg config.Config, fs map[string]*feed.Feed, log *slog.Logger) error {
	var wg sync.WaitGroup
	for _, app := range cfg.Apps {
		r := reader.New(app, fs[app.Name], log, nil)
		wg.Go(func() {
			supervise.Run(ctx, supervise.Default, r.Run, func(err error) {
				log.Error("the change stream stopped; reading again after a wait", "app", app.Name, "error", err.Error())
			})
		})
	}
	wg.Wait()
	return ctx.Err()
}
