package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/PrototeamHQ/protobase-changes/internal/archive"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/confirm"
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
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

// follow runs one reader per app into its feed, an archiver per app with an
// archive and a confirmer per app without one, until ctx ends. A reader or
// confirmer that fails runs again after a wait, on its own, so one app's
// database holds back no other app. An archiver that fails stops the others
// and returns its error: only a fresh start reads the changes it had not
// archived again from the slot.
func follow(ctx context.Context, cfg config.Config, fs map[string]*feed.Feed, hs map[string]*destination.Hub, log *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var readers []*reader.Reader
	var archivers, confirmers []job
	for _, app := range cfg.Apps {
		f := fs[app.Name]
		r := reader.New(app, f, log, nil)
		readers = append(readers, r)
		if app.Archive == nil {
			stored := confirm.Stored(confirm.NoDestinations)
			if h, ok := hs[app.Name]; ok {
				stored = h.Stored
			}
			confirmers = append(confirmers, job{app.Name, confirm.New(app.DatabaseURL, f, r, stored).Run})
			continue
		}
		cat, err := archive.Open(ctx, app.Name, app.Archive.Catalog)
		if err != nil {
			return fmt.Errorf("app %s: %w", app.Name, err)
		}
		archivers = append(archivers, job{"app " + app.Name + " archive", archive.New(*app.Archive, cat, f, r).Run})
	}

	var wg sync.WaitGroup
	for i, app := range cfg.Apps {
		r := readers[i]
		wg.Go(func() {
			supervise.Run(ctx, supervise.Default, r.Run, func(err error) {
				log.Error("the change stream stopped; reading again after a wait", "app", app.Name, "error", err.Error())
			})
		})
	}
	for _, j := range confirmers {
		wg.Go(func() {
			supervise.Run(ctx, supervise.Default, j.run, func(err error) {
				log.Error("confirming the slot stopped; confirming again after a wait", "app", j.name, "error", err.Error())
			})
		})
	}

	errs := make(chan error, len(archivers))
	for _, j := range archivers {
		wg.Go(func() {
			if err := j.run(ctx); err != nil && ctx.Err() == nil {
				errs <- fmt.Errorf("%s: %w", j.name, err)
				cancel()
			}
		})
	}
	wg.Wait()
	close(errs)
	if err, ok := <-errs; ok {
		return err
	}
	return ctx.Err()
}

// job is one long-running part of the service, such as an app's archiver.
type job struct {
	name string
	run  func(context.Context) error
}
