package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PrototeamHQ/protobase-changes/internal/archive"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

// job is one long-running part of the service, such as an app's reader.
type job struct {
	name string
	run  func(context.Context) error
}

// follow runs a reader per app, and an archiver per app with an archive,
// until ctx ends or the first job fails, which stops the others.
func follow(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var jobs []job
	for _, app := range cfg.Apps {
		t := tail.New(app.TailTransactions)
		r := reader.New(app, t, log, nil)
		jobs = append(jobs, job{"app " + app.Name, r.Run})
		if app.Archive == nil {
			continue
		}
		cat, err := archive.Open(ctx, app.Name, app.Archive.Catalog)
		if err != nil {
			return fmt.Errorf("app %s: %w", app.Name, err)
		}
		jobs = append(jobs, job{"app " + app.Name + " archive", archive.New(*app.Archive, cat, t, r).Run})
	}

	errs := make(chan error, len(jobs))
	for _, j := range jobs {
		go func() {
			if err := j.run(ctx); err != nil {
				errs <- fmt.Errorf("%s: %w", j.name, err)
				return
			}
			errs <- nil
		}()
	}

	var first error
	for range jobs {
		if err := <-errs; err != nil && first == nil {
			first = err
			cancel()
		}
	}
	return first
}
