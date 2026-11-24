package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

// follow runs one reader per app until ctx ends or the first reader fails,
// which stops the others.
func follow(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, len(cfg.Apps))
	for _, app := range cfg.Apps {
		r := reader.New(app, tail.New(app.TailTransactions), log, nil)
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
