// Package supervise keeps one app's long-running work going, so a failure
// of one app holds back only that app.
package supervise

import (
	"context"
	"time"
)

// Backoff is how long Run waits before running again after a failure: First
// after the first failure, doubling with each next one up to Max.
type Backoff struct {
	First time.Duration
	Max   time.Duration
}

// Default waits from a second up to a minute.
var Default = Backoff{First: time.Second, Max: time.Minute}

// Run calls run until ctx ends, reporting each failure to failed and waiting
// before the next call. A run that lasted Max or longer was healthy, so the
// wait after it starts again from First.
func Run(ctx context.Context, b Backoff, run func(context.Context) error, failed func(error)) {
	wait := b.First
	for {
		started := time.Now()
		err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		failed(err)
		if time.Since(started) >= b.Max {
			wait = b.First
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		wait = min(wait*2, b.Max)
	}
}
