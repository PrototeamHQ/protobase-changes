package supervise

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunsAgainAfterEachFailureUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls, failures int
	run := func(context.Context) error {
		calls++
		if calls == 3 {
			cancel()
			return ctx.Err()
		}
		return errors.New("boom")
	}
	Run(ctx, Backoff{First: time.Millisecond, Max: 4 * time.Millisecond}, run, func(error) { failures++ })
	if calls != 3 || failures != 2 {
		t.Fatalf("%d calls and %d failures, want 3 and 2", calls, failures)
	}
}

func TestWaitDoublesUpToMax(t *testing.T) {
	// A fake clock, so the waits measure exactly.
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var starts []time.Time
		run := func(context.Context) error {
			starts = append(starts, time.Now())
			if len(starts) == 5 {
				cancel()
			}
			return errors.New("boom")
		}
		Run(ctx, Backoff{First: 10 * time.Millisecond, Max: 40 * time.Millisecond}, run, func(error) {})
		want := []time.Duration{10, 20, 40, 40}
		for i, w := range want {
			if gap := starts[i+1].Sub(starts[i]); gap < w*time.Millisecond {
				t.Fatalf("wait %d took %v, want at least %vms", i, gap, w)
			}
		}
		if gap := starts[4].Sub(starts[3]); gap >= 80*time.Millisecond {
			t.Fatalf("the last wait took %v, want it held at Max", gap)
		}
	})
}

func TestHealthyRunStartsTheWaitAgain(t *testing.T) {
	// A fake clock, so the waits measure exactly.
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var starts []time.Time
		run := func(context.Context) error {
			starts = append(starts, time.Now())
			switch len(starts) {
			case 5:
				// Healthy: lasts Max. Without it, the next wait would be 160ms.
				time.Sleep(400 * time.Millisecond)
			case 6:
				cancel()
			}
			return errors.New("boom")
		}
		Run(ctx, Backoff{First: 10 * time.Millisecond, Max: 400 * time.Millisecond}, run, func(error) {})
		if gap := starts[5].Sub(starts[4]) - 400*time.Millisecond; gap >= 100*time.Millisecond {
			t.Fatalf("waited %v after a healthy run, want about First", gap)
		}
	})
}
