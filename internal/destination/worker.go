package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

// worker delivers one destination's batches in commit order from its own
// cursor, so a failing receiver holds back only itself and skips nothing.
type worker struct {
	app    string
	source string
	def    *Definition
	feed   *feed.Feed
	store  Store
	sender *Sender
	delays []time.Duration
	now    func() time.Time
	log    *slog.Logger
	// resume wakes a paused, gone or waiting destination; it holds one signal.
	resume chan struct{}
	// state is owned by run; others read it only after run returned. A
	// destination without a status yet starts at the newest change.
	state State
}

// run delivers until ctx ends or the store fails.
func (w *worker) run(ctx context.Context) error {
	if err := w.start(ctx); err != nil {
		return err
	}
	for {
		var err error
		switch {
		case w.state.Status != Active:
			err = w.waitResume(ctx)
		case w.state.Batch != nil:
			err = w.retry(ctx)
		default:
			err = w.deliverNext(ctx)
		}
		if err != nil {
			return err
		}
	}
}

// start waits for the feed to catch up, then places a new destination at
// the newest change. A cursor of another epoch means nothing in this one.
func (w *worker) start(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.feed.Ready():
	}
	head, err := w.feed.Head()
	if err != nil {
		return err
	}
	if w.state.Status == "" {
		w.state = State{Position: head, Status: Active}
		return w.save(ctx)
	}
	if w.state.Status == Active && w.state.Position.Epoch != head.Epoch {
		return w.lose(ctx)
	}
	return nil
}

// waitResume holds a paused or gone destination until it is resumed: a
// paused one tries its batch again with every try back, a gone one starts
// again from the newest change.
func (w *worker) waitResume(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.resume:
	}
	if w.state.Status == Gone {
		head, err := w.feed.Head()
		if err != nil {
			return err
		}
		w.state = State{Position: head, Status: Active}
		return w.save(ctx)
	}
	w.state.Status = Active
	if w.state.Batch != nil {
		w.state.Batch.Tried = 0
		w.state.Batch.Next = time.Time{}
	}
	return w.save(ctx)
}

// deliverNext waits for the destination's next events and tries them. The
// cursor moves past transactions without any in memory only, so a quiet
// destination writes nothing.
func (w *worker) deliverNext(ctx context.Context) error {
	for {
		// Taken before reading, so an append in between still wakes the wait.
		changed := w.feed.Changed()
		entries, ok := w.feed.Since(w.state.Position.LSN)
		if !ok {
			return w.lose(ctx)
		}
		g := gather(w.source, w.def, entries, 0)
		if len(g.events) > 0 {
			return w.try(ctx, g)
		}
		if g.end != 0 {
			w.state.Position.LSN = g.end
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-w.resume:
		}
	}
}

// retry waits until the failed batch is due, or a resume, then tries the
// same transactions again.
func (w *worker) retry(ctx context.Context) error {
	if wait := w.state.Batch.Next.Sub(w.now()); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		case <-w.resume:
		}
	}
	for {
		changed := w.feed.Changed()
		entries, ok := w.feed.Since(w.state.Position.LSN)
		if !ok {
			return w.lose(ctx)
		}
		g := gather(w.source, w.def, entries, w.state.Batch.End)
		if g.end == w.state.Batch.End {
			return w.try(ctx, g)
		}
		// The feed has not yet read up to the batch's end again since a restart.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// try sends one batch. It moves the cursor past it once delivered, and
// otherwise keeps it for a retry, or applies the destination's at_limit.
func (w *worker) try(ctx context.Context, g gathered) error {
	if len(g.events) == 0 {
		// A changed definition left the retried transactions nothing to send.
		return w.done(ctx, g)
	}
	start := w.state.Position
	id := batchID(w.def, start.Epoch, start.LSN, g.end)
	body, err := json.Marshal(g.events)
	if err != nil {
		return err
	}
	err = w.sender.send(ctx, w.def, id, body)
	var f *failure
	if err == nil {
		return w.done(ctx, g)
	}
	if !errors.As(err, &f) {
		return err
	}

	tried := 1
	if w.state.Batch != nil {
		tried = w.state.Batch.Tried + 1
	}
	w.state.Batch = &Batch{End: g.end, ID: id, Tried: tried, Error: f.reason}
	if tried < w.def.Tries {
		w.state.Batch.Next = w.now().Add(retryDelay(w.delays, tried, f.retryAfter))
		return w.save(ctx)
	}
	if w.def.AtLimit == DeadLetter {
		letter := Letter{BatchID: id, Events: body, Error: f.reason, Tried: tried, At: w.now()}
		if err := w.store.PutLetter(ctx, w.app, w.def.Name, letter); err != nil {
			return fmt.Errorf("store: %w", err)
		}
		w.log.Warn("dead-lettered a batch at the retry limit", "destination", w.def.Name, "batch", id)
		return w.done(ctx, g)
	}
	w.state.Status = Paused
	w.log.Warn("paused a destination at the retry limit", "destination", w.def.Name, "batch", id)
	return w.save(ctx)
}

// done moves the cursor past a batch.
func (w *worker) done(ctx context.Context, g gathered) error {
	w.state.Position.LSN = g.end
	w.state.Batch = nil
	return w.save(ctx)
}

// lose marks a destination whose cursor the feed can no longer resume from.
func (w *worker) lose(ctx context.Context) error {
	w.state.Status = Gone
	w.state.Batch = nil
	w.log.Warn("a destination fell behind what the feed holds", "destination", w.def.Name)
	return w.save(ctx)
}

func (w *worker) save(ctx context.Context) error {
	if err := w.store.PutState(ctx, w.app, w.def.Name, w.state); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	return nil
}
