package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
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
	// unsaved counts the transactions the cursor passed in memory since it
	// was last saved.
	unsaved int
	// dirty is whether state differs from what the store holds, such as a
	// cursor moved in memory or a save that failed.
	dirty bool
	// stored is the cursor the store holds while the destination delivers,
	// zero while unknown, or noHold while it is paused, gone or retrying a
	// batch. Others read it at any time.
	stored atomic.Uint64
}

// noHold marks a destination that holds back no confirm of the slot: one
// that stopped at a batch holds it no more than one that fell behind the tail.
const noHold = math.MaxUint64

// markStored records what the store now holds as state.
func (w *worker) markStored() { w.stored.Store(holdOf(w.state)) }

// holdOf is where a destination the store holds at s holds back the confirm.
func holdOf(s State) uint64 {
	if s.Status != Active || s.Batch != nil {
		return noHold
	}
	return uint64(s.Position.LSN)
}

// held returns where the worker got to, for the next worker of its
// destination. Call it only after run returned.
func (w *worker) held() held { return held{state: w.state, unsaved: w.dirty} }

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
	head, err := w.head(ctx)
	if err != nil {
		return err
	}
	if w.state.Status == "" {
		w.state = State{Position: head, Status: Active}
		return w.save(ctx)
	}
	if w.state.Status != Gone && w.state.Position.Epoch != head.Epoch {
		return w.lose(ctx)
	}
	// An earlier worker's state the store lacks is saved first, so the store
	// shows a gone destination and its cursor stays within the feed.
	if w.dirty {
		return w.save(ctx)
	}
	return nil
}

// head returns the feed's newest position, waiting while a connection of its
// stream has not yet caught up.
func (w *worker) head(ctx context.Context) (cursor.Position, error) {
	for {
		// Taken before Head, so readiness coming in between still wakes the wait.
		ready := w.feed.Ready()
		head, err := w.feed.Head()
		if !errors.Is(err, feed.ErrNotReady) {
			return head, err
		}
		select {
		case <-ctx.Done():
			return cursor.Position{}, ctx.Err()
		case <-ready:
		}
	}
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
		head, err := w.head(ctx)
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
// cursor moves past transactions without any in memory, and is saved only
// once it passed half of what the feed holds, so a quiet destination writes
// seldom and a restart still finds its cursor in the feed. A destination
// done with every transaction saves the head the feed advanced to, so the
// slot can be confirmed past its cursor. The store's own write is one
// transaction, so it never makes another.
func (w *worker) deliverNext(ctx context.Context) error {
	for {
		// Taken before reading, so an append in between still wakes the wait.
		changed := w.feed.Changed()
		idle := w.feed.Idle()
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
			w.dirty = true
			w.unsaved += g.seen
			if w.unsaved >= max(2, w.feed.App.TailTransactions/2) {
				if err := w.save(ctx); err != nil {
					return err
				}
			}
			continue
		}
		if idle > w.state.Position.LSN {
			w.state.Position.LSN = idle
			if err := w.save(ctx); err != nil {
				return err
			}
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
	w.dirty = true
	if err := w.store.PutState(ctx, w.app, w.def.Name, w.state); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	w.unsaved, w.dirty = 0, false
	w.markStored()
	return nil
}
