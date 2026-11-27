package relay

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

// send writes one message to the relay.
type send func(ctx context.Context, msg any) error

// worker serves one app's registrations on one connection, in the order
// the relay sent its messages.
type worker struct {
	app  string
	feed *feed.Feed
	send send
	now  func() time.Time

	mu    sync.Mutex
	inbox []Message
	wake  chan struct{}

	// Only run touches these.
	regs map[string]*registration
	// waiting are the registrations to place once the feed is ready.
	waiting []*registration
}

func newWorker(app string, f *feed.Feed, s send, now func() time.Time) *worker {
	return &worker{app: app, feed: f, send: s, now: now, wake: make(chan struct{}, 1), regs: map[string]*registration{}}
}

// post queues a message for run.
func (w *worker) post(m Message) {
	w.mu.Lock()
	w.inbox = append(w.inbox, m)
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// run takes the posted messages and sends the registrations their changes
// until ctx ends or a send fails.
func (w *worker) run(ctx context.Context) error {
	for {
		// Taken before reading, so an append in between still wakes the wait.
		changed := w.feed.Changed()
		if err := w.step(ctx); err != nil {
			return err
		}
		var ready <-chan struct{}
		if len(w.waiting) > 0 {
			ready = w.feed.Ready()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-w.wake:
		case <-ready:
		}
	}
}

// step applies the posted messages, places the registrations it can and
// sends them what the feed holds for them.
func (w *worker) step(ctx context.Context) error {
	if err := w.receive(ctx); err != nil {
		return err
	}
	if err := w.place(ctx); err != nil {
		return err
	}
	return w.deliver(ctx)
}

// receive applies the posted messages in order.
func (w *worker) receive(ctx context.Context) error {
	w.mu.Lock()
	inbox := w.inbox
	w.inbox = nil
	w.mu.Unlock()
	for _, m := range inbox {
		if m.Type == UnregisterType {
			delete(w.regs, m.ID)
			w.waiting = slices.DeleteFunc(w.waiting, func(r *registration) bool { return r.id == m.ID })
			continue
		}
		if w.registered(m.ID) {
			if err := w.notice(ctx, RefusedType, m.ID, "the id is registered already"); err != nil {
				return err
			}
			continue
		}
		r, reason := newRegistration(m)
		if r == nil {
			if err := w.notice(ctx, RefusedType, m.ID, reason); err != nil {
				return err
			}
			continue
		}
		w.waiting = append(w.waiting, r)
	}
	return nil
}

func (w *worker) registered(id string) bool {
	_, ok := w.regs[id]
	return ok || slices.ContainsFunc(w.waiting, func(r *registration) bool { return r.id == id })
}

// place starts the waiting registrations, in the order they came, while
// the feed is ready.
func (w *worker) place(ctx context.Context) error {
	for len(w.waiting) > 0 {
		r := w.waiting[0]
		var err error
		switch r.place(w.feed, w.now()) {
		case notReady:
			return nil
		case placed:
			w.regs[r.id] = r
		case gap:
			err = w.notice(ctx, GapType, r.id, "")
		case revoked:
			err = w.notice(ctx, RevokedType, r.id, "")
		case invalidCursor:
			err = w.notice(ctx, RefusedType, r.id, "invalid cursor")
		}
		if err != nil {
			return err
		}
		w.waiting = w.waiting[1:]
	}
	return nil
}

func (w *worker) notice(ctx context.Context, typ, id, reason string) error {
	return w.send(ctx, Notice{Type: typ, App: w.app, ID: id, Reason: reason})
}
