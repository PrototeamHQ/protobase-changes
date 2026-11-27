package relay

import (
	"context"
	"slices"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

// inboxSize bounds the messages a worker holds before it applies them; the
// connection reads no further meanwhile.
const inboxSize = 1024

// send writes one message to the relay.
type send func(ctx context.Context, msg any) error

// worker serves one app's registrations on one connection, in the order
// the relay sent its messages.
type worker struct {
	app  string
	feed *feed.Feed
	send send
	now  func() time.Time

	inbox chan Message

	// Only run touches these.
	regs map[string]*registration
	// waiting are the registrations to place once the feed is ready.
	waiting []*registration
}

func newWorker(app string, f *feed.Feed, s send, now func() time.Time) *worker {
	return &worker{app: app, feed: f, send: s, now: now, inbox: make(chan Message, inboxSize), regs: map[string]*registration{}}
}

// post queues a message for run, in order, waiting while the inbox is full
// until ctx ends.
func (w *worker) post(ctx context.Context, m Message) error {
	select {
	case w.inbox <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
		case m := <-w.inbox:
			if err := w.apply(ctx, m); err != nil {
				return err
			}
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
	for {
		select {
		case m := <-w.inbox:
			if err := w.apply(ctx, m); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// apply takes one message: a register waits to be placed, or is refused.
func (w *worker) apply(ctx context.Context, m Message) error {
	if m.Type == UnregisterType {
		delete(w.regs, m.ID)
		w.waiting = slices.DeleteFunc(w.waiting, func(r *registration) bool { return r.id == m.ID })
		return nil
	}
	if w.registered(m.ID) {
		return w.notice(ctx, RefusedType, m.ID, "the id is registered already")
	}
	r, reason := newRegistration(m)
	if r == nil {
		return w.notice(ctx, RefusedType, m.ID, reason)
	}
	w.waiting = append(w.waiting, r)
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
