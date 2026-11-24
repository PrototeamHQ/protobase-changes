package destination

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

var (
	// ErrStale means a set issued before the one kept, or another set issued
	// at the same time: it never replaces the newer one.
	ErrStale = errors.New("a set issued later is kept")
	// ErrUnknown means the app defines no destination of that name.
	ErrUnknown = errors.New("unknown destination")
)

// Hub runs one app's destinations, a worker each. It is safe for concurrent use.
type Hub struct {
	app    config.App
	feed   *feed.Feed
	store  Store
	sender *Sender
	log    *slog.Logger
	// delays and now are fields so tests can shorten the waits.
	delays []time.Duration
	now    func() time.Time

	mu  sync.Mutex
	raw string
	set Set
	// states holds the states of destinations that do not run yet, until a
	// worker takes one or a set removes its destination.
	states map[string]held
	// removed names the destinations whose stored state is still to delete.
	removed map[string]bool
	ctx     context.Context
	errs    chan error
	running map[string]*running
}

// held is a destination's state between workers: unsaved when the worker
// that left it moved it past what the store holds.
type held struct {
	state   State
	unsaved bool
}

type running struct {
	w      *worker
	cancel context.CancelFunc
	done   chan struct{}
}

// NewHub loads the app's set and states from the store. A kept set that no
// key of the app opens any longer leaves the app without destinations until
// it puts a new one, and their states wait for it.
func NewHub(ctx context.Context, f *feed.Feed, store Store, sender *Sender, log *slog.Logger) (*Hub, error) {
	h := &Hub{
		app: f.App, feed: f, store: store, sender: sender, log: log.With("app", f.App.Name),
		delays: retryDelays, now: time.Now, running: map[string]*running{}, removed: map[string]bool{},
	}
	raw, err := store.Definitions(ctx, h.app.Name)
	if err != nil {
		return nil, fmt.Errorf("app %s: store: %w", h.app.Name, err)
	}
	if raw != "" {
		set, err := OpenSet(raw, f.Keys, h.app.Issuer)
		if err != nil {
			h.log.Error("the kept destinations no longer open under the app's keys; put them again")
		} else {
			h.raw, h.set = raw, set
		}
	}
	stored, err := store.States(ctx, h.app.Name)
	if err != nil {
		return nil, fmt.Errorf("app %s: store: %w", h.app.Name, err)
	}
	h.states = map[string]held{}
	for name, state := range stored {
		h.states[name] = held{state: state}
	}
	return h, nil
}

// Run delivers to every destination until ctx ends or the store fails. Run
// again after a failure goes on from where each destination got to.
func (h *Hub) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.mu.Lock()
	h.ctx, h.errs = ctx, make(chan error, 1)
	for _, d := range h.set.Destinations {
		h.startLocked(d, h.takeState(d.Name))
	}
	h.mu.Unlock()

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-h.errs:
		err = fmt.Errorf("app %s: %w", h.app.Name, err)
	}
	cancel()
	h.mu.Lock()
	for name, r := range h.running {
		<-r.done
		h.states[name] = r.w.held()
		delete(h.running, name)
	}
	h.ctx = nil
	h.mu.Unlock()
	return err
}

// Put replaces the app's destinations with the set raw defines. A
// destination of the same name keeps its cursor, one the set leaves out
// loses its state, and a new one starts at the newest change. Putting the
// kept set again finishes what an earlier put of it left undone.
func (h *Hub) Put(ctx context.Context, raw string) error {
	set, err := OpenSet(raw, h.feed.Keys, h.app.Issuer)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.raw != raw {
		if h.raw != "" && !set.IssuedAt.After(h.set.IssuedAt) {
			return ErrStale
		}
		if err := h.store.PutDefinitions(ctx, h.app.Name, raw); err != nil {
			return fmt.Errorf("store: %w", err)
		}
		h.raw = raw
		h.replaceLocked(set)
	}
	for name := range h.removed {
		if err := h.store.DeleteState(ctx, h.app.Name, name); err != nil {
			return fmt.Errorf("store: %w", err)
		}
		delete(h.removed, name)
	}
	return nil
}

// replaceLocked runs the set's destinations, from their kept state where
// they have one, and marks the state of those it leaves out for deletion.
// Before Run, it only marks them: Run starts the rest.
func (h *Hub) replaceLocked(set Set) {
	old := map[string]*Definition{}
	for _, d := range h.set.Destinations {
		old[d.Name] = d
	}
	h.set = set
	kept := map[string]bool{}
	for _, d := range set.Destinations {
		kept[d.Name] = true
		delete(h.removed, d.Name)
		if h.ctx == nil {
			continue
		}
		if _, ok := h.running[d.Name]; !ok {
			h.startLocked(d, h.takeState(d.Name))
			continue
		}
		if old[d.Name].digest != d.digest {
			h.startLocked(d, h.stopLocked(d.Name))
		}
	}
	for name := range h.running {
		if !kept[name] {
			h.stopLocked(name)
			h.removed[name] = true
		}
	}
	for name := range h.states {
		if !kept[name] {
			delete(h.states, name)
			h.removed[name] = true
		}
	}
}

// takeState hands a destination's kept state to its worker, or none.
func (h *Hub) takeState(name string) held {
	state := h.states[name]
	delete(h.states, name)
	return state
}

// Resume wakes a paused or gone destination, or tries a failed batch now.
func (h *Hub) Resume(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.running[name]
	if !ok {
		return ErrUnknown
	}
	select {
	case r.w.resume <- struct{}{}:
	default:
	}
	return nil
}

// startLocked runs a worker from state, or from the newest change when the
// destination has none yet.
func (h *Hub) startLocked(d *Definition, state held) {
	ctx, cancel := context.WithCancel(h.ctx)
	w := &worker{
		app: h.app.Name, source: h.app.Issuer, def: d, feed: h.feed, store: h.store, sender: h.sender,
		delays: h.delays, now: h.now, log: h.log, resume: make(chan struct{}, 1), state: state.state,
		dirty: state.unsaved,
	}
	r := &running{w: w, cancel: cancel, done: make(chan struct{})}
	h.running[d.Name] = r
	go func() {
		defer close(r.done)
		if err := w.run(ctx); err != nil && ctx.Err() == nil {
			select {
			case h.errs <- fmt.Errorf("destination %s: %w", d.Name, err):
			default:
			}
		}
	}()
}

// stopLocked stops a worker and returns where it got to.
func (h *Hub) stopLocked(name string) held {
	r := h.running[name]
	r.cancel()
	<-r.done
	delete(h.running, name)
	return r.w.held()
}
