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
	// states holds the stored states until Run hands them to workers.
	states  map[string]State
	ctx     context.Context
	errs    chan error
	running map[string]*running
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
		delays: retryDelays, now: time.Now, running: map[string]*running{},
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
	h.states, err = store.States(ctx, h.app.Name)
	if err != nil {
		return nil, fmt.Errorf("app %s: store: %w", h.app.Name, err)
	}
	return h, nil
}

// Run delivers to every destination until ctx ends or the store fails.
func (h *Hub) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.mu.Lock()
	h.ctx, h.errs = ctx, make(chan error, 1)
	for _, d := range h.set.Destinations {
		h.startLocked(d, h.states[d.Name])
	}
	h.states = nil
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
	for _, r := range h.running {
		<-r.done
	}
	h.mu.Unlock()
	return err
}

// Put replaces the app's destinations with the set raw defines. A
// destination of the same name keeps its cursor, one the set leaves out
// loses its state, and a new one starts at the newest change.
func (h *Hub) Put(ctx context.Context, raw string) error {
	set, err := OpenSet(raw, h.feed.Keys, h.app.Issuer)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.raw == raw {
		return nil
	}
	if h.raw != "" && !set.IssuedAt.After(h.set.IssuedAt) {
		return ErrStale
	}
	if err := h.store.PutDefinitions(ctx, h.app.Name, raw); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	old := map[string]*Definition{}
	for _, d := range h.set.Destinations {
		old[d.Name] = d
	}
	h.raw, h.set = raw, set
	if h.ctx == nil {
		return h.dropStored(ctx)
	}

	kept := map[string]bool{}
	for _, d := range set.Destinations {
		kept[d.Name] = true
		if _, ok := h.running[d.Name]; !ok {
			h.startLocked(d, State{})
			continue
		}
		if old[d.Name].digest != d.digest {
			h.startLocked(d, h.stopLocked(d.Name))
		}
	}
	for name := range h.running {
		if kept[name] {
			continue
		}
		h.stopLocked(name)
		if err := h.store.DeleteState(ctx, h.app.Name, name); err != nil {
			return fmt.Errorf("store: %w", err)
		}
	}
	return nil
}

// dropStored drops the stored states of destinations a set put before Run
// leaves out; Run starts the rest.
func (h *Hub) dropStored(ctx context.Context) error {
	kept := map[string]bool{}
	for _, d := range h.set.Destinations {
		kept[d.Name] = true
	}
	for name := range h.states {
		if kept[name] {
			continue
		}
		delete(h.states, name)
		if err := h.store.DeleteState(ctx, h.app.Name, name); err != nil {
			return fmt.Errorf("store: %w", err)
		}
	}
	return nil
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
func (h *Hub) startLocked(d *Definition, state State) {
	ctx, cancel := context.WithCancel(h.ctx)
	w := &worker{
		app: h.app.Name, source: h.app.Issuer, def: d, feed: h.feed, store: h.store, sender: h.sender,
		delays: h.delays, now: h.now, log: h.log, resume: make(chan struct{}, 1), state: state,
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
func (h *Hub) stopLocked(name string) State {
	r := h.running[name]
	r.cancel()
	<-r.done
	delete(h.running, name)
	return r.w.state
}
