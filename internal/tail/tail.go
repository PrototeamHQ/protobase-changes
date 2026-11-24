// Package tail keeps the latest committed transactions of one app in memory.
package tail

import (
	"sync"

	"github.com/jackc/pglogrepl"
)

// Item is what a tail keeps: anything that committed at a WAL position.
type Item interface {
	Commit() pglogrepl.LSN
}

// Tail is a ring of the last items, bounded by a fixed count, appended in
// commit order. It is safe for concurrent use.
type Tail[T Item] struct {
	mu   sync.Mutex
	ring []T
	next int
	full bool
	// floor is the position from which the tail holds every item.
	floor   pglogrepl.LSN
	changed chan struct{}
}

func New[T Item](size int) *Tail[T] {
	if size < 1 {
		panic("tail size must be at least 1")
	}
	return &Tail[T]{ring: make([]T, size), changed: make(chan struct{})}
}

// Append adds an item, dropping the oldest when full, and wakes every waiter
// on Changed.
func (t *Tail[T]) Append(item T) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.full {
		t.floor = max(t.floor, t.ring[t.next].Commit())
	}
	t.ring[t.next] = item
	t.next = (t.next + 1) % len(t.ring)
	if t.next == 0 {
		t.full = true
	}
	close(t.changed)
	t.changed = make(chan struct{})
}

// Raise moves the floor up to lsn: the tail holds nothing committed at or
// before it that was not appended, such as before the stream it is fed from starts.
func (t *Tail[T]) Raise(lsn pglogrepl.LSN) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.floor = max(t.floor, lsn)
}

// Reset drops every item and moves the floor up to lsn, waking every waiter
// on Changed, for a stream that starts again from a new place in the WAL.
func (t *Tail[T]) Reset(lsn pglogrepl.LSN) {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.ring)
	t.next, t.full = 0, false
	t.floor = max(t.floor, lsn)
	close(t.changed)
	t.changed = make(chan struct{})
}

// Wake wakes every waiter on Changed without an item, such as when the
// position the tail is read up to moves.
func (t *Tail[T]) Wake() {
	t.mu.Lock()
	defer t.mu.Unlock()
	close(t.changed)
	t.changed = make(chan struct{})
}

// Floor returns the position from which the tail holds every item: Since
// refuses only positions below it.
func (t *Tail[T]) Floor() pglogrepl.LSN {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.floor
}

// Changed returns a channel that closes on the next Append or Wake.
func (t *Tail[T]) Changed() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.changed
}

// Since returns the kept items that committed after lsn, oldest first.
// ok is false when lsn is below the floor, so the tail may lack some of them.
func (t *Tail[T]) Since(lsn pglogrepl.LSN) (items []T, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if lsn < t.floor {
		return nil, false
	}
	kept := t.ordered()
	for i, item := range kept {
		if item.Commit() > lsn {
			return kept[i:], true
		}
	}
	return nil, true
}

// Newest returns the last appended item; ok is false when there is none.
func (t *Tail[T]) Newest() (item T, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.full && t.next == 0 {
		return item, false
	}
	return t.ring[(t.next-1+len(t.ring))%len(t.ring)], true
}

func (t *Tail[T]) ordered() []T {
	if !t.full {
		return append([]T(nil), t.ring[:t.next]...)
	}
	return append(append([]T(nil), t.ring[t.next:]...), t.ring[:t.next]...)
}
