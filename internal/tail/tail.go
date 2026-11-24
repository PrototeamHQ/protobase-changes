// Package tail keeps the latest committed transactions of one app in memory.
package tail

import (
	"sync"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// Tail is a ring of the last transactions, bounded by a fixed count.
// It is safe for concurrent use.
type Tail struct {
	mu      sync.Mutex
	ring    []*change.Transaction
	next    int
	full    bool
	dropped pglogrepl.LSN
	changed chan struct{}
}

func New(size int) *Tail {
	if size < 1 {
		panic("tail size must be at least 1")
	}
	return &Tail{ring: make([]*change.Transaction, size), changed: make(chan struct{})}
}

// Append adds a committed transaction, dropping the oldest when full, and
// wakes every waiter on Changed.
func (t *Tail) Append(tx *change.Transaction) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.full {
		t.dropped = t.ring[t.next].CommitLSN
	}
	t.ring[t.next] = tx
	t.next = (t.next + 1) % len(t.ring)
	if t.next == 0 {
		t.full = true
	}
	close(t.changed)
	t.changed = make(chan struct{})
}

// Changed returns a channel that closes on the next Append.
func (t *Tail) Changed() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.changed
}

// Since returns the kept transactions that committed after lsn, oldest first.
// ok is false when the tail no longer holds every transaction after lsn.
func (t *Tail) Since(lsn pglogrepl.LSN) (txs []*change.Transaction, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if lsn < t.dropped {
		return nil, false
	}
	kept := t.ordered()
	for i, tx := range kept {
		if tx.CommitLSN > lsn {
			return kept[i:], true
		}
	}
	return nil, true
}

// Newest returns the last appended transaction, or nil when there is none.
func (t *Tail) Newest() *change.Transaction {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ring[(t.next-1+len(t.ring))%len(t.ring)]
}

func (t *Tail) ordered() []*change.Transaction {
	if !t.full {
		return append([]*change.Transaction(nil), t.ring[:t.next]...)
	}
	return append(append([]*change.Transaction(nil), t.ring[t.next:]...), t.ring[:t.next]...)
}
