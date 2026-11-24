// Package feed holds one app's recent committed transactions, each with the
// cursor consumers resume after it, and the sessions the app revoked.
package feed

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/revoke"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

// Entry is one committed transaction with its cursor and event id, made once
// when it enters the feed and shared by every consumer.
type Entry struct {
	Tx     *change.Transaction
	Cursor string
	ID     string
}

func (e *Entry) Commit() pglogrepl.LSN { return e.Tx.CommitLSN }

// ErrNotReady means the feed's stream has not started, or has not yet read
// all the WAL there was when it connected, so revocations in it may be missing.
var ErrNotReady = errors.New("the change stream has not caught up")

// Feed is one app's change stream as consumers read it. It is safe for concurrent use.
type Feed struct {
	App     config.App
	Keys    keys.Ring
	Cursors *cursor.Codec
	Revoked *revoke.Set
	tail    *tail.Tail[*Entry]
	log     *slog.Logger

	mu      sync.Mutex
	started bool
	// ready is whether the stream read past caughtUp, where the WAL ended when
	// it connected: only then are the revocations in it all recorded.
	ready    bool
	caughtUp pglogrepl.LSN
	epoch    cursor.Epoch
	// end is where the stream reached: the end of the newest transaction, or
	// where the stream started when none came yet.
	end pglogrepl.LSN
}

func New(app config.App, ring keys.Ring, log *slog.Logger) (*Feed, error) {
	codec, err := cursor.New(ring)
	if err != nil {
		return nil, fmt.Errorf("app %s: %w", app.Name, err)
	}
	return &Feed{
		App: app, Keys: ring, Cursors: codec, Revoked: revoke.New(),
		tail: tail.New[*Entry](app.TailTransactions), log: log.With("app", app.Name),
	}, nil
}

// Resume returns where the stream of epoch picks up when it starts from the
// slot's position from: after what the feed holds. A stream of another epoch
// than an earlier one fails, since the positions held would mean nothing in it.
// New consumers wait until the stream has read up to caughtUp, where the WAL
// ended when it connected.
func (f *Feed) Resume(epoch cursor.Epoch, from, caughtUp pglogrepl.LSN) (pglogrepl.LSN, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started && epoch != f.epoch {
		return 0, fmt.Errorf("the database's system id or timeline changed from %+v to %+v", f.epoch, epoch)
	}
	if !f.started || from > f.end {
		f.tail.Raise(from)
		f.end = from
	}
	f.started = true
	f.epoch = epoch
	f.caughtUp = caughtUp
	f.ready = f.end >= caughtUp
	return f.end, nil
}

// Decoded marks that the stream sent every transaction committed before lsn.
func (f *Feed) Decoded(lsn pglogrepl.LSN) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ready = f.ready || (f.started && lsn >= f.caughtUp)
}

// Append takes in a committed transaction, first recording the sessions it revokes.
func (f *Feed) Append(tx *change.Transaction) {
	now := time.Now()
	for _, m := range tx.Messages {
		if m.Prefix != revoke.Prefix {
			continue
		}
		if err := f.Revoked.Add(m.Content, now); err != nil {
			f.log.Warn("ignored a malformed revocation", "error", err.Error(), "lsn", tx.CommitLSN.String())
		}
	}

	f.mu.Lock()
	at := cursor.Position{Epoch: f.epoch, LSN: tx.CommitLSN}
	f.end = tx.EndLSN
	f.ready = f.ready || f.end >= f.caughtUp
	f.mu.Unlock()
	f.tail.Append(&Entry{Tx: tx, Cursor: f.Cursors.Seal(at), ID: f.Cursors.ID(at)})
}

// Head returns where the stream reached, for a consumer that starts live:
// after every transaction the feed took in so far.
func (f *Feed) Head() (cursor.Position, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ready {
		return cursor.Position{}, ErrNotReady
	}
	return cursor.Position{Epoch: f.epoch, LSN: f.end}, nil
}

// Since returns the transactions committed after lsn, oldest first. ok is
// false when the feed no longer holds all of them.
func (f *Feed) Since(lsn pglogrepl.LSN) ([]*Entry, bool) { return f.tail.Since(lsn) }

// Changed returns a channel that closes when the next transaction comes in.
func (f *Feed) Changed() <-chan struct{} { return f.tail.Changed() }
