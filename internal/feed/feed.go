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
	"github.com/PrototeamHQ/protobase-changes/internal/visibility"
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
	App  config.App
	Keys keys.Ring
	// RelayKeys authenticate the relay's connection for the app; none when
	// no relay serves it.
	RelayKeys keys.Ring
	Cursors   *cursor.Codec
	Revoked   *revoke.Set
	tail      *tail.Tail[*Entry]
	log       *slog.Logger

	mu      sync.Mutex
	started bool
	// ready is whether the stream read past caughtUp, where the WAL ended when
	// it connected: only then are the revocations in it all recorded.
	ready bool
	// readyCh is closed while the feed is ready, and replaced by an open one
	// when a reconnect makes it fall behind again.
	readyCh  chan struct{}
	caughtUp pglogrepl.LSN
	epoch    cursor.Epoch
	// start is the slot's position the stream first started from, or the
	// start of the slot created anew since.
	start pglogrepl.LSN
	// end is where the stream reached: the end of the newest transaction, or
	// where the stream started when none came yet.
	end pglogrepl.LSN
	// idle is the latest position Advance moved the head to.
	idle pglogrepl.LSN
	// horizon tells the snapshots that saw everything before the floor.
	horizon horizon
	// startSnapshot is the snapshot the stream last reported through Started, and
	// restarted whether a Reset dropped the tail since the last Resume.
	startSnapshot *visibility.Snapshot
	restarted     bool
}

func New(app config.App, ring keys.Ring, log *slog.Logger) (*Feed, error) {
	codec, err := cursor.New(ring)
	if err != nil {
		return nil, fmt.Errorf("app %s: %w", app.Name, err)
	}
	return &Feed{
		App: app, Keys: ring, Cursors: codec, Revoked: revoke.New(),
		tail: tail.New[*Entry](app.TailTransactions), log: log.With("app", app.Name),
		readyCh: make(chan struct{}), horizon: newHorizon(app.TailTransactions),
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
	if !f.started {
		f.start = from
	}
	if !f.started || from > f.end || f.restarted {
		// The tail holds nothing from before from, all of which the snapshot
		// reported through Started saw.
		f.horizon.reset(f.startSnapshot)
		f.restarted = false
	}
	if !f.started || from > f.end {
		f.tail.Raise(from)
		f.end = from
	}
	f.started = true
	f.epoch = epoch
	f.caughtUp = caughtUp
	wasReady := f.ready
	f.ready = f.end >= caughtUp
	if wasReady && !f.ready {
		f.readyCh = make(chan struct{})
	}
	f.signalReady()
	return f.end, nil
}

// Reset starts the feed again at from, the start of a slot created anew
// after the old one was invalidated or dropped: the old slot's transactions
// after what the feed holds are lost, so every position before from is gone
// and its consumers, live ones included, resync from a snapshot. The new
// slot starts past every position the old one gave, so its start alone tells
// old cursors from new ones, and the cursor format needs no slot generation.
// Once the slot is confirmed, Postgres no longer shows where it started: an
// archive that serves positions below the floor must keep each slot's start
// and never serve across one. A reader of everything the stream brings, such
// as the archive, goes on from the new slot's start: the WAL between is gone
// for good, and a fresh start would begin at the same place.
//
// The feed is not ready until the stream resumes from the new slot, after the
// revocations the old slot's lost WAL held were loaded through Revoke.
func (f *Feed) Reset(from pglogrepl.LSN) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.start = from
	f.tail.Reset(from)
	f.end = from
	f.restarted = true
	f.notReady()
}

// Lost marks that Postgres invalidated the slot: the revocations in the WAL
// it lost are unknown until they are loaded through Revoke, so the feed is
// not ready, and its consumers get ErrNotReady, until a stream resumes.
func (f *Feed) Lost() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notReady()
}

// notReady makes the feed not ready. Called with mu held.
func (f *Feed) notReady() {
	if f.ready {
		f.ready = false
		f.readyCh = make(chan struct{})
	}
}

// Revoke records revocations read from revoke.Table rather than the stream,
// such as those committed before the slot's start.
func (f *Feed) Revoke(rs []revoke.Revocation) {
	f.Revoked.AddAll(rs, time.Now())
}

// Started records a snapshot taken after the stream's start, which saw
// every transaction committed before it, before the stream resumes.
func (f *Feed) Started(s visibility.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startSnapshot = &s
}

// Decoded marks that the stream sent every transaction committed before lsn.
func (f *Feed) Decoded(lsn pglogrepl.LSN) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ready = f.ready || (f.started && lsn >= f.caughtUp)
	f.signalReady()
}

// Append takes in a committed transaction, first recording the sessions it revokes.
func (f *Feed) Append(tx *change.Transaction) {
	now := time.Now()
	for _, c := range tx.Changes {
		r, ok, err := revoke.FromChange(c)
		if err != nil {
			f.log.Warn("ignored a malformed revocation", "error", err.Error(), "lsn", tx.CommitLSN.String())
		}
		if ok {
			f.Revoked.Add(r.Sid, r.Until, now)
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	at := cursor.Position{Epoch: f.epoch, LSN: tx.CommitLSN}
	f.end = tx.EndLSN
	f.ready = f.ready || f.end >= f.caughtUp
	f.signalReady()
	// Under mu, so SnapshotStart never sees the floor of a drop the horizon
	// does not know yet.
	if dropped, ok := f.tail.Append(&Entry{Tx: tx, Cursor: f.Cursors.Seal(at), ID: f.Cursors.ID(at)}); ok {
		f.horizon.drop(dropped.Tx.Xid)
	}
}

// signalReady closes readyCh once the feed is ready. Called with mu held.
func (f *Feed) signalReady() {
	if !f.ready {
		return
	}
	select {
	case <-f.readyCh:
	default:
		close(f.readyCh)
	}
}

// Ready returns a channel that is closed while the stream has read all the
// WAL there was when it last connected. A Head that fails with ErrNotReady
// after taking it waits on it, then tries again.
func (f *Feed) Ready() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readyCh
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

// SnapshotStart returns where a consumer that read the database at snapshot
// s starts: at the floor, taking every transaction from there on that s did
// not see. ok is false when s may not have seen a transaction the feed no
// longer holds.
func (f *Feed) SnapshotStart(s visibility.Snapshot) (lsn pglogrepl.LSN, ok bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ready {
		return 0, false, ErrNotReady
	}
	if !f.horizon.sawAll(s) {
		return 0, false, nil
	}
	return f.tail.Floor(), true, nil
}

// Since returns the transactions committed after lsn, oldest first. ok is
// false when the feed no longer holds all of them.
func (f *Feed) Since(lsn pglogrepl.LSN) ([]*Entry, bool) { return f.tail.Since(lsn) }

// SinceStart returns the transactions committed after lsn, or after the
// slot's position the stream first started from, or the new slot's start
// after a Reset, when that is later, such as
// for a reader of everything the stream brings. It reads that start under the
// same lock Resume sets it and raises the tail under, so a stream starting
// meanwhile cannot make the transactions look dropped. ok is false when the
// feed no longer holds all of them.
func (f *Feed) SinceStart(lsn pglogrepl.LSN) ([]*Entry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tail.Since(max(lsn, f.start))
}

// Advance moves the head up to lsn, a position before which every committed
// transaction is in the feed, such as one the server's keepalives report
// while no transaction is open, and wakes the waiters on Changed. Nothing
// committed between the newest transaction and lsn, so a consumer done with
// the newest can go on from lsn, such as a destination storing it as its
// cursor before the slot is confirmed past what it stored.
func (f *Feed) Advance(lsn pglogrepl.LSN) {
	f.mu.Lock()
	f.idle = max(f.idle, lsn)
	f.end = max(f.end, lsn)
	f.mu.Unlock()
	f.tail.Wake()
}

// Idle returns the latest position Advance moved the head to. Taken before
// Since, it is a cursor past every transaction Since returned.
func (f *Feed) Idle() pglogrepl.LSN {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.idle
}

// Floor returns the position from which the feed holds every transaction:
// a cursor below it can no longer resume.
func (f *Feed) Floor() pglogrepl.LSN { return f.tail.Floor() }

// Changed returns a channel that closes when the next transaction comes in,
// or the head advances.
func (f *Feed) Changed() <-chan struct{} { return f.tail.Changed() }
