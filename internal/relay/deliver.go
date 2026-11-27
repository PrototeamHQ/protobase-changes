package relay

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

// deliver sends each transaction after where the registrations got to as
// one event with an entry for each registration it concerns. A
// registration the feed no longer holds every change for gets gap, and one
// whose session was revoked gets revoked.
func (w *worker) deliver(ctx context.Context) error {
	var entries []*feed.Entry
	for {
		if len(w.regs) == 0 {
			return nil
		}
		var ok bool
		if entries, ok = w.feed.Since(w.lowest()); ok {
			break
		}
		if err := w.dropBelow(ctx, w.feed.Floor()); err != nil {
			return err
		}
	}
	// Checked after reading: a revocation is recorded before its
	// transaction is held, so nothing read here committed after it.
	if err := w.dropRevoked(ctx); err != nil {
		return err
	}
	ids := slices.Sorted(maps.Keys(w.regs))
	for _, entry := range entries {
		e, ok := w.event(entry, ids)
		if !ok {
			continue
		}
		if err := w.send(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// event makes the event of entry for the registrations ids that have not
// had it yet, moving them past it. ok is false when it concerns none.
func (w *worker) event(entry *feed.Entry, ids []string) (e Event, ok bool) {
	var items []Entry
	for _, id := range ids {
		r := w.regs[id]
		if entry.Commit() <= r.lsn {
			continue
		}
		r.lsn = entry.Commit()
		if r.seen != nil && r.seen.Saw(entry.Tx.Xid) {
			continue
		}
		if changes := event.Projected(entry.Tx, r.subs, r.fields); len(changes) > 0 {
			items = append(items, Entry{ID: id, Cursor: entry.Cursor, Changes: changes})
		}
	}
	if len(items) == 0 {
		return Event{}, false
	}
	return Event{
		SpecVersion: "1.0", ID: entry.ID, Source: w.feed.App.Issuer, Type: event.TransactionType,
		Time: entry.Tx.CommitTime.UTC().Format(time.RFC3339Nano), DataContentType: "application/json",
		App: w.app, Data: Data{Registrations: items},
	}, true
}

func (w *worker) lowest() (low pglogrepl.LSN) {
	first := true
	for _, r := range w.regs {
		if first || r.lsn < low {
			low, first = r.lsn, false
		}
	}
	return low
}

// dropBelow ends the registrations that got to before floor with gap.
func (w *worker) dropBelow(ctx context.Context, floor pglogrepl.LSN) error {
	for _, id := range slices.Sorted(maps.Keys(w.regs)) {
		if w.regs[id].lsn >= floor {
			continue
		}
		delete(w.regs, id)
		if err := w.notice(ctx, GapType, id, ""); err != nil {
			return err
		}
	}
	return nil
}

// dropRevoked ends the registrations whose session was revoked with revoked.
func (w *worker) dropRevoked(ctx context.Context) error {
	now := w.now()
	for _, id := range slices.Sorted(maps.Keys(w.regs)) {
		sid := w.regs[id].sid
		if sid == "" || !w.feed.Revoked.Revoked(sid, now) {
			continue
		}
		delete(w.regs, id)
		if err := w.notice(ctx, RevokedType, id, ""); err != nil {
			return err
		}
	}
	return nil
}
