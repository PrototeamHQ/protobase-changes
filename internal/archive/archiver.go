package archive

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

// Confirmer lets Postgres drop the WAL up to a position, such as the reader.
type Confirmer interface {
	Confirm(lsn pglogrepl.LSN)
	// Received is a position before which every committed transaction
	// reached the feed, so it can be confirmed once all of them are archived.
	Received() pglogrepl.LSN
}

// Archiver follows an app's feed and commits it to the archive a segment at
// a time, confirming the slot only once a segment is committed.
type Archiver struct {
	cfg     config.Archive
	cat     catalog.Catalog
	feed    *feed.Feed
	confirm Confirmer
}

func New(cfg config.Archive, cat catalog.Catalog, f *feed.Feed, confirm Confirmer) *Archiver {
	return &Archiver{cfg: cfg, cat: cat, feed: f, confirm: confirm}
}

// Run archives until ctx ends or archiving fails. A segment not yet
// committed is dropped then: the slot still holds its WAL, so the next run
// reads it again. Run must keep up with the tail: when the tail drops a
// transaction it has not read, Run fails, and the next start reads the
// transaction again from the slot.
func (a *Archiver) Run(ctx context.Context) error {
	var seg segment
	var read, idleConfirmed pglogrepl.LSN
	nextMaintenance := time.Now()
	for {
		changed := a.feed.Changed()
		// Taken before reading the feed, which then holds every transaction
		// committed before it.
		received := a.confirm.Received()
		// Everything before the slot's position the stream started from is
		// archived already, as only the archive confirms the slot.
		entries, ok := a.feed.SinceStart(read)
		if !ok {
			return fmt.Errorf("archive: the tail dropped transactions after %s before they were archived", read)
		}
		idle := len(entries) == 0 && len(seg.txs) == 0
		for _, e := range entries {
			seg.add(e.Tx, time.Now())
			read = e.Tx.CommitLSN
		}

		if len(seg.txs) > 0 && (seg.bytes >= a.cfg.SegmentBytes || !time.Now().Before(a.deadline(seg))) {
			if err := a.flush(ctx, seg); err != nil {
				return err
			}
			seg = segment{}
		}
		// Everything committed before received is archived, so an app
		// without changes does not hold the WAL other databases write.
		if idle && received > idleConfirmed {
			a.confirm.Confirm(received)
			idleConfirmed = received
			read = max(read, received)
		}
		if !time.Now().Before(nextMaintenance) {
			retention := time.Duration(a.cfg.RetentionHours) * time.Hour
			if err := maintain(ctx, a.cat, a.cfg.Namespace, retention, time.Now()); err != nil {
				return fmt.Errorf("archive maintenance: %w", err)
			}
			nextMaintenance = time.Now().Add(maintenanceInterval)
		}

		// An empty segment wakes as often as a full one rolls over, to
		// confirm how far the stream came.
		next := time.Now().Add(a.segmentDuration())
		if len(seg.txs) > 0 {
			next = a.deadline(seg)
		}
		wake := nextMaintenance
		if next.Before(wake) {
			wake = next
		}
		timer := time.NewTimer(time.Until(wake))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// deadline is when seg rolls over at the latest.
func (a *Archiver) deadline(seg segment) time.Time {
	return seg.started.Add(a.segmentDuration())
}

func (a *Archiver) segmentDuration() time.Duration {
	return time.Duration(a.cfg.SegmentSeconds) * time.Second
}

// flush commits a segment table by table, then confirms the slot up to its end.
func (a *Archiver) flush(ctx context.Context, seg segment) error {
	byTable, err := seg.tables()
	if err != nil {
		return fmt.Errorf("archive: %w", err)
	}
	for t, events := range byTable {
		if err := commitTable(ctx, a.cat, identifier(a.cfg.Namespace, t), events); err != nil {
			return fmt.Errorf("archive %s.%s: %w", t.Schema, t.Name, err)
		}
	}
	a.confirm.Confirm(seg.end())
	return nil
}
