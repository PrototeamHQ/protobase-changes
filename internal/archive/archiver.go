package archive

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

// Confirmer lets Postgres drop the WAL up to a position, such as the reader.
type Confirmer interface {
	Confirm(lsn pglogrepl.LSN)
}

// Archiver follows an app's tail and commits it to the archive a segment at
// a time, confirming the slot only once a segment is committed.
type Archiver struct {
	cfg     config.Archive
	cat     catalog.Catalog
	tail    *tail.Tail
	confirm Confirmer
}

func New(cfg config.Archive, cat catalog.Catalog, t *tail.Tail, confirm Confirmer) *Archiver {
	return &Archiver{cfg: cfg, cat: cat, tail: t, confirm: confirm}
}

// Run archives until ctx ends or archiving fails. A segment not yet
// committed is dropped then: the slot still holds its WAL, so the next run
// reads it again. Run must keep up with the tail: when the tail drops a
// transaction it has not read, Run fails, and the next start reads the
// transaction again from the slot.
func (a *Archiver) Run(ctx context.Context) error {
	var seg segment
	var read pglogrepl.LSN
	nextMaintenance := time.Now()
	for {
		changed := a.tail.Changed()
		txs, ok := a.tail.Since(read)
		if !ok {
			return fmt.Errorf("archive: the tail dropped transactions after %s before they were archived", read)
		}
		for _, tx := range txs {
			seg.add(tx, time.Now())
			read = tx.CommitLSN
		}

		if len(seg.txs) > 0 && (seg.bytes >= a.cfg.SegmentBytes || !time.Now().Before(a.deadline(seg))) {
			if err := a.flush(ctx, seg); err != nil {
				return err
			}
			seg = segment{}
		}
		if !time.Now().Before(nextMaintenance) {
			retention := time.Duration(a.cfg.RetentionHours) * time.Hour
			if err := maintain(ctx, a.cat, a.cfg.Namespace, retention, time.Now()); err != nil {
				return fmt.Errorf("archive maintenance: %w", err)
			}
			nextMaintenance = time.Now().Add(maintenanceInterval)
		}

		wake := nextMaintenance
		if len(seg.txs) > 0 && a.deadline(seg).Before(wake) {
			wake = a.deadline(seg)
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
	return seg.started.Add(time.Duration(a.cfg.SegmentSeconds) * time.Second)
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
