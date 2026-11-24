package archive

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

// confirmations records each confirmed position.
type confirmations chan pglogrepl.LSN

func (c confirmations) Confirm(lsn pglogrepl.LSN) { c <- lsn }

func startArchiver(t *testing.T, cfg config.Archive) (*tail.Tail, confirmations, func() error) {
	t.Helper()
	cat := devCatalog(t)
	tl := tail.New(100)
	confirmed := make(confirmations, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(cfg, cat, tl, confirmed).Run(ctx) }()
	stop := sync.OnceValue(func() error {
		cancel()
		return <-done
	})
	t.Cleanup(func() { stop() })
	return tl, confirmed, stop
}

func waitConfirm(t *testing.T, c confirmations, within time.Duration) pglogrepl.LSN {
	t.Helper()
	select {
	case lsn := <-c:
		return lsn
	case <-time.After(within):
		t.Fatal("nothing confirmed")
		return 0
	}
}

func TestArchiverRollsOverAfterSegmentSeconds(t *testing.T) {
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1 << 30, SegmentSeconds: 1, RetentionHours: 24}
	tl, confirmed, stop := startArchiver(t, cfg)
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	tl.Append(tx(100, time.Now(), []change.Change{insert(cols, "1", "a")}))
	tl.Append(tx(200, time.Now(), []change.Change{insert(cols, "2", "b")}))

	select {
	case lsn := <-confirmed:
		t.Fatalf("confirmed %s before the segment rolled over", lsn)
	case <-time.After(300 * time.Millisecond):
	}
	if lsn := waitConfirm(t, confirmed, 5*time.Second); lsn != 201 {
		t.Fatalf("confirmed %s, want the end of the last transaction 0/C9", lsn)
	}
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestArchiverRollsOverAtSegmentBytes(t *testing.T) {
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1, SegmentSeconds: 900, RetentionHours: 24}
	tl, confirmed, _ := startArchiver(t, cfg)
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	tl.Append(tx(100, time.Now(), []change.Change{insert(cols, "1", "a")}))
	if lsn := waitConfirm(t, confirmed, 5*time.Second); lsn != 101 {
		t.Fatalf("confirmed %s", lsn)
	}
}

func TestArchiverFailsWhenTheTailDropsUnarchivedTransactions(t *testing.T) {
	cat := devCatalog(t)
	tl := tail.New(1)
	tl.Append(tx(100, time.Now(), nil))
	tl.Append(tx(200, time.Now(), nil))
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1 << 30, SegmentSeconds: 900, RetentionHours: 24}
	err := New(cfg, cat, tl, make(confirmations, 1)).Run(context.Background())
	if err == nil {
		t.Fatal("archived past a gap")
	}
}
