package archive

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
)

// keepalives is a stream whose keepalives reached received.
type keepalives struct {
	confirmations
	received atomic.Uint64
}

func (k *keepalives) Received() pglogrepl.LSN { return pglogrepl.LSN(k.received.Load()) }

func startIdle(t *testing.T, cfg config.Archive) (*keepalives, func(*change.Transaction)) {
	t.Helper()
	tl := newFeed(t, 100)
	k := &keepalives{confirmations: make(confirmations, 10)}
	runArchiver(t, cfg, tl, k)
	return k, tl.Append
}

func noConfirm(t *testing.T, c confirmations, within time.Duration) {
	t.Helper()
	select {
	case lsn := <-c:
		t.Fatalf("confirmed %s", lsn)
	case <-time.After(within):
	}
}

func TestArchiverConfirmsTheReceivedPositionWhenIdle(t *testing.T) {
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1 << 30, SegmentSeconds: 1, RetentionHours: 24}
	k, _ := startIdle(t, cfg)
	k.received.Store(500)
	if lsn := waitConfirm(t, k.confirmations, 5*time.Second); lsn != 500 {
		t.Fatalf("confirmed %s, want the received 0/1F4", lsn)
	}
	noConfirm(t, k.confirmations, 1500*time.Millisecond)
}

func TestArchiverKeepsTheSlotAtAPendingSegment(t *testing.T) {
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1 << 30, SegmentSeconds: 900, RetentionHours: 24}
	k, appendTx := startIdle(t, cfg)
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	appendTx(tx(100, time.Now(), []change.Change{insert(cols, "1", "a")}))
	k.received.Store(500)
	appendTx(tx(200, time.Now(), []change.Change{insert(cols, "2", "b")}))
	noConfirm(t, k.confirmations, 300*time.Millisecond)
}
