package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/revoke"
)

func newFeed(t *testing.T, size int) *feed.Feed {
	t.Helper()
	app := config.App{Name: "shop", TailTransactions: size}
	f, err := feed.New(app, keys.Ring{bytes.Repeat([]byte{7}, keys.Size)}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// confirmations records each confirmed position.
type confirmations chan pglogrepl.LSN

func (c confirmations) Confirm(lsn pglogrepl.LSN) { c <- lsn }

func startArchiver(t *testing.T, cfg config.Archive) (*feed.Feed, confirmations, func() error) {
	t.Helper()
	cat := devCatalog(t)
	tl := newFeed(t, 100)
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

// The feed holds revocations only in memory, so the slot stays at the
// commit of one whose tokens may still be valid, for a restart to read again.
func TestArchiverHoldsTheSlotAtALiveRevocation(t *testing.T) {
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1, SegmentSeconds: 900, RetentionHours: 24}
	tl, confirmed, _ := startArchiver(t, cfg)
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	exp := time.Now().Add(time.Hour).Unix()
	revocation := change.Message{Prefix: revoke.Prefix, Content: []byte(fmt.Sprintf(`{"sid":"s9","exp":%d}`, exp))}
	tl.Append(tx(100, time.Now(), []change.Change{insert(cols, "1", "a")}, revocation))
	if lsn := waitConfirm(t, confirmed, 5*time.Second); lsn != 100 {
		t.Fatalf("confirmed %s, want the revocation's commit 0/64", lsn)
	}
	tl.Append(tx(200, time.Now(), []change.Change{insert(cols, "2", "b")}))
	if lsn := waitConfirm(t, confirmed, 5*time.Second); lsn != 100 {
		t.Fatalf("confirmed %s past a live revocation", lsn)
	}
}

func TestArchiverStartsAtTheSlotPosition(t *testing.T) {
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1, SegmentSeconds: 900, RetentionHours: 24}
	f, confirmed, _ := startArchiver(t, cfg)
	if _, err := f.Resume(cursor.Epoch{SystemID: 1, Timeline: 1}, 500, 500); err != nil {
		t.Fatal(err)
	}
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	f.Append(tx(600, time.Now(), []change.Change{insert(cols, "1", "a")}))
	if lsn := waitConfirm(t, confirmed, 5*time.Second); lsn != 601 {
		t.Fatalf("confirmed %s", lsn)
	}
}

// A slot created anew after Postgres invalidated the old one starts past WAL
// that is gone for good, so archiving goes on from it.
func TestArchiverGoesOnFromANewSlot(t *testing.T) {
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1, SegmentSeconds: 900, RetentionHours: 24}
	f, confirmed, _ := startArchiver(t, cfg)
	if _, err := f.Resume(cursor.Epoch{SystemID: 1, Timeline: 1}, 100, 100); err != nil {
		t.Fatal(err)
	}
	cols := []change.Column{col("id", oidInt4, true), col("body", oidText, false)}
	f.Append(tx(200, time.Now(), []change.Change{insert(cols, "1", "a")}))
	if lsn := waitConfirm(t, confirmed, 5*time.Second); lsn != 201 {
		t.Fatalf("confirmed %s", lsn)
	}
	f.Reset(500)
	f.Append(tx(600, time.Now(), []change.Change{insert(cols, "2", "b")}))
	if lsn := waitConfirm(t, confirmed, 5*time.Second); lsn != 601 {
		t.Fatalf("confirmed %s after the new slot, want 0/259", lsn)
	}
}

func TestArchiverFailsWhenTheTailDropsUnarchivedTransactions(t *testing.T) {
	cat := devCatalog(t)
	tl := newFeed(t, 1)
	tl.Append(tx(100, time.Now(), nil))
	tl.Append(tx(200, time.Now(), nil))
	cfg := config.Archive{Namespace: "shop", SegmentBytes: 1 << 30, SegmentSeconds: 900, RetentionHours: 24}
	err := New(cfg, cat, tl, make(confirmations, 1)).Run(context.Background())
	if err == nil {
		t.Fatal("archived past a gap")
	}
}
