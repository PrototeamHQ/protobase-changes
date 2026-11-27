package feed

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/visibility"
)

func snapshot(t *testing.T, text string) visibility.Snapshot {
	t.Helper()
	s, err := visibility.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// startedFeed is a feed of a tail of size, whose stream started at LSN 100
// after transaction 10 and before 11.
func startedFeed(t *testing.T, size int) *Feed {
	t.Helper()
	f, err := New(config.App{Name: "shop", TailTransactions: size}, keys.Ring{bytes.Repeat([]byte{1}, keys.Size)},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	f.Started(snapshot(t, "11:11:"))
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	return f
}

func xact(xid uint32, lsn pglogrepl.LSN) *change.Transaction {
	return &change.Transaction{Xid: xid, CommitLSN: lsn, EndLSN: lsn + 8}
}

func TestSnapshotStartNeedsTheStartSeen(t *testing.T) {
	f := startedFeed(t, 10)
	f.Append(xact(11, 110))
	if lsn, ok, err := f.SnapshotStart(snapshot(t, "11:13:")); err != nil || !ok || lsn != 100 {
		t.Fatalf("after the start: %v %v %v, want the floor", lsn, ok, err)
	}
	// A snapshot from before the stream started may have missed what
	// committed before it, which the feed never held.
	if _, ok, _ := f.SnapshotStart(snapshot(t, "10:12:10")); ok {
		t.Fatal("a snapshot from before the start passed")
	}
}

func TestSnapshotStartNeedsTheDroppedSeen(t *testing.T) {
	f := startedFeed(t, 2)
	for i, xid := range []uint32{11, 13, 12} {
		f.Append(xact(xid, pglogrepl.LSN(110+10*i)))
	}
	// 11 is dropped: a snapshot that saw it starts at its commit, where the
	// feed holds every later transaction.
	if lsn, ok, _ := f.SnapshotStart(snapshot(t, "12:14:12")); !ok || lsn != 110 {
		t.Fatalf("got %v %v, want the floor after the dropped transaction", lsn, ok)
	}
	for _, text := range []string{"11:14:11", "10:11:"} {
		if _, ok, _ := f.SnapshotStart(snapshot(t, text)); ok {
			t.Errorf("%s missed a dropped transaction and passed", text)
		}
	}
}

func TestSnapshotStartAfterAResetNeedsTheNewStart(t *testing.T) {
	f := startedFeed(t, 10)
	f.Reset(200)
	if _, _, err := f.SnapshotStart(snapshot(t, "11:11:")); !errors.Is(err, ErrNotReady) {
		t.Fatalf("got %v, want ErrNotReady until the stream resumes", err)
	}
	f.Started(snapshot(t, "20:20:"))
	if _, err := f.Resume(epoch, 200, 200); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := f.SnapshotStart(snapshot(t, "11:11:")); ok {
		t.Fatal("a snapshot from before the new slot passed")
	}
	if lsn, ok, _ := f.SnapshotStart(snapshot(t, "20:21:")); !ok || lsn != 200 {
		t.Fatalf("got %v %v, want the new slot's start", lsn, ok)
	}
}

func TestSnapshotStartWithoutAStartSnapshotFails(t *testing.T) {
	f := newFeed(t)
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := f.SnapshotStart(snapshot(t, "11:11:")); ok {
		t.Fatal("passed without knowing what the start saw")
	}
}

func TestSnapshotStartNeedsTheForgottenSeen(t *testing.T) {
	f := startedFeed(t, 1)
	// The tail and the ring of dropped xids hold one each: 11 is forgotten
	// once 12 is dropped.
	for i, xid := range []uint32{11, 12, 13} {
		f.Append(xact(xid, pglogrepl.LSN(110+10*i)))
	}
	// This one saw 12 and the newest dropped, but listed 11 in progress.
	if _, ok, _ := f.SnapshotStart(snapshot(t, "11:14:11")); ok {
		t.Fatal("a snapshot that may have missed a forgotten transaction passed")
	}
	if _, ok, _ := f.SnapshotStart(snapshot(t, "13:14:13")); !ok {
		t.Fatal("a snapshot that saw every dropped transaction failed")
	}
}
