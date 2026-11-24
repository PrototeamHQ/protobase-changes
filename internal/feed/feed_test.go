package feed

import (
	"bytes"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/revoke"
)

var epoch = cursor.Epoch{SystemID: 1, Timeline: 1}

func newFeed(t *testing.T) *Feed {
	t.Helper()
	f, err := New(config.App{Name: "shop", TailTransactions: 10}, keys.Ring{bytes.Repeat([]byte{1}, keys.Size)},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func tx(lsn pglogrepl.LSN, messages ...change.Message) *change.Transaction {
	return &change.Transaction{CommitLSN: lsn, EndLSN: lsn + 8, Messages: messages}
}

func TestResumeStartsAfterWhatIsHeld(t *testing.T) {
	f := newFeed(t)
	if _, err := f.Head(); err != ErrNotReady {
		t.Fatalf("epoch before start: %v", err)
	}
	if start, err := f.Resume(epoch, 100, 100); err != nil || start != 100 {
		t.Fatalf("first resume %v %v", start, err)
	}
	if _, ok := f.Since(99); ok {
		t.Fatal("a position before the stream started is held")
	}
	f.Append(tx(120))
	if head, err := f.Head(); err != nil || head != (cursor.Position{Epoch: epoch, LSN: 128}) {
		t.Fatalf("head %+v %v, want after the held transaction", head, err)
	}
	if start, err := f.Resume(epoch, 100, 100); err != nil || start != 128 {
		t.Fatalf("second resume %v %v, want after the held transaction", start, err)
	}
	if _, err := f.Resume(cursor.Epoch{SystemID: 1, Timeline: 2}, 100, 100); err == nil {
		t.Fatal("resumed on another timeline")
	}
}

func TestSinceStartReadsFromWhereTheStreamFirstStarted(t *testing.T) {
	f := newFeed(t)
	if entries, ok := f.SinceStart(0); !ok || len(entries) != 0 {
		t.Fatalf("before the stream started: %v %v", entries, ok)
	}
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.SinceStart(0); !ok {
		t.Fatal("a position before the stream started looks dropped")
	}
	f.Append(tx(120))
	if _, err := f.Resume(epoch, 200, 200); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.SinceStart(0); ok {
		t.Fatal("a later start skipped over what the stream left out")
	}
}

func TestAppendSealsTheCursor(t *testing.T) {
	f := newFeed(t)
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	f.Append(tx(120))
	entries, ok := f.Since(100)
	if !ok || len(entries) != 1 {
		t.Fatalf("since 100: %v %v", entries, ok)
	}
	at, err := f.Cursors.Open(entries[0].Cursor)
	if err != nil || at != (cursor.Position{Epoch: epoch, LSN: 120}) {
		t.Fatalf("cursor opens to %+v, %v", at, err)
	}
	if entries[0].ID != f.Cursors.ID(at) {
		t.Fatal("the entry's id is not its position's")
	}
}

func TestAppendRecordsRevocations(t *testing.T) {
	f := newFeed(t)
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	exp := time.Now().Add(time.Hour).Unix()
	f.Append(tx(120,
		change.Message{Prefix: revoke.Prefix, Content: []byte(`{"sid":"s9","exp":` + strconv.FormatInt(exp, 10) + `}`)},
		change.Message{Prefix: revoke.Prefix, Content: []byte(`not json`)},
		change.Message{Prefix: "protobase_actor", Content: []byte(`{"sid":"s8","exp":` + strconv.FormatInt(exp, 10) + `}`)}))
	if !f.Revoked.Revoked("s9", time.Now()) || f.Revoked.Revoked("s8", time.Now()) {
		t.Fatal("revoked the wrong sessions")
	}
	if _, ok := f.Since(100); !ok {
		t.Fatal("a malformed revocation stopped the transaction")
	}
}

func TestNotReadyUntilTheWALAtConnectIsRead(t *testing.T) {
	f := newFeed(t)
	if _, err := f.Resume(epoch, 100, 200); err != nil {
		t.Fatal(err)
	}
	f.Append(tx(120))
	f.Decoded(150)
	if _, err := f.Head(); err != ErrNotReady {
		t.Fatalf("ready at 150 before the WAL at connect, 200, was read: %v", err)
	}
	f.Decoded(200)
	if _, err := f.Head(); err != nil {
		t.Fatalf("not ready once read up to 200: %v", err)
	}

	// Each new connection waits again for what its WAL held.
	if _, err := f.Resume(epoch, 100, 300); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Head(); err != ErrNotReady {
		t.Fatalf("ready right after reconnecting: %v", err)
	}
	f.Append(tx(300))
	if _, err := f.Head(); err != nil {
		t.Fatalf("not ready once a transaction ended past 300: %v", err)
	}
}

func TestReadyClosesOnceCaughtUp(t *testing.T) {
	f := newFeed(t)
	f.Resume(epoch, 100, 200)
	select {
	case <-f.Ready():
		t.Fatal("ready before reading up to where the WAL ended")
	default:
	}
	f.Decoded(200)
	f.Decoded(300)
	select {
	case <-f.Ready():
	default:
		t.Fatal("not ready after reading up to where the WAL ended")
	}
}

func TestReadyReopensWhenAReconnectFallsBehind(t *testing.T) {
	f := newFeed(t)
	f.Resume(epoch, 100, 100)
	<-f.Ready()
	f.Resume(epoch, 100, 300)
	select {
	case <-f.Ready():
		t.Fatal("ready while the new connection has not caught up")
	default:
	}
	if _, err := f.Head(); err != ErrNotReady {
		t.Fatalf("head %v, want not ready", err)
	}
	f.Decoded(300)
	<-f.Ready()
}

func TestResetOfTheFirstSlotDropsNothing(t *testing.T) {
	f := newFeed(t)
	f.Reset(100)
	if _, ok := f.SinceStart(0); !ok {
		t.Fatal("a slot created before the stream started looks dropped")
	}
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	f.Append(tx(120))
	f.Reset(500)
	if _, ok := f.SinceStart(128); ok {
		t.Fatal("a reset after the stream started skipped over what it dropped")
	}
}

func TestResetForgetsTheOldSlot(t *testing.T) {
	f := newFeed(t)
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	f.Append(tx(120))
	changed := f.Changed()

	f.Reset(500)
	<-changed
	if _, ok := f.Since(128); ok {
		t.Fatal("a position of the old slot still resumes")
	}
	if start, err := f.Resume(epoch, 500, 500); err != nil || start != 500 {
		t.Fatalf("resume after reset %v %v, want the new slot's start", start, err)
	}
	if head, err := f.Head(); err != nil || head.LSN != 500 {
		t.Fatalf("head %+v %v, want the new slot's start", head, err)
	}
	f.Append(tx(520))
	if entries, ok := f.Since(500); !ok || len(entries) != 1 {
		t.Fatalf("since 500: %v %v", entries, ok)
	}
}
