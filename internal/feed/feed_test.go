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
	if _, err := f.Epoch(); err != ErrNotStarted {
		t.Fatalf("epoch before start: %v", err)
	}
	if start, err := f.Resume(epoch, 100); err != nil || start != 100 {
		t.Fatalf("first resume %v %v", start, err)
	}
	if _, ok := f.Since(99); ok {
		t.Fatal("a position before the stream started is held")
	}
	f.Append(tx(120))
	if head, err := f.Head(); err != nil || head != (cursor.Position{Epoch: epoch, LSN: 128}) {
		t.Fatalf("head %+v %v, want after the held transaction", head, err)
	}
	if start, err := f.Resume(epoch, 100); err != nil || start != 128 {
		t.Fatalf("second resume %v %v, want after the held transaction", start, err)
	}
	if _, err := f.Resume(cursor.Epoch{SystemID: 1, Timeline: 2}, 100); err == nil {
		t.Fatal("resumed on another timeline")
	}
}

func TestAppendSealsTheCursor(t *testing.T) {
	f := newFeed(t)
	if _, err := f.Resume(epoch, 100); err != nil {
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
	if _, err := f.Resume(epoch, 100); err != nil {
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
