package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/revoke"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

var epoch = cursor.Epoch{SystemID: 1, Timeline: 1}

func newFeed(t *testing.T, size int) *feed.Feed {
	t.Helper()
	f, err := feed.New(config.App{Name: "shop", Issuer: "https://shop.example", TailTransactions: size},
		keys.Ring{bytes.Repeat([]byte{1}, keys.Size)}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	return f
}

// on appends a transaction of one insert into table at lsn.
func on(f *feed.Feed, lsn pglogrepl.LSN, table string, messages ...change.Message) {
	f.Append(&change.Transaction{CommitLSN: lsn, EndLSN: lsn + 8, Messages: messages, Changes: []change.Change{
		{Table: change.Table{Schema: "public", Name: table}, Operation: change.Insert,
			Key: change.Row{"id": {Kind: change.Text, Text: strconv.Itoa(int(lsn))}}},
	}})
}

func tok() token.Token {
	return token.Token{Session: "s9", Expiry: time.Now().Add(time.Hour), Payload: token.Thin, Resources: map[string]bool{"messages": true}}
}

func next(t *testing.T, s *Stream, limit int) []event.Event {
	t.Helper()
	events, err := s.Next(context.Background(), 20*time.Millisecond, limit)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func types(events []event.Event) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.Type)
	}
	return out
}

func TestReplaysThenStreamsLive(t *testing.T) {
	f := newFeed(t, 10)
	on(f, 110, "messages")
	on(f, 120, "messages")
	start := f.Cursors.Seal(cursor.Position{Epoch: epoch, LSN: 110})
	s, err := Open(f, tok(), start, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	got := next(t, s, 100)
	if len(got) != 1 || got[0].Type != event.TransactionType {
		t.Fatalf("replay %v", types(got))
	}
	replayed := got[0].Cursor

	done := make(chan []event.Event)
	go func() {
		events, _ := s.Next(context.Background(), time.Minute, 100)
		done <- events
	}()
	on(f, 130, "messages")
	if live := <-done; len(live) != 1 || live[0].Data.Changes[0].Key["id"] != "130" {
		t.Fatalf("live %+v", live)
	}

	// The cursor of an event resumes right after it.
	again, err := Open(f, tok(), replayed, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if got := next(t, again, 100); len(got) != 1 || got[0].Data.Changes[0].Key["id"] != "130" {
		t.Fatalf("resumed %+v", got)
	}
}

func TestStartsAtTheTokenCursorOrLive(t *testing.T) {
	f := newFeed(t, 10)
	on(f, 110, "messages")
	on(f, 120, "messages")
	snapshot := tok()
	at := pglogrepl.LSN(115)
	snapshot.Cursor = &at
	s, err := Open(f, snapshot, "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if got := next(t, s, 100); len(got) != 1 || got[0].Data.Changes[0].Key["id"] != "120" {
		t.Fatalf("from the token's cursor %+v", got)
	}

	live, err := Open(f, tok(), "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if got := next(t, live, 100); len(got) != 1 || got[0].Type != event.PositionType {
		t.Fatalf("live start replayed %v", types(got))
	}
}

func TestPositionPassesUnreadableTransactions(t *testing.T) {
	f := newFeed(t, 3)
	s, err := Open(f, tok(), "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []pglogrepl.LSN{110, 120, 130} {
		on(f, lsn, "secrets")
	}
	got := next(t, s, 100)
	if len(got) != 1 || got[0].Type != event.PositionType {
		t.Fatalf("got %v, want only a position", types(got))
	}
	at, err := f.Cursors.Open(got[0].Cursor)
	if err != nil || at.LSN != 130 {
		t.Fatalf("position at %+v %v, want past the unreadable transactions", at, err)
	}
	// Resuming there works once the tail dropped what came before it.
	on(f, 140, "secrets")
	if _, err := Open(f, tok(), got[0].Cursor, time.Now); err != nil {
		t.Fatal(err)
	}
}

func TestLimitStopsWhereTheCursorResumes(t *testing.T) {
	f := newFeed(t, 10)
	s, err := Open(f, tok(), "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []pglogrepl.LSN{110, 120, 130} {
		on(f, lsn, "messages")
	}
	if got := next(t, s, 2); len(got) != 2 {
		t.Fatalf("got %d events, want the limit of 2", len(got))
	}
	if got := next(t, s, 2); len(got) != 1 || got[0].Data.Changes[0].Key["id"] != "130" {
		t.Fatalf("after the limit %+v", got)
	}
}

func TestRefusesWhatItCannotResume(t *testing.T) {
	f := newFeed(t, 2)
	for _, lsn := range []pglogrepl.LSN{110, 120, 130} {
		on(f, lsn, "messages")
	}
	cases := map[string]struct {
		sealed string
		want   error
	}{
		"dropped":       {f.Cursors.Seal(cursor.Position{Epoch: epoch, LSN: 100}), ErrGone},
		"other epoch":   {f.Cursors.Seal(cursor.Position{Epoch: cursor.Epoch{SystemID: 1, Timeline: 2}, LSN: 120}), ErrGone},
		"not a cursor":  {"abc", cursor.ErrInvalid},
		"other app key": {otherCursor(t), cursor.ErrInvalid},
	}
	for name, c := range cases {
		if _, err := Open(f, tok(), c.sealed, time.Now); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
	// The oldest held commit is the floor, and a cursor there still resumes.
	if _, err := Open(f, tok(), f.Cursors.Seal(cursor.Position{Epoch: epoch, LSN: 110}), time.Now); err != nil {
		t.Fatalf("at the floor: %v", err)
	}
}

func otherCursor(t *testing.T) string {
	codec, err := cursor.New(keys.Ring{bytes.Repeat([]byte{2}, keys.Size)})
	if err != nil {
		t.Fatal(err)
	}
	return codec.Seal(cursor.Position{Epoch: epoch, LSN: 120})
}

func TestFallingBehindEndsTheStream(t *testing.T) {
	f := newFeed(t, 2)
	s, err := Open(f, tok(), "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []pglogrepl.LSN{110, 120, 130} {
		on(f, lsn, "messages")
	}
	if _, err := s.Next(context.Background(), time.Minute, 100); !errors.Is(err, ErrGone) {
		t.Fatalf("got %v, want ErrGone", err)
	}
}

func TestRevocationStopsBeforeItsTransaction(t *testing.T) {
	f := newFeed(t, 10)
	s, err := Open(f, tok(), "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	on(f, 110, "messages")
	if got := next(t, s, 100); len(got) != 1 {
		t.Fatalf("before the revocation %v", types(got))
	}
	exp := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	on(f, 120, "messages", change.Message{Prefix: revoke.Prefix, Content: []byte(`{"sid":"s9","exp":` + exp + `}`)})
	if _, err := s.Next(context.Background(), time.Minute, 100); !errors.Is(err, ErrRevoked) {
		t.Fatalf("got %v, want ErrRevoked", err)
	}
	if _, err := Open(f, tok(), "", time.Now); !errors.Is(err, ErrRevoked) {
		t.Fatalf("opened a revoked session: %v", err)
	}
}

func TestExpiryEndsTheStream(t *testing.T) {
	f := newFeed(t, 10)
	short := tok()
	short.Expiry = time.Now().Add(30 * time.Millisecond)
	s, err := Open(f, short, "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next(context.Background(), time.Minute, 100); !errors.Is(err, ErrExpired) {
		t.Fatalf("got %v, want ErrExpired", err)
	}
	if _, err := Open(f, short, "", time.Now); !errors.Is(err, ErrExpired) {
		t.Fatalf("opened with an expired token: %v", err)
	}
}

func TestNotStarted(t *testing.T) {
	f, err := feed.New(config.App{Name: "shop", TailTransactions: 1}, keys.Ring{bytes.Repeat([]byte{1}, keys.Size)},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(f, tok(), "", time.Now); !errors.Is(err, feed.ErrNotReady) {
		t.Fatalf("got %v", err)
	}
}
