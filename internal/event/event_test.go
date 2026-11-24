package event

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

func entry() *feed.Entry {
	messages := change.Table{Schema: "public", Name: "messages"}
	audit := change.Table{Schema: "audit", Name: "log"}
	text := func(s string) change.Value { return change.Value{Kind: change.Text, Text: s} }
	return &feed.Entry{Cursor: "c1", ID: "i1", Tx: &change.Transaction{
		CommitTime: time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC),
		Changes: []change.Change{
			{Table: messages, Operation: change.Update, Key: change.Row{"id": text("1")},
				NewRow: change.Row{"id": text("1"), "body": {Kind: change.Unchanged}, "note": {Kind: change.Null}}},
			{Table: audit, Operation: change.Insert, Key: change.Row{"id": text("7")}, NewRow: change.Row{"id": text("7")}},
			{Table: messages, Operation: change.Delete, Key: change.Row{"id": text("2")},
				OldRowKind: change.OldKeyOnly, OldRow: change.Row{"id": text("2")}},
		},
		Messages: []change.Message{
			{Prefix: ActorPrefix, Content: []byte(`{"sub":"u42"}`), Position: 0},
			{Prefix: ActorPrefix, Content: []byte(`not json`), Position: 2},
		},
	}}
}

func encode(t *testing.T, e Event) string {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestThinCarriesResourceOperationAndKey(t *testing.T) {
	e, ok := Transaction("https://shop.example", entry(), token.Token{Payload: token.Thin, Resources: map[string]bool{"messages": true}})
	if !ok {
		t.Fatal("no event")
	}
	want := `{"specversion":"1.0","id":"i1","source":"https://shop.example","type":"protobase.changes.transaction",` +
		`"time":"2026-11-24T10:00:00Z","datacontenttype":"application/json","cursor":"c1","data":{"changes":[` +
		`{"resource":"messages","operation":"update","key":{"id":"1"}},` +
		`{"resource":"messages","operation":"delete","key":{"id":"2"}}]}}`
	if got := encode(t, e); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestRawCarriesWhatTheWALStates(t *testing.T) {
	e, ok := Transaction("s", entry(), token.Token{Payload: token.Raw, Resources: map[string]bool{"messages": true, "audit.log": true}})
	if !ok {
		t.Fatal("no event")
	}
	want := `{"changes":[` +
		`{"resource":"messages","operation":"update","key":{"id":"1"},"new":{"id":"1","note":null},"actor":{"sub":"u42"}},` +
		`{"resource":"audit.log","operation":"insert","key":{"id":"7"},"new":{"id":"7"},"actor":{"sub":"u42"}},` +
		`{"resource":"messages","operation":"delete","key":{"id":"2"},"old":{"id":"2"}}]}`
	data, err := json.Marshal(e.Data)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("got  %s\nwant %s", data, want)
	}
}

func TestNoEventWithoutReadableChanges(t *testing.T) {
	if _, ok := Transaction("s", entry(), token.Token{Payload: token.Raw, Resources: map[string]bool{"log": true}}); ok {
		t.Fatal("made an event of a transaction the token may not read")
	}
}

func TestPositionCarriesOnlyTheCursor(t *testing.T) {
	a := Position("s", "c9", time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
	b := Position("s", "c9", time.Now())
	if a.ID == b.ID || a.Data != nil || a.Cursor != "c9" || a.Type != PositionType {
		t.Fatalf("position events %+v %+v", a, b)
	}
}

func TestTablesWithADotAreNeverRead(t *testing.T) {
	text := change.Value{Kind: change.Text, Text: "1"}
	e := &feed.Entry{Tx: &change.Transaction{Changes: []change.Change{
		{Table: change.Table{Schema: "public", Name: "audit.log"}, Operation: change.Insert, Key: change.Row{"id": text}},
		{Table: change.Table{Schema: "a.b", Name: "c"}, Operation: change.Insert, Key: change.Row{"id": text}},
		{Table: change.Table{Schema: "a", Name: "b.c"}, Operation: change.Insert, Key: change.Row{"id": text}},
	}}}
	tok := token.Token{Payload: token.Raw, Resources: map[string]bool{"audit.log": true, "a.b.c": true}}
	if got, ok := Transaction("s", e, tok); ok {
		t.Fatalf("read a table whose resource names another: %+v", got.Data)
	}
}
