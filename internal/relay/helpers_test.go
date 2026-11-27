package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/revoke"
	"github.com/PrototeamHQ/protobase-changes/internal/visibility"
)

var epoch = cursor.Epoch{SystemID: 1, Timeline: 1}

// newFeed is a feed of a tail of size whose stream started at LSN 100,
// after transaction 10 and before 11.
func newFeed(t *testing.T, size int) *feed.Feed {
	t.Helper()
	f, err := feed.New(config.App{Name: "shop", Issuer: "https://shop.example", TailTransactions: size},
		keys.Ring{bytes.Repeat([]byte{1}, keys.Size)}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	start, err := visibility.Parse("11:11:")
	if err != nil {
		t.Fatal(err)
	}
	f.Started(start)
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	return f
}

// insert appends transaction xid at lsn, of one insert into messages.
func insert(f *feed.Feed, xid uint32, lsn pglogrepl.LSN, more ...change.Change) {
	id := strconv.Itoa(int(xid))
	f.Append(&change.Transaction{Xid: xid, CommitLSN: lsn, EndLSN: lsn + 8, Changes: append([]change.Change{
		{Table: change.Table{Schema: "public", Name: "messages"}, Operation: change.Insert,
			Key:    change.Row{"id": {Kind: change.Text, Text: id}},
			NewRow: change.Row{"id": {Kind: change.Text, Text: id}, "body": {Kind: change.Text, Text: "b" + id}}},
	}, more...)})
}

// revocation is the app's insert into revoke.Table that revokes sid for an hour.
func revocation(sid string) change.Change {
	exp := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	return change.Change{Table: revoke.Table, Operation: change.Insert,
		NewRow: change.Row{"sid": {Kind: change.Text, Text: sid}, "exp": {Kind: change.Text, Text: exp}}}
}

func seal(f *feed.Feed, lsn pglogrepl.LSN) string {
	return f.Cursors.Seal(cursor.Position{Epoch: epoch, LSN: lsn})
}

// fakeRelay posts messages to a worker of app shop and keeps what it sends.
type fakeRelay struct {
	t    *testing.T
	w    *worker
	sent []any
}

func newRelay(t *testing.T, f *feed.Feed) *fakeRelay {
	r := &fakeRelay{t: t}
	r.w = newWorker("shop", f, func(_ context.Context, msg any) error {
		r.sent = append(r.sent, msg)
		return nil
	}, time.Now)
	return r
}

// post hands msg to the worker as the connection reads it.
func (r *fakeRelay) post(msg map[string]any) {
	r.t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		r.t.Fatal(err)
	}
	m, err := parse(data)
	if err != nil {
		r.t.Fatalf("%s: %v", data, err)
	}
	if err := r.w.post(context.Background(), m); err != nil {
		r.t.Fatal(err)
	}
}

// step runs the worker once and returns what it sent.
func (r *fakeRelay) step() []any {
	r.t.Helper()
	if err := r.w.step(context.Background()); err != nil {
		r.t.Fatal(err)
	}
	sent := r.sent
	r.sent = nil
	return sent
}

// summary lists what was sent: each event as its "registration:key" pairs,
// each notice as "type registration".
func summary(sent []any) []string {
	var out []string
	for _, msg := range sent {
		switch m := msg.(type) {
		case Notice:
			out = append(out, m.Type+" "+m.ID)
		case Event:
			for _, entry := range m.Data.Registrations {
				for _, c := range entry.Changes {
					out = append(out, entry.ID+":"+c.Key["id"].(string))
				}
			}
		}
	}
	return out
}

func register(id string, more map[string]any) map[string]any {
	m := map[string]any{"type": "register", "app": "shop", "id": id, "sid": "s9",
		"subs": []map[string]any{{"resource": "messages"}}}
	for k, v := range more {
		m[k] = v
	}
	return m
}
