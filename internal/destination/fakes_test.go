package destination

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

var (
	epoch = cursor.Epoch{SystemID: 1, Timeline: 1}
	quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
)

// memStore keeps a store's rows in memory, for tests of what the hub writes;
// the SQL store has tests of its own.
type memStore struct {
	mu      sync.Mutex
	raw     map[string]string
	states  map[string]State
	letters []Letter
	writes  int
}

func newMemStore() *memStore { return &memStore{raw: map[string]string{}, states: map[string]State{}} }

func (m *memStore) Definitions(_ context.Context, app string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.raw[app], nil
}

func (m *memStore) PutDefinitions(_ context.Context, app, raw string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.raw[app] = raw
	return nil
}

func (m *memStore) States(_ context.Context, _ string) (map[string]State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]State{}
	for name, s := range m.states {
		out[name] = copyState(s)
	}
	return out, nil
}

func (m *memStore) PutState(_ context.Context, _ string, name string, s State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[name] = copyState(s)
	m.writes++
	return nil
}

func (m *memStore) DeleteState(_ context.Context, _ string, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, name)
	return nil
}

func (m *memStore) PutLetter(_ context.Context, _ string, _ string, l Letter) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.letters = append(m.letters, l)
	return nil
}

func (m *memStore) state(name string) (State, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.states[name]
	return copyState(s), ok
}

func copyState(s State) State {
	if s.Batch != nil {
		b := *s.Batch
		s.Batch = &b
	}
	return s
}

// received is one request a receiver got.
type received struct {
	id     string
	events []event.Event
	body   string
}

// receiver answers each request with the status answer gives, after
// checking its signature, and records the requests.
type receiver struct {
	t      *testing.T
	server *httptest.Server
	answer func(r received) (int, string)
	mu     sync.Mutex
	got    []received
}

func newReceiver(t *testing.T, answer func(r received) (int, string)) *receiver {
	rc := &receiver{t: t, answer: answer}
	verifier, err := standardwebhooks.NewWebhook(secret)
	if err != nil {
		t.Fatal(err)
	}
	rc.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := verifier.Verify(body, r.Header); err != nil {
			t.Errorf("signature: %v", err)
		}
		if ct := r.Header.Get("Content-Type"); ct != batchContentType {
			t.Errorf("content type %q", ct)
		}
		got := received{id: r.Header.Get("webhook-id"), body: string(body)}
		json.Unmarshal(body, &got.events)
		rc.mu.Lock()
		rc.got = append(rc.got, got)
		rc.mu.Unlock()
		status, answer := rc.answer(got)
		w.WriteHeader(status)
		io.WriteString(w, answer)
	}))
	t.Cleanup(rc.server.Close)
	return rc
}

func (rc *receiver) requests() []received {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]received(nil), rc.got...)
}

func ok(received) (int, string) { return http.StatusNoContent, "" }

// newFeed is a feed whose stream started and caught up at 100.
func newFeed(t *testing.T, tail int) *feed.Feed {
	t.Helper()
	f, err := feed.New(config.App{Name: "shop", Issuer: issuer, TailTransactions: tail}, ring, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	return f
}

// order commits an order with its status, as the WAL states it under
// REPLICA IDENTITY FULL.
func order(f *feed.Feed, lsn pglogrepl.LSN, id, from, to string) {
	row := func(status string) change.Row {
		return change.Row{"id": {Kind: change.Text, Text: id}, "status": {Kind: change.Text, Text: status}}
	}
	c := change.Change{Table: change.Table{Schema: "public", Name: "orders"}, Operation: change.Insert,
		Key: change.Row{"id": {Kind: change.Text, Text: id}}, NewRow: row(to)}
	if from != "" {
		c.Operation, c.OldRowKind, c.OldRow = change.Update, change.OldFull, row(from)
	}
	f.Append(&change.Transaction{CommitLSN: lsn, EndLSN: lsn + 8, CommitTime: now, Changes: []change.Change{c}})
}

// startHub runs a hub with the set until the test ends, retrying after 10ms.
func startHub(t *testing.T, f *feed.Feed, store Store, raw string, functions string) *Hub {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h, err := NewHub(ctx, f, store, &Sender{Client: http.DefaultClient, FunctionsURL: functions, Now: time.Now}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	h.delays = []time.Duration{10 * time.Millisecond}
	if raw != "" {
		if err := h.Put(ctx, raw); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != context.Canceled {
			t.Errorf("hub stopped: %v", err)
		}
	})
	return h
}

// eventually waits up to 5s for cond.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func positionAt(lsn pglogrepl.LSN) cursor.Position { return cursor.Position{Epoch: epoch, LSN: lsn} }
