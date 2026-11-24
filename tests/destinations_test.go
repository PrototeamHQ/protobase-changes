//go:build integration

package tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/serve"
	"github.com/PrototeamHQ/protobase-changes/internal/store"
	"github.com/PrototeamHQ/protobase-changes/internal/store/storetest"
)

var webhookSecret = "whsec_" + base64.StdEncoding.EncodeToString([]byte("the destination test's secret"))

// postgresStore opens a store in the test database on a fresh changes
// schema, which it drops afterwards.
func postgresStore(t *testing.T, ctx context.Context, db *pgx.Conn, url string) *store.SQL {
	t.Helper()
	exec(t, ctx, db, "DROP SCHEMA IF EXISTS changes CASCADE")
	s, err := store.Open(config.Store{Kind: config.Postgres, URL: url})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		exec(t, context.Background(), db, "DROP SCHEMA IF EXISTS changes CASCADE")
	})
	return s
}

func TestPostgresStore(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := postgresStore(t, ctx, connect(t, ctx, url), url)
	if err := s.Check(ctx); !errors.Is(err, store.ErrSchema) {
		t.Fatalf("check before migrate: %v", err)
	}
	for range 2 {
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Check(ctx); err != nil {
		t.Fatal(err)
	}
	storetest.RoundTrip(t, s)
}

// hookReceiver records the events of every signed batch it gets.
type hookReceiver struct {
	mu      sync.Mutex
	batches [][]event.Event
}

func (h *hookReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	verifier, _ := standardwebhooks.NewWebhook(webhookSecret)
	if verifier.Verify(body, r.Header) != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var events []event.Event
	json.Unmarshal(body, &events)
	h.mu.Lock()
	h.batches = append(h.batches, events)
	h.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// types lists the type and row id of every event received, in order. The
// test table has REPLICA IDENTITY FULL, whose changes carry no key.
func (h *hookReceiver) types() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, batch := range h.batches {
		for _, e := range batch {
			out = append(out, fmt.Sprintf("%s:%v", e.Type, e.Data.Changes[0].NewRow["id"]))
		}
	}
	return out
}

// TestDeliversFromTheDatabase follows a real database into two destinations
// whose cursors live in the same database: one gets every transaction, the
// other only a named event that compares the old and new row. The store's
// own writes reach the stream too, but raise no further write.
func TestDeliversFromTheDatabase(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)
	exec(t, ctx, db, "ALTER TABLE "+name+" REPLICA IDENTITY FULL")
	s := postgresStore(t, ctx, db, url)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	app := config.App{Name: "shop", DatabaseURL: url, Slot: name, Publication: name, TailTransactions: 100, Issuer: issuer}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := newFeed(t, app)
	stop := start(t, ctx, reader.New(app, f, log, nil))
	defer stop()
	hub := destination.NewHub(f, s, &destination.Sender{Client: http.DefaultClient, Now: time.Now}, log)
	hubCtx, stopHub := context.WithCancel(ctx)
	hubDone := make(chan error, 1)
	go func() { hubDone <- hub.Run(hubCtx) }()
	select {
	case <-hub.Ready():
	case err := <-hubDone:
		stopHub()
		t.Fatalf("the hub stopped before it loaded: %v", err)
	case <-ctx.Done():
		stopHub()
		<-hubDone
		t.Fatal("timed out waiting for the hub to load")
	}
	defer func() {
		stopHub()
		if err := <-hubDone; !errors.Is(err, context.Canceled) {
			t.Errorf("hub stopped: %v", err)
		}
	}()
	srv := httptest.NewServer(serve.New(map[string]*feed.Feed{"shop": f}, map[string]*destination.Hub{"shop": hub}).Handler())
	defer srv.Close()

	every, renames := &hookReceiver{}, &hookReceiver{}
	everySrv, renamesSrv := httptest.NewServer(every), httptest.NewServer(renames)
	defer everySrv.Close()
	defer renamesSrv.Close()
	set := seal(t, map[string]any{"iss": issuer, "iat": time.Now().Unix(), "destinations": []map[string]any{
		{"name": "every", "url": everySrv.URL, "secret": webhookSecret, "payload": "raw",
			"subs": []map[string]any{{"resource": name}}, "retry": map[string]any{"at_limit": "pause"}},
		{"name": "renames", "url": renamesSrv.URL, "secret": webhookSecret, "payload": "raw",
			"events": []map[string]any{{"type": name + ".renamed", "resource": name, "operations": []string{"update"},
				"old": map[string]any{"eq": []any{"body", "one"}}, "new": map[string]any{"neq": []any{"body", "one"}}}},
			"retry": map[string]any{"at_limit": "dead_letter"}},
	}})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL+serve.DestinationsPath("shop"), strings.NewReader(set))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("put destinations: %v %v", resp, err)
	}
	resp.Body.Close()
	waitStates(t, ctx, db, 2)
	begin, err := f.Head()
	if err != nil {
		t.Fatal(err)
	}

	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'one')", name))
	exec(t, ctx, db, fmt.Sprintf("UPDATE %s SET body = 'uno' WHERE id = 1", name))
	exec(t, ctx, db, fmt.Sprintf("UPDATE %s SET body = 'eins' WHERE id = 1", name))
	waitFor(t, ctx, f, func() bool { return len(every.types()) == 3 && len(renames.types()) == 1 })
	if got := renames.types(); got[0] != name+".renamed:1" {
		t.Fatalf("named events %v", got)
	}

	// Each delivered batch wrote its destination's cursor once. Those writes
	// are in the stream as well, and pass without a write of their own. The
	// receivers answer before the cursors are saved, so wait for both saves:
	// every past the last update, renames past the first.
	var commits []string
	entries, _ := f.Since(begin.LSN)
	for _, e := range entries {
		if len(e.Tx.Changes) > 0 && e.Tx.Changes[0].Table.Name == name {
			commits = append(commits, e.Tx.CommitLSN.String())
		}
	}
	if len(commits) != 3 {
		t.Fatalf("the feed holds %d transactions of the table, want 3", len(commits))
	}
	waitSaved(t, ctx, db, map[string]string{"every": commits[2], "renames": commits[1]})
	xmin := lastStateWrite(t, ctx, db)
	waitFor(t, ctx, f, func() bool {
		entries, _ := f.Since(begin.LSN)
		for _, e := range entries {
			if e.Tx.Xid == xmin {
				return true
			}
		}
		return false
	})
	time.Sleep(300 * time.Millisecond)
	after := lastStateWrite(t, ctx, db)
	if after != xmin || len(every.types()) != 3 || len(renames.types()) != 1 {
		t.Fatalf("the store's own writes led to more: xmin %d then %d, events %v and %v", xmin, after, every.types(), renames.types())
	}
}

// lastStateWrite returns the transaction id of the latest write of a destination state.
func lastStateWrite(t *testing.T, ctx context.Context, db *pgx.Conn) uint32 {
	t.Helper()
	var xmin uint32
	if err := db.QueryRow(ctx, "SELECT max(xmin::text::bigint) FROM changes.states").Scan(&xmin); err != nil {
		t.Fatal(err)
	}
	return xmin
}

// waitSaved waits until each destination's saved cursor is at least at its
// LSN: a batch also moves past the transactions it took nothing from.
func waitSaved(t *testing.T, ctx context.Context, db *pgx.Conn, want map[string]string) {
	t.Helper()
	for {
		done := true
		for destination, lsn := range want {
			var at bool
			err := db.QueryRow(ctx, "SELECT lsn::pg_lsn >= $2::pg_lsn FROM changes.states WHERE destination = $1",
				destination, lsn).Scan(&at)
			if err != nil {
				t.Fatal(err)
			}
			done = done && at
		}
		if done {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for the cursors %v", want)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitStates waits until the store holds n destination states.
func waitStates(t *testing.T, ctx context.Context, db *pgx.Conn, n int) {
	t.Helper()
	for {
		var count int
		if err := db.QueryRow(ctx, "SELECT count(*) FROM changes.states").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == n {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the destinations to start")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
