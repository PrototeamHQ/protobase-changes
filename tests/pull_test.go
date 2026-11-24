//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/serve"
)

const issuer = "https://shop.example"

// TestPullResumesByCursor follows a real database through the long poll
// endpoint: a consumer resumes after each cursor without acknowledging
// anything, and the app's revocation of its session cuts it off.
func TestPullResumesByCursor(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := connect(t, ctx, url)
	name := fmt.Sprintf("pcs_test_%d", time.Now().UnixNano())
	setUp(t, ctx, db, name)
	app := config.App{Name: "shop", DatabaseURL: url, Slot: name, Publication: name, TailTransactions: 100, Issuer: issuer}
	f := newFeed(t, app)
	stop := start(t, ctx, reader.New(app, f, slog.New(slog.NewTextHandler(io.Discard, nil)), nil))
	defer stop()
	srv := httptest.NewServer(serve.New(map[string]*feed.Feed{"shop": f}).Handler())
	defer srv.Close()
	tok := sealToken(t, name)

	// The consumer starts where the stream started, as from a snapshot read there.
	waitFor(t, ctx, f, func() bool { _, err := f.Head(); return err == nil })
	head, err := f.Head()
	if err != nil {
		t.Fatal(err)
	}
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'one')", name))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'two')", name))

	got := collect(t, ctx, srv.URL, tok, f.Cursors.Seal(head), 2)
	if got[0].Data.Changes[0].NewRow["body"] != "one" || got[1].Data.Changes[0].NewRow["body"] != "two" {
		t.Fatalf("pulled %+v", got)
	}
	// The first event's cursor resumes right after it, as often as asked.
	for range 2 {
		again := collect(t, ctx, srv.URL, tok, got[0].Cursor, 1)
		if again[0].ID != got[1].ID || again[0].Cursor == "" {
			t.Fatalf("resumed to %+v, want %+v", again[0], got[1])
		}
	}

	exec(t, ctx, db, fmt.Sprintf(`BEGIN; SELECT pg_logical_emit_message(true, 'protobase_revoke', '{"sid":"s9","exp":%d}');
		INSERT INTO %s (id, body) VALUES (3, 'after'); COMMIT`, time.Now().Add(time.Hour).Unix(), name))
	waitFor(t, ctx, f, func() bool { return f.Revoked.Revoked("s9", time.Now()) })
	if _, status := pull(t, ctx, srv.URL, tok, got[1].Cursor, time.Now().Add(time.Second)); status != http.StatusUnauthorized {
		t.Fatalf("revoked session got %d", status)
	}
}

// collect long polls from after until it holds n transaction events.
func collect(t *testing.T, ctx context.Context, base, tok, after string, n int) []event.Event {
	t.Helper()
	var out []event.Event
	for len(out) < n {
		events, status := pull(t, ctx, base, tok, after, time.Now().Add(30*time.Second))
		if status != http.StatusOK {
			t.Fatalf("pull answered %d", status)
		}
		for _, e := range events {
			if e.Type == event.TransactionType {
				out = append(out, e)
			}
			after = e.Cursor
		}
	}
	return out
}

func pull(t *testing.T, ctx context.Context, base, tok, after string, deadline time.Time) ([]event.Event, int) {
	t.Helper()
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+serve.ChangesPath("shop", after), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			// The poll was still waiting at the deadline.
			return nil, 0
		}
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, res.StatusCode
	}
	var events []event.Event
	if err := json.NewDecoder(res.Body).Decode(&events); err != nil {
		if ctx.Err() != nil {
			return nil, 0
		}
		t.Fatal(err)
	}
	return events, res.StatusCode
}

func sealToken(t *testing.T, table string) string {
	t.Helper()
	enc, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.DIRECT, Key: testKey}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jwt.Encrypted(enc).Claims(map[string]any{
		"iss": issuer, "sub": "u42", "sid": "s9", "exp": time.Now().Add(time.Hour).Unix(),
		"payload": "raw", "subs": []map[string]any{{"resource": table}},
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}
