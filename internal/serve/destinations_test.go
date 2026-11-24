package serve

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/store"
)

// destinationServer serves shop with a hub on a SQLite store, and blog without one.
func destinationServer(t *testing.T) (url string, kept *store.SQL) {
	t.Helper()
	fx := newFixture(t)
	ctx := context.Background()
	kept, err := store.Open(config.Store{Kind: config.SQLite, Path: filepath.Join(t.TempDir(), "changes.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { kept.Close() })
	if err := kept.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := destination.NewHub(ctx, fx.feed, kept, &destination.Sender{Client: http.DefaultClient, Now: time.Now}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		hub.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	s := New(map[string]*feed.Feed{"shop": fx.feed, "blog": fx.feed}, map[string]*destination.Hub{"shop": hub})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv.URL, kept
}

func set(t *testing.T, iat int64) string {
	secret := "whsec_" + base64.StdEncoding.EncodeToString([]byte("a secret of the app"))
	return seal(t, map[string]any{"iss": issuer, "iat": iat, "destinations": []map[string]any{{
		"name": "orders-hook", "url": "https://receiver.example/hook", "secret": secret, "payload": "thin",
		"subs": []map[string]any{{"resource": "orders"}}, "retry": map[string]any{"at_limit": "pause"},
	}}})
}

func do(t *testing.T, method, url, auth, body string) int {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestPutDestinations(t *testing.T) {
	url, kept := destinationServer(t)
	now := time.Now().Unix()
	raw := set(t, now)
	cases := []struct {
		name, app, body string
		want            int
	}{
		{"valid", "shop", raw, http.StatusNoContent},
		{"the same again", "shop", raw + "\n", http.StatusNoContent},
		{"older", "shop", set(t, now-1), http.StatusConflict},
		{"not a token", "shop", "nonsense", http.StatusUnauthorized},
		{"live token", "shop", seal(t, claims(nil)), http.StatusUnauthorized},
		{"invalid destinations", "shop", seal(t, map[string]any{"iss": issuer, "iat": now + 1, "destinations": []any{1}}), http.StatusBadRequest},
		{"no store", "blog", raw, http.StatusNotFound},
		{"unknown app", "nope", raw, http.StatusNotFound},
	}
	for _, c := range cases {
		if got := do(t, http.MethodPut, url+DestinationsPath(c.app), "", c.body); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
	if stored, err := kept.Definitions(context.Background(), "shop"); err != nil || stored != raw {
		t.Fatalf("kept %q %v, want the put token", stored, err)
	}
}

func TestResumeDestination(t *testing.T) {
	url, _ := destinationServer(t)
	if got := do(t, http.MethodPut, url+DestinationsPath("shop"), "", set(t, time.Now().Unix())); got != http.StatusNoContent {
		t.Fatalf("put %d", got)
	}
	resume := func(name string, exp time.Time) string {
		return seal(t, map[string]any{"iss": issuer, "exp": exp.Unix(), "resume": name})
	}
	later := time.Now().Add(time.Minute)
	cases := []struct {
		name, destination, token string
		want                     int
	}{
		{"valid", "orders-hook", resume("orders-hook", later), http.StatusNoContent},
		{"other name", "orders-hook", resume("audit", later), http.StatusUnauthorized},
		{"expired", "orders-hook", resume("orders-hook", time.Now().Add(-time.Minute)), http.StatusUnauthorized},
		{"no token", "orders-hook", "", http.StatusUnauthorized},
		{"unknown", "audit", resume("audit", later), http.StatusNotFound},
	}
	for _, c := range cases {
		if got := do(t, http.MethodPost, url+ResumePath("shop", c.destination), c.token, ""); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
}
