package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

// dial connects to Serve serving feeds.
func dial(t *testing.T, feeds map[string]*feed.Feed) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		Serve(r.Context(), c, feeds, time.Now)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func write(t *testing.T, c *websocket.Conn, msg map[string]any) {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(context.Background(), websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, c *websocket.Conn, into any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatal(err)
	}
}

func TestServeSendsEachAppItsChanges(t *testing.T) {
	shop, blog := newFeed(t, 10), newFeed(t, 10)
	c := dial(t, map[string]*feed.Feed{"shop": shop, "blog": blog})
	write(t, c, register("r1", map[string]any{"cursor": seal(shop, 100)}))
	write(t, c, register("r1", map[string]any{"app": "blog", "cursor": seal(blog, 100)}))
	// An app the key does not cover is refused, like one that does not exist.
	write(t, c, register("r1", map[string]any{"app": "other", "cursor": seal(shop, 100)}))
	var n Notice
	read(t, c, &n)
	if n.Type != RefusedType || n.App != "other" || n.ID != "r1" {
		t.Fatalf("got %+v, want refused", n)
	}
	insert(blog, 11, 110)
	var e Event
	read(t, c, &e)
	if e.App != "blog" || e.Data.Registrations[0].ID != "r1" {
		t.Fatalf("got %+v, want blog's change", e)
	}
}

func TestAnInvalidMessageClosesTheConnection(t *testing.T) {
	for name, m := range map[string]map[string]any{
		"unknown type":  {"type": "subscribe", "app": "shop", "id": "r1"},
		"unknown field": register("r1", map[string]any{"cursor": "abc", "extra": 1}),
		"no id":         {"type": "unregister", "app": "shop"},
	} {
		c := dial(t, map[string]*feed.Feed{"shop": newFeed(t, 10)})
		write(t, c, m)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, err := c.Read(ctx)
		cancel()
		if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
			t.Errorf("%s: %v, want a policy violation", name, err)
		}
	}
}
