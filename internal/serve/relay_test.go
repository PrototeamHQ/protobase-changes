package serve

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/relay"
	"github.com/PrototeamHQ/protobase-changes/internal/visibility"
)

var relayKey = bytes.Repeat([]byte{2}, keys.Size)

// relayServer serves shop, whose relay key is relayKey, and blog, which
// has another.
func relayServer(t *testing.T) string {
	t.Helper()
	feeds := map[string]*feed.Feed{}
	for name, k := range map[string][]byte{"shop": relayKey, "blog": bytes.Repeat([]byte{3}, keys.Size)} {
		f, err := feed.New(config.App{Name: name, Issuer: issuer, TailTransactions: 3}, keys.Ring{key},
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		f.RelayKeys = keys.Ring{k}
		start, err := visibility.Parse("1:1:")
		if err != nil {
			t.Fatal(err)
		}
		f.Started(start)
		if _, err := f.Resume(cursor.Epoch{SystemID: 1, Timeline: 1}, 100, 100); err != nil {
			t.Fatal(err)
		}
		feeds[name] = f
	}
	srv := httptest.NewServer(New(feeds, nil).Handler())
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + RelayPath
}

func dialRelay(ctx context.Context, url string, k []byte) (*websocket.Conn, *http.Response, error) {
	header := http.Header{}
	if k != nil {
		header.Set("Authorization", "Bearer "+base64.RawURLEncoding.EncodeToString(k))
	}
	return websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header, Subprotocols: []string{relay.Subprotocol}})
}

func TestRelayRefusesAWrongKeyBeforeTheHandshake(t *testing.T) {
	url := relayServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, k := range map[string][]byte{"missing": nil, "wrong": bytes.Repeat([]byte{9}, keys.Size), "an app's own key": key} {
		_, res, err := dialRelay(ctx, url, k)
		if err == nil || res == nil || res.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s key: %v %v, want 401", name, res, err)
		}
	}
}

func TestRelayServesOnlyTheAppsItsKeyCovers(t *testing.T) {
	url := relayServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := dialRelay(ctx, url, relayKey)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if c.Subprotocol() != relay.Subprotocol {
		t.Fatalf("subprotocol %q", c.Subprotocol())
	}
	for _, app := range []string{"blog", "shop"} {
		msg, err := json.Marshal(map[string]any{"type": "register", "app": app, "id": "r1", "anonymous": true,
			"snapshot": "1:1:", "subs": []map[string]any{{"resource": "messages"}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Write(ctx, websocket.MessageText, msg); err != nil {
			t.Fatal(err)
		}
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var n relay.Notice
	if err := json.Unmarshal(data, &n); err != nil || n.Type != relay.RefusedType || n.App != "blog" {
		t.Fatalf("got %s, want blog refused", data)
	}
}
