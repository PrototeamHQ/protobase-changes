//go:build integration

package tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/reader"
	"github.com/PrototeamHQ/protobase-changes/internal/serve"
)

// sessionsDir holds the recorded relay sessions, one JSON file each, which
// the relay's own tests replay.
const sessionsDir = "relay-sessions"

// testRelayKey is the key the tests' relay connects with.
var testRelayKey = bytes.Repeat([]byte{8}, keys.Size)

// frame is one message on the relay's connection, as it went.
type frame struct {
	From    string          `json:"from"`
	Message json.RawMessage `json:"message"`
}

// relayConn is the relay's end of a connection, recording every frame.
type relayConn struct {
	t      *testing.T
	c      *websocket.Conn
	frames []frame
}

// relayApp follows a fresh table of a real database as app shop with a
// tail of size, and connects a relay to the service serving it.
func relayApp(t *testing.T, ctx context.Context, size int) (*pgx.Conn, string, *feed.Feed, *relayConn) {
	t.Helper()
	url := databaseURL(t)
	db := connect(t, ctx, url)
	name := "pcs_test_" + strings.ReplaceAll(time.Now().Format("150405.000000000"), ".", "_")
	setUp(t, ctx, db, name)
	app := config.App{Name: "shop", DatabaseURL: url, Slot: name, Publication: name, TailTransactions: size, Issuer: issuer}
	f := newFeed(t, app)
	f.RelayKeys = keys.Ring{testRelayKey}
	t.Cleanup(start(t, ctx, reader.New(app, f, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)))
	srv := httptest.NewServer(serve.New(map[string]*feed.Feed{"shop": f}, nil).Handler())
	t.Cleanup(srv.Close)
	waitFor(t, ctx, f, func() bool { _, err := f.Head(); return err == nil })

	header := http.Header{"Authorization": {"Bearer " + base64.RawURLEncoding.EncodeToString(testRelayKey)}}
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+serve.RelayPath, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return db, name, f, &relayConn{t: t, c: c}
}

func (r *relayConn) send(ctx context.Context, msg map[string]any) {
	r.t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.c.Write(ctx, websocket.MessageText, data); err != nil {
		r.t.Fatal(err)
	}
	r.frames = append(r.frames, frame{From: "relay", Message: data})
}

// received is a message from the service, read as a notice or an event.
type received struct {
	Type string `json:"type"`
	App  string `json:"app"`
	ID   string `json:"id"`
	Data struct {
		Registrations []struct {
			ID      string `json:"id"`
			Cursor  string `json:"cursor"`
			Changes []struct {
				Key    map[string]any `json:"key"`
				NewRow map[string]any `json:"new"`
			} `json:"changes"`
		} `json:"registrations"`
	} `json:"data"`
}

func (r *relayConn) receive(ctx context.Context) received {
	r.t.Helper()
	_, data, err := r.c.Read(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	r.frames = append(r.frames, frame{From: "service", Message: data})
	var m received
	if err := json.Unmarshal(data, &m); err != nil {
		r.t.Fatal(err)
	}
	return m
}

// save writes the session's frames to its file in sessionsDir.
func (r *relayConn) save(session string) {
	r.t.Helper()
	data, err := json.MarshalIndent(map[string]any{"session": session, "frames": r.frames}, "", "  ")
	if err != nil {
		r.t.Fatal(err)
	}
	if err := os.MkdirAll(sessionsDir, 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sessionsDir, session+".json"), append(data, '\n'), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

// snapshot returns pg_current_snapshot() as a page's read would take it.
func snapshot(t *testing.T, ctx context.Context, db *pgx.Conn) string {
	t.Helper()
	var s string
	if err := db.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func registerMsg(id, table string, more map[string]any) map[string]any {
	m := map[string]any{"type": "register", "app": "shop", "id": id, "sid": "s-" + id,
		"subs": []map[string]any{{"resource": table}}, "fields": []string{"body"}}
	for k, v := range more {
		m[k] = v
	}
	return m
}

// bodies lists the body of every change an event holds, as "registration:body".
func (m received) bodies() []string {
	var out []string
	for _, entry := range m.Data.Registrations {
		for _, c := range entry.Changes {
			out = append(out, entry.ID+":"+c.NewRow["body"].(string))
		}
	}
	return out
}
