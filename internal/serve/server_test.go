package serve

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/revoke"
)

const issuer = "https://shop.example"

var key = bytes.Repeat([]byte{1}, keys.Size)

type fixture struct {
	feed *feed.Feed
	url  string
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	f, err := feed.New(config.App{Name: "shop", Issuer: issuer, TailTransactions: 3}, keys.Ring{key},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Resume(cursor.Epoch{SystemID: 1, Timeline: 1}, 100, 100); err != nil {
		t.Fatal(err)
	}
	s := New(map[string]*feed.Feed{"shop": f})
	s.keepalive = 50 * time.Millisecond
	s.pollTimeout = 50 * time.Millisecond
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return fixture{feed: f, url: srv.URL}
}

// insert appends a transaction of one insert into messages at lsn.
func (fx fixture) insert(lsn pglogrepl.LSN, messages ...change.Message) {
	fx.feed.Append(&change.Transaction{CommitLSN: lsn, EndLSN: lsn + 8, Messages: messages, Changes: []change.Change{
		{Table: change.Table{Schema: "public", Name: "messages"}, Operation: change.Insert,
			Key: change.Row{"id": {Kind: change.Text, Text: strconv.Itoa(int(lsn))}}},
	}})
}

func (fx fixture) cursor(lsn pglogrepl.LSN) string {
	return fx.feed.Cursors.Seal(cursor.Position{Epoch: cursor.Epoch{SystemID: 1, Timeline: 1}, LSN: lsn})
}

func claims(change func(map[string]any)) map[string]any {
	c := map[string]any{
		"iss": issuer, "sub": "u42", "sid": "s9", "exp": time.Now().Add(time.Hour).Unix(),
		"payload": "thin", "subs": []map[string]any{{"resource": "messages"}},
	}
	if change != nil {
		change(c)
	}
	return c
}

func seal(t *testing.T, claims map[string]any) string {
	t.Helper()
	enc, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.DIRECT, Key: key}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jwt.Encrypted(enc).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func get(t *testing.T, url, tok string, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	return res
}

func poll(t *testing.T, fx fixture, tok, after string) []event.Event {
	t.Helper()
	res := get(t, fx.url+ChangesPath("shop", after), tok, nil)
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != batchContentType {
		t.Fatalf("poll answered %d %s", res.StatusCode, res.Header.Get("Content-Type"))
	}
	var events []event.Event
	if err := json.NewDecoder(res.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	return events
}

func ids(events []event.Event) []string {
	var out []string
	for _, e := range events {
		if e.Data == nil {
			out = append(out, e.Type)
			continue
		}
		out = append(out, e.Data.Changes[0].Key["id"].(string))
	}
	return out
}

func TestLongPollResumesFromEachCursor(t *testing.T) {
	fx := newFixture(t)
	fx.insert(110)
	fx.insert(120)
	tok := seal(t, claims(nil))

	got := poll(t, fx, tok, fx.cursor(100))
	if strings.Join(ids(got), ",") != "110,120" {
		t.Fatalf("replayed %v", ids(got))
	}
	// Nothing new: the poll times out with a position at the same place.
	idle := poll(t, fx, tok, got[1].Cursor)
	if len(idle) != 1 || idle[0].Type != event.PositionType {
		t.Fatalf("idle poll %v", ids(idle))
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		fx.insert(130)
	}()
	if live := poll(t, fx, tok, idle[0].Cursor); strings.Join(ids(live), ",") != "130" {
		t.Fatalf("live %v", ids(live))
	}
}

func TestRefusals(t *testing.T) {
	fx := newFixture(t)
	for _, lsn := range []pglogrepl.LSN{110, 120, 130, 140} {
		fx.insert(lsn)
	}
	other := bytes.Repeat([]byte{2}, keys.Size)
	otherCodec, err := cursor.New(keys.Ring{other})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		path, tok string
		want      int
	}{
		"unknown app":  {ChangesPath("blog", ""), seal(t, claims(nil)), http.StatusNotFound},
		"no token":     {ChangesPath("shop", ""), "", http.StatusUnauthorized},
		"bad token":    {ChangesPath("shop", ""), "abc", http.StatusUnauthorized},
		"other issuer": {ChangesPath("shop", ""), seal(t, claims(func(c map[string]any) { c["iss"] = "https://blog.example" })), http.StatusUnauthorized},
		"no session":   {ChangesPath("shop", ""), seal(t, claims(func(c map[string]any) { delete(c, "sid") })), http.StatusUnauthorized},
		"conditions": {ChangesPath("shop", ""), seal(t, claims(func(c map[string]any) {
			c["subs"] = []map[string]any{{"resource": "messages", "where": map[string]any{"eq": []string{"id", "1"}}}}
		})), http.StatusBadRequest},
		"bad cursor":    {ChangesPath("shop", "abc"), seal(t, claims(nil)), http.StatusBadRequest},
		"forged cursor": {ChangesPath("shop", otherCodec.Seal(cursor.Position{Epoch: cursor.Epoch{SystemID: 1, Timeline: 1}, LSN: 130})), seal(t, claims(nil)), http.StatusBadRequest},
		"gone":          {ChangesPath("shop", fx.cursor(100)), seal(t, claims(nil)), http.StatusGone},
	}
	for name, c := range cases {
		if res := get(t, fx.url+c.path, c.tok, nil); res.StatusCode != c.want {
			t.Errorf("%s: %d, want %d", name, res.StatusCode, c.want)
		}
	}
}

func TestRevokedSessionIsRefused(t *testing.T) {
	fx := newFixture(t)
	exp := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	fx.insert(110, change.Message{Prefix: revoke.Prefix, Content: []byte(`{"sid":"s9","exp":` + exp + `}`)})
	if res := get(t, fx.url+ChangesPath("shop", fx.cursor(100)), seal(t, claims(nil)), nil); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked session got %d", res.StatusCode)
	}
	other := seal(t, claims(func(c map[string]any) { c["sid"] = "s8" }))
	if got := poll(t, fx, other, fx.cursor(100)); strings.Join(ids(got), ",") != "110" {
		t.Fatalf("another session got %v", ids(got))
	}
}

func TestEventStreamResumesWithLastEventID(t *testing.T) {
	fx := newFixture(t)
	fx.insert(110)
	fx.insert(120)
	tok := seal(t, claims(nil))
	// The reconnect keeps the first URL's cursor; the last event's id wins.
	header := http.Header{"Accept": {"text/event-stream"}, "Last-Event-Id": {fx.cursor(110)}}
	res := get(t, fx.url+ChangesPath("shop", fx.cursor(100)), tok, header)
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "text/event-stream" ||
		res.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("answered %d %v", res.StatusCode, res.Header)
	}
	lines := bufio.NewScanner(res.Body)
	read := func() (id string, e event.Event) {
		t.Helper()
		for lines.Scan() {
			line := lines.Text()
			switch {
			case strings.HasPrefix(line, "id: "):
				id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "data: "):
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e); err != nil {
					t.Fatal(err)
				}
			case line == "":
				return id, e
			}
		}
		t.Fatalf("stream ended: %v", lines.Err())
		return
	}
	id, e := read()
	if e.Data == nil || e.Data.Changes[0].Key["id"] != "120" || id != e.Cursor {
		t.Fatalf("first event %s %+v", id, e)
	}
	if _, e := read(); e.Type != event.PositionType {
		t.Fatalf("keepalive %+v", e)
	}
	fx.insert(130)
	if _, e := read(); e.Data == nil || e.Data.Changes[0].Key["id"] != "130" {
		t.Fatalf("live event %+v", e)
	}
}

func TestWebSocketStreamsUntilRevoked(t *testing.T) {
	fx := newFixture(t)
	fx.insert(110)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, res, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(fx.url, "http")+ChangesPath("shop", fx.cursor(100)),
		&websocket.DialOptions{
			HTTPHeader:   http.Header{"Authorization": {"Bearer " + seal(t, claims(nil))}},
			Subprotocols: []string{Subprotocol},
		})
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if res.Header.Get("Sec-WebSocket-Protocol") != Subprotocol {
		t.Fatalf("subprotocol %q", res.Header.Get("Sec-WebSocket-Protocol"))
	}
	read := func() event.Event {
		t.Helper()
		typ, data, err := c.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var e event.Event
		if typ != websocket.MessageText || json.Unmarshal(data, &e) != nil {
			t.Fatalf("message %v %s", typ, data)
		}
		return e
	}
	if e := read(); e.Data == nil || e.Data.Changes[0].Key["id"] != "110" {
		t.Fatalf("replayed %+v", e)
	}
	exp := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	fx.insert(120, change.Message{Prefix: revoke.Prefix, Content: []byte(`{"sid":"s9","exp":` + exp + `}`)})
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
				t.Fatalf("closed with %v", err)
			}
			return
		}
		var e event.Event
		if json.Unmarshal(data, &e) != nil || e.Type != event.PositionType {
			t.Fatalf("got %s after the revocation", data)
		}
	}
}

func TestWebSocketRefusesBeforeTheHandshake(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, res, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(fx.url, "http")+ChangesPath("shop", ""), nil)
	if err == nil || res == nil || res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("dial without a token: %v %v", res, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("timed out")
	}
}
