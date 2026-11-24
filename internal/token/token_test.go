package token

import (
	"bytes"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
)

const issuer = "https://shop.example"

var (
	key  = bytes.Repeat([]byte{1}, keys.Size)
	now  = time.Unix(1795470000, 0)
	ring = keys.Ring{key}
)

func seal(t *testing.T, key []byte, enc jose.ContentEncryption, claims map[string]any) string {
	t.Helper()
	encrypter, err := jose.NewEncrypter(enc, jose.Recipient{Algorithm: jose.DIRECT, Key: key}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jwt.Encrypted(encrypter).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func valid() map[string]any {
	return map[string]any{
		"iss": issuer, "sub": "u42", "sid": "s9", "exp": now.Unix() + 60,
		"payload": "raw", "cursor": "0/1A2B3C",
		"subs": []map[string]any{{"resource": "messages"}, {"resource": "billing.invoices"}},
	}
}

func TestOpen(t *testing.T) {
	tok, err := Open(seal(t, key, jose.A256GCM, valid()), ring, issuer, now)
	if err != nil {
		t.Fatal(err)
	}
	if tok.Subject != "u42" || tok.Session != "s9" || tok.Payload != Raw || tok.Cursor == nil || *tok.Cursor != 0x1A2B3C ||
		!tok.Expiry.Equal(now.Add(time.Minute)) || !tok.Subscriptions.Has("messages") || !tok.Subscriptions.Has("billing.invoices") {
		t.Fatalf("opened %+v", tok)
	}
}

func TestOpenWithAnOlderKey(t *testing.T) {
	newer := bytes.Repeat([]byte{2}, keys.Size)
	if _, err := Open(seal(t, key, jose.A256GCM, valid()), keys.Ring{newer, key}, issuer, now); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRefuses(t *testing.T) {
	with := func(k string, v any) map[string]any {
		c := valid()
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
		return c
	}
	other := bytes.Repeat([]byte{3}, keys.Size)
	cases := map[string]string{
		"garbage":      "a.b.c.d.e",
		"other key":    seal(t, other, jose.A256GCM, valid()),
		"other enc":    seal(t, key, jose.A128CBC_HS256, valid()),
		"expired":      seal(t, key, jose.A256GCM, with("exp", now.Unix())),
		"no exp":       seal(t, key, jose.A256GCM, with("exp", nil)),
		"other issuer": seal(t, key, jose.A256GCM, with("iss", "https://other.example")),
		"no sid":       seal(t, key, jose.A256GCM, with("sid", nil)),
		"bad payload":  seal(t, key, jose.A256GCM, with("payload", "full")),
		"no subs":      seal(t, key, jose.A256GCM, with("subs", []any{})),
		"no resource":  seal(t, key, jose.A256GCM, with("subs", []map[string]any{{"resource": ""}})),
		"bad cursor":   seal(t, key, jose.A256GCM, with("cursor", "1A2B3C")),
		"future nbf":   seal(t, key, jose.A256GCM, with("nbf", now.Unix()+10)),
	}
	for name, raw := range cases {
		if _, err := Open(raw, ring, issuer, now); err != ErrInvalid {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}

func TestOpenCompilesConditions(t *testing.T) {
	c := valid()
	c["subs"] = []map[string]any{
		{"resource": "messages", "where": map[string]any{"eq": []string{"channel_id", "xyz"}}},
		{"resource": "messages", "where": nil},
	}
	tok, err := Open(seal(t, key, jose.A256GCM, c), ring, issuer, now)
	if err != nil {
		t.Fatal(err)
	}
	insert := func(channel string) *change.Change {
		return &change.Change{Operation: change.Insert, NewRow: change.Row{"channel_id": {Kind: change.Text, Text: channel}}}
	}
	if got := tok.Subscriptions.Match("messages", insert("xyz")); len(got) != 2 || got[0].ID != 0 || got[1].ID != 1 {
		t.Fatalf("matched %+v", got)
	}
	if got := tok.Subscriptions.Match("messages", insert("abc")); len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("matched %+v", got)
	}
}

func TestOpenRefusesBadConditions(t *testing.T) {
	c := valid()
	c["subs"] = []map[string]any{{"resource": "messages", "where": map[string]any{"like": []string{"channel_id", "x%"}}}}
	if _, err := Open(seal(t, key, jose.A256GCM, c), ring, issuer, now); err != ErrConditions {
		t.Fatalf("a subscription with bad conditions: %v", err)
	}
}
