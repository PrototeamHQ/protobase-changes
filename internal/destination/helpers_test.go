package destination

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/PrototeamHQ/protobase-changes/internal/keys"
)

const issuer = "https://shop.example"

var (
	key    = bytes.Repeat([]byte{1}, keys.Size)
	ring   = keys.Ring{key}
	secret = "whsec_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 24))
	now    = time.Unix(1795470000, 0)
)

func seal(t *testing.T, claims map[string]any) string {
	t.Helper()
	encrypter, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.DIRECT, Key: key}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jwt.Encrypted(encrypter).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// hook is a valid destination of every transaction of orders.
func hook(url string) map[string]any {
	return map[string]any{
		"name": "orders-hook", "url": url, "secret": secret, "payload": "raw",
		"subs":  []map[string]any{{"resource": "orders"}},
		"retry": map[string]any{"at_limit": "pause"},
	}
}

func setToken(t *testing.T, iat int64, destinations ...map[string]any) string {
	t.Helper()
	if destinations == nil {
		destinations = []map[string]any{}
	}
	return seal(t, map[string]any{"iss": issuer, "iat": iat, "destinations": destinations})
}
