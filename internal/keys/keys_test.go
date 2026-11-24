package keys

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	first, second := bytes.Repeat([]byte{1}, Size), bytes.Repeat([]byte{2}, Size)
	write := func(name string, key []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(base64.RawURLEncoding.EncodeToString(key)+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	ring, err := Load([]string{write("a", first), write("b", second)})
	if err != nil {
		t.Fatal(err)
	}
	if len(ring) != 2 || !bytes.Equal(ring[0], first) || !bytes.Equal(ring[1], second) {
		t.Fatalf("loaded %x", ring)
	}
}

func TestParseRefuses(t *testing.T) {
	cases := map[string]string{
		"short":   base64.RawURLEncoding.EncodeToString(make([]byte, 16)),
		"padded":  base64.URLEncoding.EncodeToString(make([]byte, Size)),
		"invalid": "not a key!",
	}
	for name, input := range cases {
		if _, err := Parse([]byte(input)); err == nil {
			t.Errorf("%s: parsed without error", name)
		}
	}
}
