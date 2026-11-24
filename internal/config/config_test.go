package config

import (
	"strings"
	"testing"
)

const valid = `{"apps": [{"name": "shop", "database_url": "postgres://localhost/shop",
  "slot": "protobase_changes", "publication": "protobase_changes", "tail_transactions": 1000,
  "issuer": "https://shop.example", "key_files": ["/run/secrets/shop_key"]}], "listen": ":8080"}`

func TestParseValid(t *testing.T) {
	c, err := Parse(strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Apps) != 1 || c.Apps[0].Slot != "protobase_changes" || c.Apps[0].TailTransactions != 1000 {
		t.Fatalf("parsed %+v", c)
	}
}

func TestParseAcceptsTrailingWhitespace(t *testing.T) {
	if _, err := Parse(strings.NewReader(valid + "\n \n")); err != nil {
		t.Fatal(err)
	}
}

func TestParseRefuses(t *testing.T) {
	cases := map[string]string{
		"unknown field": `{"apps": [], "extra": 1}`,
		"no apps":       `{"listen": ":8080", "apps": []}`,
		"slot case":     strings.Replace(valid, `"slot": "protobase_changes"`, `"slot": "Protobase"`, 1),
		"slot length":   strings.Replace(valid, `"slot": "protobase_changes"`, `"slot": "`+strings.Repeat("a", 64)+`"`, 1),
		"publication":   strings.Replace(valid, `"publication": "protobase_changes"`, `"publication": "a-b"`, 1),
		"no url":        strings.Replace(valid, `"postgres://localhost/shop"`, `""`, 1),
		"no tail":       strings.Replace(valid, `1000`, `0`, 1),
		"twice":         strings.Replace(valid, `}]`, `}, `+valid[10:strings.Index(valid, `}]`)+1]+`]`, 1),
		"no listen":     strings.Replace(valid, `":8080"`, `""`, 1),
		"no issuer":     strings.Replace(valid, `"https://shop.example"`, `""`, 1),
		"no keys":       strings.Replace(valid, `["/run/secrets/shop_key"]`, `[]`, 1),
		"trailing":      valid + `{}`,
		"trailing ]":    valid + `]`,
		"trailing }":    valid + `}`,
		"slot digit":    strings.Replace(valid, `"slot": "protobase_changes"`, `"slot": "123"`, 1),
	}
	for name, input := range cases {
		if _, err := Parse(strings.NewReader(input)); err == nil {
			t.Errorf("%s: parsed without error", name)
		}
	}
}
