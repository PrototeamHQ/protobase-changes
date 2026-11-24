package config

import (
	"strings"
	"testing"
)

const valid = `{"apps": [{"name": "shop", "database_url": "postgres://localhost/shop",
  "slot": "protobase_changes", "publication": "protobase_changes", "tail_transactions": 1000}]}`

func TestParseValid(t *testing.T) {
	c, err := Parse(strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Apps) != 1 || c.Apps[0].Slot != "protobase_changes" || c.Apps[0].TailTransactions != 1000 {
		t.Fatalf("parsed %+v", c)
	}
}

func TestParseRefuses(t *testing.T) {
	cases := map[string]string{
		"unknown field": `{"apps": [], "extra": 1}`,
		"no apps":       `{"apps": []}`,
		"slot case":     strings.Replace(valid, `"slot": "protobase_changes"`, `"slot": "Protobase"`, 1),
		"slot length":   strings.Replace(valid, `"slot": "protobase_changes"`, `"slot": "`+strings.Repeat("a", 64)+`"`, 1),
		"publication":   strings.Replace(valid, `"publication": "protobase_changes"`, `"publication": "a-b"`, 1),
		"no url":        strings.Replace(valid, `"postgres://localhost/shop"`, `""`, 1),
		"no tail":       strings.Replace(valid, `1000`, `0`, 1),
		"twice":         `{"apps": [` + valid[10:len(valid)-2] + `, ` + valid[10:len(valid)-2] + `]}`,
		"trailing":      valid + `{}`,
	}
	for name, input := range cases {
		if _, err := Parse(strings.NewReader(input)); err == nil {
			t.Errorf("%s: parsed without error", name)
		}
	}
}
