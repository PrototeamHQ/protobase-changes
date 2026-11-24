package destination

import (
	"errors"
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

func TestOpenSet(t *testing.T) {
	paid := hook("https://receiver.example/hook")
	paid["name"], paid["function"], paid["batch_size"] = "paid", "route-orders", 10
	paid["subs"] = nil
	paid["events"] = []map[string]any{{"type": "orders.paid", "resource": "orders", "operations": []string{"insert", "update"},
		"old": map[string]any{"neq": []any{"status", "paid"}}, "new": map[string]any{"eq": []any{"status", "paid"}}}}
	paid["retry"] = map[string]any{"tries": 3, "at_limit": "dead_letter"}

	set, err := OpenSet(setToken(t, now.Unix(), hook("https://receiver.example/hook"), paid), ring, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if !set.IssuedAt.Equal(now) || len(set.Destinations) != 2 {
		t.Fatalf("set %+v", set)
	}
	h, p := set.Destinations[0], set.Destinations[1]
	if h.BatchSize != defaultBatchSize || h.Tries != maxTries || h.AtLimit != Pause || h.Payload != token.Raw || !h.Subscriptions.Has("orders") {
		t.Fatalf("hook %+v", h)
	}
	if p.BatchSize != 10 || p.Tries != 3 || p.AtLimit != DeadLetter || p.Function != "route-orders" || len(p.Events) != 1 ||
		p.Events[0].Type != "orders.paid" || len(p.Events[0].Operations) != 2 || p.Events[0].When.Old == nil {
		t.Fatalf("paid %+v", p)
	}
	if h.digest == p.digest {
		t.Fatal("two definitions share a digest")
	}

	row := func(status string) change.Row {
		return change.Row{"id": {Kind: change.Text, Text: "1"}, "status": {Kind: change.Text, Text: status}}
	}
	turned := &change.Change{Operation: change.Update, OldRowKind: change.OldFull, OldRow: row("open"), NewRow: row("paid")}
	if got := p.types("orders", turned); len(got) != 1 || got[0] != "orders.paid" {
		t.Fatalf("types %v, want orders.paid", got)
	}
	twice := hook("https://receiver.example/hook")
	twice["events"] = []map[string]any{{"type": "orders.touched", "resource": "orders"}, {"type": "orders.touched", "resource": "orders"}}
	both, err := OpenSet(setToken(t, now.Unix(), twice), ring, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if got := both.Destinations[0].types("orders", turned); len(got) != 1 {
		t.Fatalf("types %v, want each type once", got)
	}
	deleted := &change.Change{Operation: change.Delete, OldRowKind: change.OldFull, OldRow: row("open")}
	if got := p.types("orders", deleted); len(got) != 0 {
		t.Fatalf("a delete raised %v outside the event's operations", got)
	}
}

func TestOpenSetRefuses(t *testing.T) {
	valid := func() map[string]any { return hook("https://receiver.example/hook") }
	with := func(key string, value any) map[string]any {
		d := valid()
		d[key] = value
		return d
	}
	invalidDefinitions := map[string][]map[string]any{
		"name":          {with("name", "1st")},
		"no target":     {with("url", "")},
		"relative url":  {with("url", "/hook")},
		"secret":        {with("secret", "not-whsec")},
		"payload":       {with("payload", "full")},
		"batch size":    {with("batch_size", maxBatchSize+1)},
		"tries":         {with("tries", 1)},
		"retry tries":   {with("retry", map[string]any{"tries": maxTries + 1, "at_limit": "pause"})},
		"at limit":      {with("retry", map[string]any{"at_limit": "drop"})},
		"nothing":       {with("subs", nil)},
		"where":         {with("subs", []map[string]any{{"resource": "orders", "where": map[string]any{"like": []any{"a", "b"}}}})},
		"event type":    {with("events", []map[string]any{{"type": "paid", "resource": "orders"}})},
		"event op":      {with("events", []map[string]any{{"type": "orders.cleared", "resource": "orders", "operations": []string{"truncate"}}})},
		"twice":         {valid(), valid()},
		"function name": {with("function", "a/b")},
	}
	for name, defs := range invalidDefinitions {
		if _, err := OpenSet(setToken(t, now.Unix(), defs...), ring, issuer); !errors.Is(err, ErrDefinitions) {
			t.Errorf("%s: %v, want invalid destinations", name, err)
		}
	}

	invalidTokens := map[string]map[string]any{
		"issuer":      {"iss": "https://other.example", "iat": now.Unix(), "destinations": []any{}},
		"no iat":      {"iss": issuer, "destinations": []any{}},
		"expiry":      {"iss": issuer, "iat": now.Unix(), "exp": now.Unix() + 60, "destinations": []any{}},
		"live token":  {"iss": issuer, "iat": now.Unix(), "sid": "s9", "destinations": []any{}},
		"resume":      {"iss": issuer, "iat": now.Unix(), "resume": "orders-hook", "destinations": []any{}},
		"no dest key": {"iss": issuer, "iat": now.Unix()},
	}
	for name, claims := range invalidTokens {
		if _, err := OpenSet(seal(t, claims), ring, issuer); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: %v, want invalid token", name, err)
		}
	}
	if _, err := OpenSet("not.a.token", ring, issuer); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("garbage: %v", err)
	}
}

func TestOpenResume(t *testing.T) {
	name, err := OpenResume(seal(t, map[string]any{"iss": issuer, "exp": now.Unix() + 60, "resume": "orders-hook"}), ring, issuer, now)
	if err != nil || name != "orders-hook" {
		t.Fatalf("resume %q %v", name, err)
	}
	for label, claims := range map[string]map[string]any{
		"expired":   {"iss": issuer, "exp": now.Unix(), "resume": "orders-hook"},
		"no expiry": {"iss": issuer, "resume": "orders-hook"},
		"set":       {"iss": issuer, "exp": now.Unix() + 60, "resume": "orders-hook", "destinations": []any{}},
		"live":      {"iss": issuer, "exp": now.Unix() + 60, "resume": "orders-hook", "sid": "s9"},
		"no name":   {"iss": issuer, "exp": now.Unix() + 60},
	} {
		if _, err := OpenResume(seal(t, claims), ring, issuer, now); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: %v, want invalid token", label, err)
		}
	}
}
