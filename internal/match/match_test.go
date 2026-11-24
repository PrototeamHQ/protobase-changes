package match

import (
	"reflect"
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

func index(t *testing.T, wheres ...string) *Index {
	t.Helper()
	subs := make([]Subscription, len(wheres))
	for i, where := range wheres {
		subs[i] = Subscription{ID: 10 + i, Resource: "messages"}
		if where != "" {
			subs[i].Where = compile(t, where)
		}
	}
	return NewIndex(subs)
}

func row(kv ...string) change.Row {
	r := change.Row{}
	for i := 0; i < len(kv); i += 2 {
		r[kv[i]] = text(kv[i+1])
	}
	return r
}

func expect(t *testing.T, name string, got []Tag, want ...Tag) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: %+v, want %+v", name, got, want)
	}
}

func TestEffects(t *testing.T) {
	ix := index(t, `{"eq": ["channel_id", "a"]}`)
	key := row("id", "1")
	inA, inB := row("id", "1", "channel_id", "a"), row("id", "1", "channel_id", "b")
	cases := map[string]struct {
		c    change.Change
		want []Tag
	}{
		"insert in":  {change.Change{Operation: change.Insert, Key: key, NewRow: inA}, []Tag{{10, Enter}}},
		"insert out": {change.Change{Operation: change.Insert, Key: key, NewRow: inB}, nil},
		"update in to in": {change.Change{Operation: change.Update, Key: key, OldRowKind: change.OldFull, OldRow: inA, NewRow: inA},
			[]Tag{{10, Update}}},
		"update out to in": {change.Change{Operation: change.Update, Key: key, OldRowKind: change.OldFull, OldRow: inB, NewRow: inA},
			[]Tag{{10, Enter}}},
		"update in to out": {change.Change{Operation: change.Update, Key: key, OldRowKind: change.OldFull, OldRow: inA, NewRow: inB},
			[]Tag{{10, Leave}}},
		"update out to out": {change.Change{Operation: change.Update, Key: key, OldRowKind: change.OldFull, OldRow: inB, NewRow: inB}, nil},
		// Without the old row the update may be an enter: the consumer keeps the row.
		"update unknown to in": {change.Change{Operation: change.Update, Key: key, NewRow: inA}, []Tag{{10, Update}}},
		// It may have left too, but saying so would show a row of channel b.
		"update unknown to out": {change.Change{Operation: change.Update, Key: key, NewRow: inB}, nil},
		"delete in": {change.Change{Operation: change.Delete, Key: key, OldRowKind: change.OldFull, OldRow: inA},
			[]Tag{{10, Leave}}},
		"delete out":     {change.Change{Operation: change.Delete, Key: key, OldRowKind: change.OldFull, OldRow: inB}, nil},
		"delete unknown": {change.Change{Operation: change.Delete, Key: key, OldRowKind: change.OldKeyOnly, OldRow: key}, nil},
		"truncate":       {change.Change{Operation: change.Truncate}, []Tag{{10, Leave}}},
	}
	for name, tc := range cases {
		expect(t, name, ix.Match("messages", &tc.c), tc.want...)
	}
}

func TestKeyConditionsNeedNoFullOldRow(t *testing.T) {
	ix := index(t, `{"eq": ["id", 1]}`)
	key := row("id", "1")
	expect(t, "update keeping the key", ix.Match("messages", &change.Change{Operation: change.Update, Key: key, NewRow: row("id", "1", "x", "y")}),
		Tag{10, Update})
	expect(t, "update changing the key", ix.Match("messages", &change.Change{Operation: change.Update, Key: row("id", "2"),
		OldRowKind: change.OldKeyOnly, OldRow: key, NewRow: row("id", "2")}), Tag{10, Leave})
	expect(t, "delete", ix.Match("messages", &change.Change{Operation: change.Delete, Key: key, OldRowKind: change.OldKeyOnly, OldRow: key}),
		Tag{10, Leave})
}

func TestUnchangedValuesComeFromTheFullOldRow(t *testing.T) {
	ix := index(t, `{"eq": ["body", "long"]}`)
	unchanged := change.Row{"id": text("1"), "body": {Kind: change.Unchanged}}
	expect(t, "with the old row", ix.Match("messages", &change.Change{Operation: change.Update,
		OldRowKind: change.OldFull, OldRow: row("id", "1", "body", "long"), NewRow: unchanged}), Tag{10, Update})
	expect(t, "without", ix.Match("messages", &change.Change{Operation: change.Update, Key: row("id", "1"), NewRow: unchanged}))
}

func TestOneChangeIsTaggedWithEverySubscriptionInOrder(t *testing.T) {
	ix := index(t,
		`{"eq": ["channel_id", "b"]}`,
		"",
		`{"and": [{"in": ["channel_id", ["a", "c"]]}, {"eq": ["tenant_id", "t1"]}]}`,
		`{"eq": ["channel_id", "a"]}`,
		`{"gt": ["n", 5]}`,
		`{"eq": ["channel_id", "z"]}`,
	)
	c := change.Change{Operation: change.Update, OldRowKind: change.OldFull,
		OldRow: row("id", "1", "channel_id", "b", "tenant_id", "t1", "n", "9"), NewRow: row("id", "1", "channel_id", "a", "tenant_id", "t1", "n", "1")}
	expect(t, "update", ix.Match("messages", &c), Tag{10, Leave}, Tag{11, Update}, Tag{12, Enter}, Tag{13, Enter}, Tag{14, Leave})
	expect(t, "other resource", ix.Match("orders", &c))
}

func TestEqualitiesAreLookedUpNotScanned(t *testing.T) {
	ix := index(t,
		`{"eq": ["channel_id", "a"]}`,
		`{"eq": ["channel_id", "b"]}`,
		`{"and": [{"gt": ["n", 1]}, {"and": [{"eq": ["tenant_id", 7]}]}]}`,
		`{"in": ["tenant_id", [8, 9]]}`,
		`{"or": [{"eq": ["channel_id", "a"]}, {"eq": ["channel_id", "b"]}]}`,
	)
	r := ix.resources["messages"]
	ids := func(rows ...*view) []int {
		var out []int
		for _, e := range r.candidates(rows...) {
			out = append(out, e.ID)
		}
		return out
	}
	if got := ids(&view{row: row("channel_id", "b", "tenant_id", "7.0")}); !reflect.DeepEqual(got, []int{11, 12, 14}) {
		t.Errorf("candidates %v", got)
	}
	if got := ids(&view{row: row("channel_id", "a", "tenant_id", "9")}, &view{row: row("channel_id", "c", "tenant_id", "x")}); !reflect.DeepEqual(got, []int{10, 13, 14}) {
		t.Errorf("candidates of two rows %v", got)
	}
	if !reflect.DeepEqual(r.columns, []string{"channel_id", "tenant_id"}) || len(r.scan) != 1 {
		t.Errorf("indexed %v, scanned %d", r.columns, len(r.scan))
	}
}
