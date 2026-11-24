package match

import (
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

func TestTransitionHolds(t *testing.T) {
	paid := Transition{
		Old: compile(t, `{"neq": ["status", "paid"]}`),
		New: compile(t, `{"eq": ["status", "paid"]}`),
	}
	full := func(op change.Operation, old, new string) *change.Change {
		c := &change.Change{Operation: op, Key: change.Row{"id": text("1")}}
		if old != "" {
			c.OldRowKind, c.OldRow = change.OldFull, change.Row{"id": text("1"), "status": text(old)}
		}
		if new != "" {
			c.NewRow = change.Row{"id": text("1"), "status": text(new)}
		}
		return c
	}
	keyOnly := &change.Change{Operation: change.Update, Key: change.Row{"id": text("1")},
		NewRow: change.Row{"id": text("1"), "status": text("paid")}}

	cases := []struct {
		name string
		c    *change.Change
		want bool
	}{
		{"open to paid", full(change.Update, "open", "paid"), true},
		{"paid to paid", full(change.Update, "paid", "paid"), false},
		{"paid to open", full(change.Update, "paid", "open"), false},
		{"inserted paid", full(change.Insert, "", "paid"), true},
		{"inserted open", full(change.Insert, "", "open"), false},
		{"deleted open", full(change.Delete, "open", ""), true},
		{"old row unknown", keyOnly, false},
		{"truncate", &change.Change{Operation: change.Truncate}, false},
	}
	for _, c := range cases {
		if got := paid.Holds(c.c); got != c.want {
			t.Errorf("%s: holds %v, want %v", c.name, got, c.want)
		}
	}
	if !(Transition{}).Holds(keyOnly) {
		t.Error("a transition without conditions refused a change")
	}
}
