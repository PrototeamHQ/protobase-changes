package event

import (
	"reflect"
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/match"
)

func TestVisibleShowsOnlyRowsInASubscription(t *testing.T) {
	text := func(s string) change.Row { return change.Row{"id": {Kind: change.Text, Text: s}} }
	full := &change.Change{Operation: change.Update, Key: text("k"), OldRowKind: change.OldFull, OldRow: text("old"), NewRow: text("new")}
	rekeyed := &change.Change{Operation: change.Update, Key: text("new"), OldRowKind: change.OldKeyOnly, OldRow: text("old"), NewRow: text("new")}
	cases := map[string]struct {
		c                   *change.Change
		tags                []match.Tag
		key, oldRow, newRow change.Row
	}{
		"enter":        {full, []match.Tag{{ID: 0, Effect: match.Enter}}, text("k"), nil, text("new")},
		"update":       {full, []match.Tag{{ID: 0, Effect: match.Update}}, text("k"), text("old"), text("new")},
		"leave":        {full, []match.Tag{{ID: 0, Effect: match.Leave}}, text("k"), text("old"), nil},
		"enter, leave": {full, []match.Tag{{ID: 0, Effect: match.Enter}, {ID: 1, Effect: match.Leave}}, text("k"), text("old"), text("new")},
		"leave by key": {rekeyed, []match.Tag{{ID: 0, Effect: match.Leave}}, text("old"), text("old"), nil},
		"enter by key": {rekeyed, []match.Tag{{ID: 0, Effect: match.Enter}}, text("new"), nil, text("new")},
	}
	for name, tc := range cases {
		key, oldRow, newRow := visible(tc.c, tc.tags)
		if !reflect.DeepEqual(key, tc.key) || !reflect.DeepEqual(oldRow, tc.oldRow) || !reflect.DeepEqual(newRow, tc.newRow) {
			t.Errorf("%s: %v %v %v", name, key, oldRow, newRow)
		}
	}
}
