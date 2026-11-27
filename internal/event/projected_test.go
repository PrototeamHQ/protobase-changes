package event

import (
	"reflect"
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/match"
)

func TestProjectedKeepsOnlyTheFields(t *testing.T) {
	text := func(s string) change.Value { return change.Value{Kind: change.Text, Text: s} }
	tx := &change.Transaction{Changes: []change.Change{
		{Table: change.Table{Schema: "public", Name: "messages"}, Operation: change.Insert, Key: change.Row{"id": text("1")},
			NewRow: change.Row{"id": text("1"), "body": text("hi"), "secret": text("x")}},
		{Table: change.Table{Schema: "public", Name: "other"}, Operation: change.Insert, Key: change.Row{"id": text("2")}},
	}}
	subs := match.NewIndex([]match.Subscription{{ID: 0, Resource: "messages"}})

	got := Projected(tx, subs, []string{"body", "missing"})
	want := []Change{{Resource: "messages", Operation: change.Insert, Key: map[string]any{"id": "1"},
		NewRow: map[string]any{"body": "hi"}, Subscriptions: []match.Tag{{ID: 0, Effect: match.Enter}}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if thin := Projected(tx, subs, nil); len(thin) != 1 || thin[0].NewRow != nil || thin[0].Key["id"] != "1" {
		t.Fatalf("without fields %+v, want the key alone", thin)
	}
}
