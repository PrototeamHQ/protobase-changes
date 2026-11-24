// Package storetest checks a destination store against the same cases in
// every database it runs on.
package storetest

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
)

// Store is a destination store that also reads its dead letters back.
type Store interface {
	destination.Store
	Letters(ctx context.Context, app, name string) ([]destination.Letter, error)
}

// RoundTrip checks that a store keeps what it is given, per app.
func RoundTrip(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	if raw, err := s.Definitions(ctx, "shop"); err != nil || raw != "" {
		t.Fatalf("definitions before any: %q %v", raw, err)
	}
	for _, raw := range []string{"first", "second"} {
		if err := s.PutDefinitions(ctx, "shop", raw); err != nil {
			t.Fatal(err)
		}
	}
	if raw, err := s.Definitions(ctx, "shop"); err != nil || raw != "second" {
		t.Fatalf("definitions %q %v, want the last put", raw, err)
	}

	epoch := cursor.Epoch{SystemID: 1<<63 + 5, Timeline: 2}
	active := destination.State{Position: cursor.Position{Epoch: epoch, LSN: 0x1A2B3C}, Status: destination.Active}
	failing := destination.State{Position: cursor.Position{Epoch: epoch, LSN: 0x10}, Status: destination.Active,
		Batch: &destination.Batch{End: 0x20, ID: "b1", Tried: 3, Next: time.UnixMilli(1795480000123), Error: "the receiver answered 503"}}
	paused := destination.State{Position: cursor.Position{Epoch: epoch, LSN: 0x10}, Status: destination.Paused,
		Batch: &destination.Batch{End: 0x20, ID: "b1", Tried: 10, Error: "the receiver answered 500"}}
	for name, st := range map[string]destination.State{"hook": failing, "other": active} {
		if err := s.PutState(ctx, "shop", name, st); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PutState(ctx, "shop", "hook", paused); err != nil {
		t.Fatal(err)
	}
	if err := s.PutState(ctx, "blog", "hook", active); err != nil {
		t.Fatal(err)
	}
	states, err := s.States(ctx, "shop")
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]destination.State{"hook": paused, "other": active}; !reflect.DeepEqual(states, want) {
		t.Fatalf("states %+v, want %+v", states, want)
	}
	if err := s.PutState(ctx, "shop", "hook", failing); err != nil {
		t.Fatal(err)
	}
	if states, _ := s.States(ctx, "shop"); !reflect.DeepEqual(states["hook"], failing) {
		t.Fatalf("failing state %+v, want %+v", states["hook"].Batch, failing.Batch)
	}
	if err := s.DeleteState(ctx, "shop", "other"); err != nil {
		t.Fatal(err)
	}
	if states, _ := s.States(ctx, "shop"); len(states) != 1 {
		t.Fatalf("states after delete %+v", states)
	}

	letter := destination.Letter{BatchID: "b1", Events: []byte(`[{"id":"e1"}]`), Error: "no answer within 30s", Tried: 10,
		At: time.UnixMilli(1795480000456)}
	for range 2 {
		if err := s.PutLetter(ctx, "shop", "hook", letter); err != nil {
			t.Fatal(err)
		}
	}
	letters, err := s.Letters(ctx, "shop", "hook")
	if err != nil || len(letters) != 1 || !reflect.DeepEqual(letters[0], letter) {
		t.Fatalf("letters %+v %v, want the one put twice", letters, err)
	}
}
