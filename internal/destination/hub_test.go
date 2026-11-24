package destination

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/event"
)

// started waits until the destination stored where it starts.
func started(t *testing.T, store *memStore, name string) {
	t.Helper()
	eventually(t, name+" to start", func() bool { _, ok := store.state(name); return ok })
}

func delivered(rc *receiver, n int) func() bool {
	return func() bool { return len(rc.requests()) >= n }
}

func TestDeliversSignedBatchesFromItsCursor(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	order(f, 110, "0", "", "open") // before the destination exists
	startHub(t, f, store, setToken(t, now.Unix(), hook(rc.server.URL)), "")
	started(t, store, "orders-hook")

	order(f, 120, "1", "", "open")
	eventually(t, "the first batch", delivered(rc, 1))
	order(f, 130, "2", "", "open")
	order(f, 140, "3", "", "open")
	eventually(t, "the cursor past 140", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 140 })

	var keys []any
	for _, r := range rc.requests() {
		for _, e := range r.events {
			if e.Type != event.TransactionType || e.Source != issuer {
				t.Fatalf("event %+v", e)
			}
			keys = append(keys, e.Data.Changes[0].Key["id"])
		}
	}
	if len(keys) != 3 || keys[0] != "1" || keys[1] != "2" || keys[2] != "3" {
		t.Fatalf("delivered %v, want orders 1, 2 and 3 in order", keys)
	}
}

func TestRaisesNamedEvents(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	paid := hook(rc.server.URL)
	paid["subs"] = nil
	paid["events"] = []map[string]any{{"type": "orders.paid", "resource": "orders",
		"old": map[string]any{"neq": []any{"status", "paid"}}, "new": map[string]any{"eq": []any{"status", "paid"}}}}
	startHub(t, f, store, setToken(t, now.Unix(), paid), "")
	started(t, store, "orders-hook")

	order(f, 120, "1", "", "open")
	order(f, 130, "1", "open", "paid")
	order(f, 140, "1", "paid", "paid")
	eventually(t, "the cursor past 130", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN >= 130 })
	got := rc.requests()
	if len(got) != 1 || len(got[0].events) != 1 || got[0].events[0].Type != "orders.paid" ||
		got[0].events[0].Data.Changes[0].OldRow["status"] != "open" {
		t.Fatalf("got %+v, want one orders.paid with the old row", got)
	}
}

func TestRetriesTheSameBatchWithoutHoldingBackOthers(t *testing.T) {
	f := newFeed(t, 100)
	var failures atomic.Int32
	failing := newReceiver(t, func(received) (int, string) {
		if failures.Add(1) <= 2 {
			return http.StatusServiceUnavailable, ""
		}
		return http.StatusOK, ""
	})
	healthy := newReceiver(t, ok)
	other := hook(healthy.server.URL)
	other["name"] = "audit"
	store := newMemStore()
	startHub(t, f, store, setToken(t, now.Unix(), hook(failing.server.URL), other), "")
	started(t, store, "orders-hook")
	started(t, store, "audit")

	order(f, 120, "1", "", "open")
	eventually(t, "the healthy receiver", delivered(healthy, 1))
	eventually(t, "the third try", delivered(failing, 3))
	eventually(t, "the cursor past 120", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 120 && s.Batch == nil })

	tries := failing.requests()
	if tries[0].id == "" || tries[1].id != tries[0].id || tries[2].id != tries[0].id || tries[2].body != tries[0].body {
		t.Fatalf("retries changed the batch: %+v", tries)
	}
}

func TestPausesAtTheLimitAndResumesTheSameBatch(t *testing.T) {
	f := newFeed(t, 100)
	var up atomic.Bool
	rc := newReceiver(t, func(received) (int, string) {
		if up.Load() {
			return http.StatusOK, ""
		}
		return http.StatusInternalServerError, ""
	})
	d := hook(rc.server.URL)
	d["retry"] = map[string]any{"tries": 2, "at_limit": "pause"}
	store := newMemStore()
	h := startHub(t, f, store, setToken(t, now.Unix(), d), "")
	started(t, store, "orders-hook")

	order(f, 120, "1", "", "open")
	eventually(t, "the pause", func() bool { s, _ := store.state("orders-hook"); return s.Status == Paused })
	order(f, 130, "2", "", "open")
	s, _ := store.state("orders-hook")
	if s.Position.LSN == 120 || s.Batch == nil || s.Batch.Tried != 2 || s.Batch.Error != "the receiver answered 500" {
		t.Fatalf("paused state %+v %+v", s, s.Batch)
	}

	up.Store(true)
	if err := h.Resume("orders-hook"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the cursor past 130", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 130 })
	got := rc.requests()
	if len(got) != 4 || got[2].id != got[0].id || got[2].body != got[0].body || got[3].events[0].Data.Changes[0].Key["id"] != "2" {
		t.Fatalf("after resume %+v", got)
	}
	if err := h.Resume("nobody"); !errors.Is(err, ErrUnknown) {
		t.Fatalf("resumed an unknown destination: %v", err)
	}
}

func TestDeadLettersAtTheLimit(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, func(r received) (int, string) {
		if r.events[0].Data.Changes[0].Key["id"] == "1" {
			return http.StatusBadRequest, ""
		}
		return http.StatusOK, ""
	})
	d := hook(rc.server.URL)
	d["retry"] = map[string]any{"tries": 1, "at_limit": "dead_letter"}
	store := newMemStore()
	startHub(t, f, store, setToken(t, now.Unix(), d), "")
	started(t, store, "orders-hook")

	order(f, 120, "1", "", "open")
	order(f, 130, "2", "", "open")
	eventually(t, "the cursor past 130", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 130 })
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.letters) != 1 || store.letters[0].Tried != 1 || store.letters[0].Error != "the receiver answered 400" {
		t.Fatalf("letters %+v", store.letters)
	}
}

func TestFunctionDropsOrForwardsBatches(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	fn := newReceiver(t, func(r received) (int, string) {
		if r.events[0].Data.Changes[0].Key["id"] == "1" {
			return http.StatusOK, "[]"
		}
		return http.StatusOK, `[{"specversion":"1.0","id":"x","source":"shop","type":"orders.flagged"}]`
	})
	d := hook(rc.server.URL)
	d["function"] = "route-orders"
	store := newMemStore()
	startHub(t, f, store, setToken(t, now.Unix(), d), fn.server.URL+"/api/functions")
	started(t, store, "orders-hook")

	order(f, 120, "1", "", "open")
	eventually(t, "the cursor past 120", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 120 })
	order(f, 130, "2", "", "open")
	eventually(t, "the forwarded batch", delivered(rc, 1))
	got := rc.requests()
	if len(got) != 1 || got[0].events[0].Type != "orders.flagged" || got[0].id != fn.requests()[1].id {
		t.Fatalf("forwarded %+v", got)
	}
}

func TestRestartResendsTheFailedBatch(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	raw := setToken(t, now.Unix(), hook(rc.server.URL))
	store.PutDefinitions(context.Background(), "shop", raw)
	// A batch of the transaction at 120 failed before the restart; 130 came after it.
	store.PutState(context.Background(), "shop", "orders-hook", State{Position: positionAt(100), Status: Active,
		Batch: &Batch{End: 120, ID: "old", Tried: 1}})
	order(f, 120, "1", "", "open")
	order(f, 130, "2", "", "open")
	startHub(t, f, store, "", "")

	eventually(t, "the cursor past 130", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 130 })
	got := rc.requests()
	if len(got) != 2 || len(got[0].events) != 1 || got[0].events[0].Data.Changes[0].Key["id"] != "1" {
		t.Fatalf("after restart %+v, want the failed batch alone first", got)
	}
}

func TestPutKeepsCursorsAndRefusesOlderSets(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	h := startHub(t, f, store, setToken(t, now.Unix(), hook(rc.server.URL)), "")
	started(t, store, "orders-hook")
	order(f, 120, "1", "", "open")
	eventually(t, "the cursor past 120", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 120 })

	ctx := context.Background()
	if err := h.Put(ctx, setToken(t, now.Unix()-1)); !errors.Is(err, ErrStale) {
		t.Fatalf("put an older set: %v", err)
	}
	changed := hook(rc.server.URL)
	changed["batch_size"] = 5
	if err := h.Put(ctx, setToken(t, now.Unix()+1, changed)); err != nil {
		t.Fatal(err)
	}
	order(f, 130, "2", "", "open")
	eventually(t, "the cursor past 130", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 130 })
	if got := rc.requests(); len(got) != 2 {
		t.Fatalf("a changed definition resent or skipped: %+v", got)
	}

	if err := h.Put(ctx, setToken(t, now.Unix()+2)); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.state("orders-hook"); ok {
		t.Fatal("a removed destination kept its state")
	}
}

func TestQuietDestinationWritesNothing(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	d := hook(rc.server.URL)
	d["subs"] = []map[string]any{{"resource": "invoices"}}
	store := newMemStore()
	startHub(t, f, store, setToken(t, now.Unix(), d), "")
	started(t, store, "orders-hook")
	for i := range 20 {
		order(f, pglogrepl.LSN(120+i), "1", "", "open")
	}
	f.Append(&change.Transaction{CommitLSN: 200, EndLSN: 208, Changes: []change.Change{{
		Table: change.Table{Schema: "public", Name: "invoices"}, Operation: change.Insert,
		Key: change.Row{"id": {Kind: change.Text, Text: "9"}}, NewRow: change.Row{"id": {Kind: change.Text, Text: "9"}}}}})
	eventually(t, "the cursor past 200", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 200 })
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.writes != 2 {
		t.Fatalf("wrote %d times, want once at the start and once for the one batch", store.writes)
	}
}

func TestFallsBehindTheFeed(t *testing.T) {
	f := newFeed(t, 2)
	rc := newReceiver(t, ok)
	store := newMemStore()
	store.PutDefinitions(context.Background(), "shop", setToken(t, now.Unix(), hook(rc.server.URL)))
	store.PutState(context.Background(), "shop", "orders-hook", State{Position: positionAt(100), Status: Active})
	for i := range 3 {
		order(f, pglogrepl.LSN(120+i), "1", "", "open")
	}
	h := startHub(t, f, store, "", "")
	eventually(t, "the destination to be gone", func() bool { s, _ := store.state("orders-hook"); return s.Status == Gone })
	if err := h.Resume("orders-hook"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a restart from the newest change", func() bool { s, _ := store.state("orders-hook"); return s.Status == Active })
	order(f, 140, "2", "", "open")
	eventually(t, "the next delivery", delivered(rc, 1))
}
