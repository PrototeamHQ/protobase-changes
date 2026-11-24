package destination

import (
	"net/http"
	"testing"
)

func TestStoresTheAdvancedHeadOnceDoneWithEveryTransaction(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	h := startHub(t, f, store, setToken(t, now.Unix(), hook(rc.server.URL)), "")
	started(t, store, "orders-hook")
	order(f, 120, "1", "", "open")
	eventually(t, "the cursor at 120", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 120 })
	if lsn, held := h.Stored(); !held || lsn != 120 {
		t.Fatalf("stored %v %v, want 120", lsn, held)
	}

	f.Advance(900)
	eventually(t, "the cursor at the advanced head", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 900 })
	eventually(t, "the hub to report it", func() bool { lsn, _ := h.Stored(); return lsn == 900 })

	order(f, 950, "2", "", "open")
	eventually(t, "the next batch", delivered(rc, 2))
}

func TestAPausedDestinationHoldsBackNoConfirm(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, func(received) (int, string) { return http.StatusInternalServerError, "" })
	d := hook(rc.server.URL)
	d["retry"] = map[string]any{"tries": 1, "at_limit": "pause"}
	store := newMemStore()
	h := startHub(t, f, store, setToken(t, now.Unix(), d), "")
	started(t, store, "orders-hook")
	order(f, 120, "1", "", "open")
	eventually(t, "the pause", func() bool { s, _ := store.state("orders-hook"); return s.Status == Paused })
	if lsn, held := h.Stored(); held {
		t.Fatalf("a paused destination holds the slot at %v", lsn)
	}
}

func TestAStartingDestinationHoldsAtZero(t *testing.T) {
	f := newFeed(t, 100)
	f.Lost()
	rc := newReceiver(t, ok)
	h := startHub(t, f, newMemStore(), setToken(t, now.Unix(), hook(rc.server.URL)), "")
	eventually(t, "the destination to run", func() bool { _, held := h.Stored(); return held })
	if lsn, _ := h.Stored(); lsn != 0 {
		t.Fatalf("stored %v before the destination saved, want 0", lsn)
	}
}
