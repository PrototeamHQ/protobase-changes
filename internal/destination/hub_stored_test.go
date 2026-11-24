package destination

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestStoresTheAdvancedHeadOnceDoneWithEveryTransaction(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	h := startHub(t, f, store, setToken(t, now.Unix(), hook(rc.server.URL)), "")
	started(t, store, "orders-hook")
	order(f, 120, "1", "", "open")
	eventually(t, "the cursor at 120", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 120 })
	eventually(t, "the hub to report 120", func() bool { lsn, held := h.Stored(); return held && lsn == 120 })

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
	eventually(t, "the paused destination to hold nothing", func() bool { _, held := h.Stored(); return !held })
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

func TestAHubNotLoadedHoldsAtZero(t *testing.T) {
	h := NewHub(newFeed(t, 100), newMemStore(), &Sender{Client: http.DefaultClient, Now: time.Now}, quiet)
	if lsn, held := h.Stored(); !held || lsn != 0 {
		t.Fatalf("stored %v %v before the hub loaded, want 0", lsn, held)
	}
}

// Between runs of the hub, such as while its store is down, a destination
// waits in the hub's states and still holds back the confirm.
func TestAWaitingDestinationHoldsAtItsStoredCursor(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	h := NewHub(f, store, &Sender{Client: http.DefaultClient, Now: time.Now}, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	eventually(t, "the hub to load", h.isLoaded)
	if err := h.Put(ctx, setToken(t, now.Unix(), hook(rc.server.URL))); err != nil {
		t.Fatal(err)
	}
	started(t, store, "orders-hook")
	order(f, 120, "1", "", "open")
	eventually(t, "the cursor at 120", func() bool { s, _ := store.state("orders-hook"); return s.Position.LSN == 120 })
	cancel()
	<-done
	if lsn, held := h.Stored(); !held || lsn != 120 {
		t.Fatalf("stored %v %v between runs, want 120", lsn, held)
	}
}
