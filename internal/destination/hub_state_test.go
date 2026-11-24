package destination

import (
	"context"
	"testing"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
)

func TestKeepsStatesOfASetThatNoLongerOpens(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	// The kept set was sealed under a key the app has since retired.
	other := make([]byte, len(key))
	encrypter, _ := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.DIRECT, Key: other}, nil)
	retired, _ := jwt.Encrypted(encrypter).Claims(map[string]any{"iss": issuer, "iat": now.Unix(), "destinations": []any{}}).Serialize()
	store.PutDefinitions(context.Background(), "shop", retired)
	store.PutState(context.Background(), "shop", "orders-hook", State{Position: positionAt(100), Status: Active})
	order(f, 120, "1", "", "open")

	h := startHub(t, f, store, "", "")
	if err := h.Put(context.Background(), setToken(t, now.Unix(), hook(rc.server.URL))); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the change after the kept cursor", delivered(rc, 1))
}

func TestPutAgainFinishesDeletes(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	h := startHub(t, f, store, setToken(t, now.Unix(), hook(rc.server.URL)), "")
	started(t, store, "orders-hook")

	store.mu.Lock()
	store.failDeletes = 1
	store.mu.Unlock()
	empty := setToken(t, now.Unix()+1)
	if err := h.Put(context.Background(), empty); err == nil {
		t.Fatal("a failed delete went unreported")
	}
	if _, ok := store.state("orders-hook"); !ok {
		t.Fatal("the state went although its delete failed")
	}
	if err := h.Put(context.Background(), empty); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.state("orders-hook"); ok {
		t.Fatal("putting the set again left the removed state")
	}
}

func TestPausedOfAnotherEpochIsGone(t *testing.T) {
	f := newFeed(t, 100)
	rc := newReceiver(t, ok)
	store := newMemStore()
	store.PutDefinitions(context.Background(), "shop", setToken(t, now.Unix(), hook(rc.server.URL)))
	store.PutState(context.Background(), "shop", "orders-hook", State{
		Position: cursor.Position{Epoch: cursor.Epoch{SystemID: 1, Timeline: 2}, LSN: 100}, Status: Paused,
		Batch: &Batch{End: 120, ID: "old", Tried: 10}})
	startHub(t, f, store, "", "")
	eventually(t, "the destination to be gone", func() bool { s, _ := store.state("orders-hook"); return s.Status == Gone })
}

func TestSavesTheCursorPastHalfTheFeed(t *testing.T) {
	f := newFeed(t, 10)
	rc := newReceiver(t, ok)
	d := hook(rc.server.URL)
	d["subs"] = []map[string]any{{"resource": "invoices"}}
	store := newMemStore()
	startHub(t, f, store, setToken(t, now.Unix(), d), "")
	started(t, store, "orders-hook")
	for i := range 6 {
		order(f, positionAt(120).LSN+10*pglogrepl.LSN(i), "1", "", "open")
	}
	eventually(t, "the cursor saved past five transactions", func() bool {
		s, _ := store.state("orders-hook")
		return s.Position.LSN >= 160
	})
}
