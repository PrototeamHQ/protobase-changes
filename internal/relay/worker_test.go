package relay

import (
	"fmt"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/event"
)

func expect(t *testing.T, sent []any, want ...string) {
	t.Helper()
	if got := summary(sent); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
}

func TestRegisterFromASnapshotGetsWhatItDidNotSee(t *testing.T) {
	f := newFeed(t, 10)
	// 11 committed before the snapshot, 12 was in progress at it and 13 began after it.
	insert(f, 11, 110)
	insert(f, 12, 120)
	r := newRelay(t, f)
	r.post(register("r1", map[string]any{"snapshot": "12:13:12", "fields": []string{"body"}}))
	sent := r.step()
	expect(t, sent, "r1:12")

	e := sent[0].(Event)
	if e.Type != event.TransactionType || e.App != "shop" || e.Source != "https://shop.example" || e.SpecVersion != "1.0" {
		t.Fatalf("event %+v", e)
	}
	entry := e.Data.Registrations[0]
	if held, _ := f.Since(110); entry.Cursor != held[0].Cursor || e.ID != held[0].ID {
		t.Fatalf("entry %+v, want the transaction's cursor", entry)
	}
	if c := entry.Changes[0]; c.NewRow["body"] != "b12" || c.NewRow["id"] != nil || c.Subscriptions[0].ID != 0 {
		t.Fatalf("change %+v, want the body field alone", c)
	}
	insert(f, 13, 130)
	expect(t, r.step(), "r1:13")
}

func TestRegisterFromACursorResumesAfterIt(t *testing.T) {
	f := newFeed(t, 10)
	insert(f, 11, 110)
	insert(f, 12, 120)
	r := newRelay(t, f)
	r.post(register("r1", map[string]any{"cursor": seal(f, 110)}))
	sent := r.step()
	expect(t, sent, "r1:12")
	if c := sent[0].(Event).Data.Registrations[0].Changes[0]; c.NewRow != nil {
		t.Fatalf("change %+v, want no row without fields", c)
	}
}

func TestAStartTheTailNoLongerHoldsGetsGap(t *testing.T) {
	f := newFeed(t, 2)
	for i := range 3 {
		insert(f, uint32(11+i), pglogrepl.LSN(110+10*i))
	}
	r := newRelay(t, f)
	r.post(register("cursor", map[string]any{"cursor": seal(f, 100)}))
	// The snapshot missed transaction 11, which the tail dropped.
	r.post(register("snapshot", map[string]any{"snapshot": "11:12:11"}))
	// This one saw 11 and gets the rest.
	r.post(register("seen", map[string]any{"snapshot": "12:13:12"}))
	expect(t, r.step(), "gap cursor", "gap snapshot", "seen:12", "seen:13")
}

func TestFallingBehindTheTailGetsGap(t *testing.T) {
	f := newFeed(t, 2)
	r := newRelay(t, f)
	r.post(register("r1", map[string]any{"cursor": seal(f, 100)}))
	r.post(register("r2", map[string]any{"cursor": seal(f, 100)}))
	insert(f, 11, 110)
	expect(t, r.step(), "r1:11", "r2:11")
	r.post(map[string]any{"type": "unregister", "app": "shop", "id": "r1"})
	for i := range 2 {
		insert(f, uint32(12+i), pglogrepl.LSN(120+10*i))
	}
	expect(t, r.step(), "r2:12", "r2:13")
	r.post(register("r3", map[string]any{"cursor": seal(f, 110)}))
	expect(t, r.step(), "r3:12", "r3:13")
	for i := range 3 {
		insert(f, uint32(14+i), pglogrepl.LSN(140+10*i))
	}
	expect(t, r.step(), "gap r2", "gap r3")
}

func TestRevocationEndsTheRegistration(t *testing.T) {
	f := newFeed(t, 10)
	r := newRelay(t, f)
	r.post(register("r1", map[string]any{"cursor": seal(f, 100)}))
	r.post(register("visitor", map[string]any{"cursor": seal(f, 100), "sid": "", "anonymous": true}))
	insert(f, 11, 110)
	expect(t, r.step(), "r1:11", "visitor:11")
	// The transaction that revoked it goes only to the others.
	insert(f, 12, 120, revocation("s9"))
	expect(t, r.step(), "revoked r1", "visitor:12")
	// A register for a revoked session ends at once.
	r.post(register("r2", map[string]any{"cursor": seal(f, 120)}))
	expect(t, r.step(), "revoked r2")
}

func TestUnregisterStopsItsChanges(t *testing.T) {
	f := newFeed(t, 10)
	r := newRelay(t, f)
	r.post(register("r1", map[string]any{"cursor": seal(f, 100)}))
	r.post(register("r2", map[string]any{"cursor": seal(f, 100)}))
	r.post(map[string]any{"type": "unregister", "app": "shop", "id": "r1"})
	insert(f, 11, 110)
	expect(t, r.step(), "r2:11")
	// Its id can be registered again.
	r.post(register("r1", map[string]any{"cursor": seal(f, 110)}))
	insert(f, 12, 120)
	expect(t, r.step(), "r1:12", "r2:12")
}

func TestRegistersWaitForTheFeed(t *testing.T) {
	f := newFeed(t, 10)
	f.Lost()
	r := newRelay(t, f)
	r.post(register("r1", map[string]any{"cursor": seal(f, 100)}))
	expect(t, r.step())
	if _, err := f.Resume(epoch, 100, 100); err != nil {
		t.Fatal(err)
	}
	insert(f, 11, 110)
	expect(t, r.step(), "r1:11")
}

func TestRefusedRegisters(t *testing.T) {
	f := newFeed(t, 10)
	r := newRelay(t, f)
	r.post(register("ok", map[string]any{"cursor": seal(f, 100)}))
	r.step()
	cases := map[string]map[string]any{
		"taken id":       register("ok", map[string]any{"cursor": seal(f, 100)}),
		"no subs":        register("x", map[string]any{"cursor": seal(f, 100), "subs": []map[string]any{}}),
		"bad conditions": register("x", map[string]any{"cursor": seal(f, 100), "subs": []map[string]any{{"resource": "messages", "where": map[string]any{"nope": 1}}}}),
		"no resource":    register("x", map[string]any{"cursor": seal(f, 100), "subs": []map[string]any{{"where": nil}}}),
		"sid and anon":   register("x", map[string]any{"cursor": seal(f, 100), "anonymous": true}),
		"no session":     register("x", map[string]any{"cursor": seal(f, 100), "sid": ""}),
		"both starts":    register("x", map[string]any{"cursor": seal(f, 100), "snapshot": "11:11:"}),
		"no start":       register("x", nil),
		"bad snapshot":   register("x", map[string]any{"snapshot": "11:10:"}),
		"bad cursor":     register("x", map[string]any{"cursor": "abc"}),
	}
	for name, m := range cases {
		r.post(m)
		sent := r.step()
		if n, ok := sent[0].(Notice); len(sent) != 1 || !ok || n.Type != RefusedType || n.ID != m["id"] || n.App != "shop" || n.Reason == "" {
			t.Errorf("%s: sent %+v, want refused", name, sent)
		}
	}
	insert(f, 11, 110)
	expect(t, r.step(), "ok:11")
}
