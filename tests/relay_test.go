//go:build integration

package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func expectBodies(t *testing.T, m received, want ...string) {
	t.Helper()
	if m.Type != "protobase.changes.transaction" || fmt.Sprint(m.bodies()) != fmt.Sprint(want) {
		t.Fatalf("got %+v, want %v", m, want)
	}
}

func expectNotice(t *testing.T, m received, typ, id string) {
	t.Helper()
	if m.Type != typ || m.ID != id || m.App != "shop" {
		t.Fatalf("got %+v, want %s for %s", m, typ, id)
	}
}

// TestRelayRegisterFromASnapshot registers a page's live query from the
// snapshot it read: it gets the transaction in progress at the snapshot and
// those after it, never one the snapshot saw.
func TestRelayRegisterFromASnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, table, _, relay := relayApp(t, ctx, 100)
	open := connect(t, ctx, databaseURL(t))
	tx, err := open.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, ctx, tx, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'in progress')", table))
	// A later transaction that commits first puts the open one below the
	// snapshot's xmax, in its list of those in progress.
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'seen')", table))
	read := snapshot(t, ctx, db)
	if strings.HasSuffix(read, ":") {
		t.Fatalf("snapshot %s lists no transaction in progress", read)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (3, 'after')", table))

	relay.send(ctx, registerMsg("r1", table, map[string]any{"snapshot": read}))
	expectBodies(t, relay.receive(ctx), "r1:in progress")
	expectBodies(t, relay.receive(ctx), "r1:after")
	relay.send(ctx, map[string]any{"type": "unregister", "app": "shop", "id": "r1"})
	relay.save("register-snapshot")
}

// TestRelayRegisterFromACursor registers again from the cursor of an event,
// as a relay does after it reconnects: the changes after it come again.
func TestRelayRegisterFromACursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, table, _, relay := relayApp(t, ctx, 100)
	relay.send(ctx, registerMsg("r1", table, map[string]any{"snapshot": snapshot(t, ctx, db)}))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'one')", table))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'two')", table))
	first := relay.receive(ctx)
	expectBodies(t, first, "r1:one")
	expectBodies(t, relay.receive(ctx), "r1:two")
	relay.send(ctx, map[string]any{"type": "unregister", "app": "shop", "id": "r1"})

	relay.send(ctx, registerMsg("r2", table, map[string]any{"cursor": first.Data.Registrations[0].Cursor, "sid": "s-r1"}))
	expectBodies(t, relay.receive(ctx), "r2:two")
	relay.send(ctx, map[string]any{"type": "unregister", "app": "shop", "id": "r2"})
	relay.save("register-cursor")
}

// TestRelayGap registers from a snapshot that missed a transaction the
// tail no longer holds: the page reads again.
func TestRelayGap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, table, f, relay := relayApp(t, ctx, 2)
	read := snapshot(t, ctx, db)
	floor := f.Floor()
	for i := range 3 {
		exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (%d, 'missed')", table, i))
	}
	waitFor(t, ctx, f, func() bool { return f.Floor() > floor })
	relay.send(ctx, registerMsg("r1", table, map[string]any{"snapshot": read}))
	expectNotice(t, relay.receive(ctx), "gap", "r1")
	relay.save("gap")
}

// TestRelayRevoked ends a registration when the app revokes its session.
func TestRelayRevoked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, table, _, relay := relayApp(t, ctx, 100)
	relay.send(ctx, registerMsg("r1", table, map[string]any{"snapshot": snapshot(t, ctx, db)}))
	relay.send(ctx, registerMsg("visitor", table, map[string]any{"snapshot": snapshot(t, ctx, db), "sid": "", "anonymous": true}))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'before')", table))
	expectBodies(t, relay.receive(ctx), "r1:before", "visitor:before")
	exec(t, ctx, db, revocation(t, db, "s-r1"))
	expectNotice(t, relay.receive(ctx), "revoked", "r1")
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'after')", table))
	expectBodies(t, relay.receive(ctx), "visitor:after")
	relay.save("revoked")
}

// TestRelayUnregister stops one registration's changes and keeps the others'.
func TestRelayUnregister(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, table, _, relay := relayApp(t, ctx, 100)
	relay.send(ctx, registerMsg("r1", table, map[string]any{"snapshot": snapshot(t, ctx, db)}))
	relay.send(ctx, registerMsg("r2", table, map[string]any{"snapshot": snapshot(t, ctx, db)}))
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (1, 'both')", table))
	expectBodies(t, relay.receive(ctx), "r1:both", "r2:both")
	relay.send(ctx, map[string]any{"type": "unregister", "app": "shop", "id": "r1"})
	// A register of a taken id is refused after the unregister is applied,
	// so the change below comes after it.
	relay.send(ctx, registerMsg("r2", table, map[string]any{"snapshot": snapshot(t, ctx, db)}))
	expectNotice(t, relay.receive(ctx), "refused", "r2")
	exec(t, ctx, db, fmt.Sprintf("INSERT INTO %s (id, body) VALUES (2, 'one')", table))
	expectBodies(t, relay.receive(ctx), "r2:one")
	relay.save("unregister")
}
