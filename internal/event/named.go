package event

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

// Types names the event types a change of a resource raises, such as
// orders.paid for an order whose status turned paid.
type Types func(resource string, c *change.Change) []string

// Named makes one event per change of entry's transaction and per type that
// types names for it, in commit order. Each carries its one change, shaped
// by payload, and an id that stays the same on every replay. A raw payload
// states both rows: the app chose the event for its own use, so no
// subscription's access hides either.
func Named(source string, entry *feed.Entry, payload token.Payload, types Types) []Event {
	tx := entry.Tx
	var actorOf []json.RawMessage
	if payload == token.Raw {
		actorOf = actors(tx)
	}
	var out []Event
	for i := range tx.Changes {
		c := &tx.Changes[i]
		resource, ok := Resource(c.Table)
		if !ok {
			continue
		}
		for _, typ := range types(resource, c) {
			data := Change{Resource: resource, Operation: c.Operation, Key: values(c.Key)}
			if payload == token.Raw {
				data.OldRow, data.NewRow, data.Actor = values(c.OldRow), values(c.NewRow), actorOf[i]
			}
			out = append(out, Event{
				SpecVersion:     "1.0",
				ID:              namedID(entry.ID, i, typ),
				Source:          source,
				Type:            typ,
				Time:            tx.CommitTime.UTC().Format(time.RFC3339Nano),
				DataContentType: "application/json",
				Cursor:          entry.Cursor,
				Data:            &Data{Changes: []Change{data}},
			})
		}
	}
	return out
}

// namedID derives an event's id from its transaction's, the change's place
// in it and the type, so it reveals nothing more than the transaction id does.
func namedID(txID string, i int, typ string) string {
	sum := sha256.Sum256([]byte(txID + "\x00" + strconv.Itoa(i) + "\x00" + typ))
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}
