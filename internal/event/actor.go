package event

import (
	"encoding/json"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// ActorPrefix is the logical message prefix the app names who made the
// changes that follow it with.
const ActorPrefix = "protobase_actor"

// actors returns, for each change of tx, the actor of the last actor message
// before it, or nil when none came before it or it is not JSON.
func actors(tx *change.Transaction) []json.RawMessage {
	out := make([]json.RawMessage, len(tx.Changes))
	var current json.RawMessage
	next := 0
	for i := range tx.Changes {
		for ; next < len(tx.Messages) && tx.Messages[next].Position <= i; next++ {
			m := tx.Messages[next]
			if m.Prefix != ActorPrefix {
				continue
			}
			current = nil
			if json.Valid(m.Content) {
				current = m.Content
			}
		}
		out[i] = current
	}
	return out
}
