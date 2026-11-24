package destination

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

// gathered is what a destination found in the feed after its cursor.
type gathered struct {
	events []event.Event
	// end is the commit of the last transaction looked at, events or not;
	// zero when there was none.
	end pglogrepl.LSN
}

// gather looks at the entries in order and takes the destination's events
// of whole transactions: up to its batch size, or, when until is set, of
// every transaction up to the one that committed at until, as a retried
// batch had them.
func gather(source string, d *Definition, entries []*feed.Entry, until pglogrepl.LSN) gathered {
	tok := token.Token{Payload: d.Payload, Subscriptions: d.Subscriptions}
	var g gathered
	for _, entry := range entries {
		if until != 0 && entry.Commit() > until {
			break
		}
		if until == 0 && len(g.events) >= d.BatchSize {
			break
		}
		if e, ok := event.Transaction(source, entry, tok); ok {
			g.events = append(g.events, e)
		}
		g.events = append(g.events, event.Named(source, entry, d.Payload, d.types)...)
		g.end = entry.Commit()
	}
	return g
}

// batchID is a batch's webhook-id: the same on every try of the same
// events, and new when the definition changes. The definition's digest
// covers its secret, so the id reveals nothing of the positions.
func batchID(d *Definition, epoch cursor.Epoch, start, end pglogrepl.LSN) string {
	h := sha256.New()
	h.Write(d.digest[:])
	for _, n := range []uint64{epoch.SystemID, uint64(epoch.Timeline), uint64(start), uint64(end)} {
		h.Write(binary.BigEndian.AppendUint64(nil, n))
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil)[:16])
}
