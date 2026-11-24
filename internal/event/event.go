// Package event shapes what consumers receive as CloudEvents 1.0 in the
// structured JSON format.
package event

import (
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

const (
	// TransactionType carries one committed transaction's changes.
	TransactionType = "protobase.changes.transaction"
	// PositionType carries only a cursor: where the consumer got to when
	// nothing it may read came for a while.
	PositionType = "protobase.changes.position"
)

// Event is one CloudEvent. Cursor is the extension attribute consumers
// resume after this event with.
type Event struct {
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Source          string `json:"source"`
	Type            string `json:"type"`
	Time            string `json:"time"`
	DataContentType string `json:"datacontenttype,omitempty"`
	Cursor          string `json:"cursor"`
	Data            *Data  `json:"data,omitempty"`
}

// Transaction makes the event of entry as tok may read it. ok is false when
// tok may read none of its changes: the consumer then learns nothing of it.
func Transaction(source string, entry *feed.Entry, tok token.Token) (e Event, ok bool) {
	data := data(entry.Tx, tok)
	if len(data.Changes) == 0 {
		return Event{}, false
	}
	return Event{
		SpecVersion:     "1.0",
		ID:              entry.ID,
		Source:          source,
		Type:            TransactionType,
		Time:            entry.Tx.CommitTime.UTC().Format(time.RFC3339Nano),
		DataContentType: "application/json",
		Cursor:          entry.Cursor,
		Data:            &data,
	}, true
}

// Position makes an event that only moves the consumer's cursor.
func Position(source, cursor string, now time.Time) Event {
	id := make([]byte, 16)
	rand.Read(id)
	return Event{
		SpecVersion: "1.0",
		ID:          base64.RawURLEncoding.EncodeToString(id),
		Source:      source,
		Type:        PositionType,
		Time:        now.UTC().Format(time.RFC3339Nano),
		Cursor:      cursor,
	}
}
