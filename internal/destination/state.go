package destination

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
)

// Status is whether a destination delivers.
type Status string

const (
	Active Status = "active"
	// Paused: a batch failed its last try; resuming tries it again.
	Paused Status = "paused"
	// Gone: the feed no longer holds the changes after the destination's
	// cursor; resuming starts it again from the newest change.
	Gone Status = "gone"
)

// State is where a destination got to. It is written once per delivered
// batch, and on each failed try.
type State struct {
	// Position is after the last transaction the destination is done with.
	Position cursor.Position
	Status   Status
	// Batch is the batch that failed at least once, kept so its retries send
	// the same events under the same id; nil when none failed.
	Batch *Batch
}

// Batch is a batch in flight that failed at least once.
type Batch struct {
	// End is the commit of its last transaction.
	End pglogrepl.LSN
	ID  string
	// Tried counts its failed tries, and Next is when it is tried again.
	Tried int
	Next  time.Time
	// Error says why its last try failed.
	Error string
}

// Letter is a batch that failed its last try, kept for a destination that
// dead-letters instead of pausing.
type Letter struct {
	BatchID string
	// Events is the batch's body, a JSON array of CloudEvents.
	Events []byte
	Error  string
	Tried  int
	At     time.Time
}

// Store keeps each app's destination set and every destination's state. A
// set is kept as the token that defined it, so its secrets stay encrypted.
type Store interface {
	// Definitions returns the app's set token, or "" when it has none.
	Definitions(ctx context.Context, app string) (string, error)
	PutDefinitions(ctx context.Context, app, raw string) error
	States(ctx context.Context, app string) (map[string]State, error)
	PutState(ctx context.Context, app, name string, s State) error
	DeleteState(ctx context.Context, app, name string) error
	// PutLetter keeps a batch once, however often it is put.
	PutLetter(ctx context.Context, app, name string, l Letter) error
}
