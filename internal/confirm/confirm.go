// Package confirm confirms the slot of an app without an archive.
package confirm

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

// every is how often the confirm moves.
const every = time.Second

// Slot lets Postgres drop the WAL up to a position, such as the reader.
type Slot interface {
	Confirm(lsn pglogrepl.LSN)
	// Received is a position before which every committed transaction
	// reached the feed.
	Received() pglogrepl.LSN
}

// Stored returns the lowest cursor a destination store holds, such as a
// hub's; ok is false when no destination holds back the confirm.
type Stored func() (lsn pglogrepl.LSN, ok bool)

// NoDestinations is the Stored of an app without destinations.
func NoDestinations() (pglogrepl.LSN, bool) { return 0, false }

// Confirmer confirms the slot of an app without an archive. Consumers
// replay from the feed's in-memory tail and destinations keep their cursors
// in their store, so the WAL behind the tail serves nothing: the confirmer
// confirms the tail's floor. Once the stream received a step of WAL past
// where the head was last advanced, such as WAL of other databases while the
// app is idle, it advances the feed's head there and confirms it after the
// destinations stored it, so the slot keeps a bounded amount of WAL.
type Confirmer struct {
	// url is the app database's, whose server's WAL settings set the step.
	url    string
	feed   *feed.Feed
	slot   Slot
	stored Stored
	// step is how far the received position must pass the advanced head
	// before the head advances again, which bounds the destinations' writes.
	step pglogrepl.LSN

	advanced, confirmed pglogrepl.LSN
}

// New returns the confirmer of the slot that slot reads into f, from the
// app database at url.
func New(url string, f *feed.Feed, slot Slot, stored Stored) *Confirmer {
	return &Confirmer{url: url, feed: f, slot: slot, stored: stored}
}

// tick advances the head when the stream received a step past it, then
// confirms as far as it may.
func (c *Confirmer) tick() {
	if received := c.slot.Received(); received >= c.advanced+c.step {
		c.feed.Advance(received)
		c.advanced = received
	}
	target := c.advanced
	if lsn, ok := c.stored(); ok {
		target = min(target, lsn)
	}
	target = max(target, c.feed.Floor())
	if target > c.confirmed {
		c.slot.Confirm(target)
		c.confirmed = target
	}
}

// Run reads the step, then confirms until ctx ends or reading the step fails.
func (c *Confirmer) Run(ctx context.Context) error {
	step, err := readStep(ctx, c.url)
	if err != nil {
		return err
	}
	c.step = step
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		c.tick()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
