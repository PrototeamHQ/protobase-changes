package confirm

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
)

// fakeSlot records what is confirmed and reports what was received.
type fakeSlot struct {
	received  pglogrepl.LSN
	confirmed []pglogrepl.LSN
}

func (s *fakeSlot) Confirm(lsn pglogrepl.LSN) { s.confirmed = append(s.confirmed, lsn) }
func (s *fakeSlot) Received() pglogrepl.LSN   { return s.received }
func (s *fakeSlot) last() pglogrepl.LSN {
	if len(s.confirmed) == 0 {
		return 0
	}
	return s.confirmed[len(s.confirmed)-1]
}

// confirmFixture is a confirmer of a feed of two transactions that started
// at 100, with a step of 1000.
func confirmFixture(t *testing.T, stored Stored) (*Confirmer, *feed.Feed, *fakeSlot) {
	t.Helper()
	f, err := feed.New(config.App{Name: "shop", TailTransactions: 2}, keys.Ring{bytes.Repeat([]byte{1}, keys.Size)},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Resume(cursor.Epoch{SystemID: 1, Timeline: 1}, 100, 100); err != nil {
		t.Fatal(err)
	}
	s := &fakeSlot{}
	return &Confirmer{feed: f, slot: s, stored: stored, step: 1000}, f, s
}

func commit(f *feed.Feed, lsn pglogrepl.LSN) {
	f.Append(&change.Transaction{CommitLSN: lsn, EndLSN: lsn + 8})
}

func TestConfirmsTheTailFloorWhileTransactionsComeIn(t *testing.T) {
	c, f, s := confirmFixture(t, NoDestinations)
	c.tick()
	if s.last() != 100 {
		t.Fatalf("confirmed %v, want the stream's start", s.confirmed)
	}
	commit(f, 200)
	commit(f, 300)
	commit(f, 400)
	c.tick()
	if s.last() != 200 {
		t.Fatalf("confirmed %v, want the dropped transaction's commit 200", s.confirmed)
	}
}

func TestConfirmsNothingUntilAStepWasReceived(t *testing.T) {
	c, _, s := confirmFixture(t, NoDestinations)
	c.tick()
	s.received = 999
	c.tick()
	if len(s.confirmed) != 1 || s.last() != 100 {
		t.Fatalf("confirmed %v, want only the stream's start below a step", s.confirmed)
	}
}

func TestConfirmsTheReceivedPositionOnceTheDestinationsStoredIt(t *testing.T) {
	var stored pglogrepl.LSN = 300
	c, f, s := confirmFixture(t, func() (pglogrepl.LSN, bool) { return stored, true })
	commit(f, 300)
	s.received = 5000
	c.tick()
	if head, _ := f.Head(); head.LSN != 5000 || f.Idle() != 5000 {
		t.Fatalf("head %v, idle %v, want both advanced to the received 5000", head, f.Idle())
	}
	if s.last() != 300 {
		t.Fatalf("confirmed %v, want the destination's stored 300 until it stores the head", s.confirmed)
	}
	stored = 5000
	c.tick()
	if s.last() != 5000 {
		t.Fatalf("confirmed %v, want the received 5000", s.confirmed)
	}
}

func TestConfirmsTheReceivedPositionWithoutDestinations(t *testing.T) {
	c, _, s := confirmFixture(t, NoDestinations)
	s.received = 5000
	c.tick()
	if s.last() != 5000 {
		t.Fatalf("confirmed %v, want the received 5000", s.confirmed)
	}
}

func TestConfirmNeverGoesBack(t *testing.T) {
	var stored pglogrepl.LSN = 5000
	c, _, s := confirmFixture(t, func() (pglogrepl.LSN, bool) { return stored, true })
	s.received = 5000
	c.tick()
	// A destination that starts holds the confirm back at zero.
	stored = 0
	s.received = 9000
	c.tick()
	for i := 1; i < len(s.confirmed); i++ {
		if s.confirmed[i] <= s.confirmed[i-1] {
			t.Fatalf("confirmed %v, want every confirm past the one before", s.confirmed)
		}
	}
	if s.last() != 5000 {
		t.Fatalf("confirmed %v, want it held at 5000", s.confirmed)
	}
}

func TestStepIsASegmentOrHalfTheKeptWAL(t *testing.T) {
	const segment = 16 << 20
	for _, tc := range []struct {
		keep int64
		want pglogrepl.LSN
	}{{-1, segment}, {1 << 20, 512 << 10}, {1 << 30, segment}} {
		if got := step(segment, tc.keep); got != tc.want {
			t.Errorf("step(%d) = %d, want %d", tc.keep, got, tc.want)
		}
	}
}
