// Package stream reads one consumer's way through an app's feed: from where
// it resumes, through what it holds, then live.
package stream

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

var (
	// ErrGone means the feed no longer holds every change after the position,
	// or the position is of another epoch: the consumer resyncs from a snapshot.
	ErrGone = errors.New("the changes after this position are no longer held")
	// ErrRevoked means the app revoked the token's session.
	ErrRevoked = errors.New("the session was revoked")
	// ErrExpired means the token expired; the consumer resumes with a fresh one.
	ErrExpired = errors.New("the token expired")
)

// Stream is one consumer's place in a feed. It is not safe for concurrent use.
type Stream struct {
	feed *feed.Feed
	tok  token.Token
	now  func() time.Time
	// lsn is where the consumer got to; cursor is the cursor it resumes there with.
	lsn    pglogrepl.LSN
	cursor string
}

// Open places a consumer at the sealed cursor it gives, else at the token's
// snapshot cursor, else live after everything taken in so far.
func Open(f *feed.Feed, tok token.Token, sealed string, now func() time.Time) (*Stream, error) {
	head, err := f.Head()
	if err != nil {
		return nil, err
	}
	s := &Stream{feed: f, tok: tok, now: now}
	switch {
	case sealed != "":
		at, err := f.Cursors.Open(sealed)
		if err != nil {
			return nil, err
		}
		if at.Epoch != head.Epoch {
			return nil, ErrGone
		}
		s.lsn, s.cursor = at.LSN, sealed
	case tok.Cursor != nil:
		s.lsn = *tok.Cursor
		s.cursor = f.Cursors.Seal(cursor.Position{Epoch: head.Epoch, LSN: s.lsn})
	default:
		s.lsn, s.cursor = head.LSN, f.Cursors.Seal(head)
	}
	if err := s.check(); err != nil {
		return nil, err
	}
	if _, ok := f.Since(s.lsn); !ok {
		return nil, ErrGone
	}
	return s, nil
}

// Next returns up to limit events the consumer may read after its place,
// waiting for them. When none comes within wait, it returns a position event
// instead, so the consumer keeps up with transactions it may not read.
func (s *Stream) Next(ctx context.Context, wait time.Duration, limit int) ([]event.Event, error) {
	timeout := time.NewTimer(wait)
	defer timeout.Stop()
	expiry := time.NewTimer(s.tok.Expiry.Sub(s.now()))
	defer expiry.Stop()

	for {
		// Taken before reading, so an append in between still wakes the wait.
		changed := s.feed.Changed()
		entries, ok := s.feed.Since(s.lsn)
		if !ok {
			return nil, ErrGone
		}
		// Checked after reading: a revocation is recorded before its
		// transaction is held, so nothing read here committed after it.
		if err := s.check(); err != nil {
			return nil, err
		}
		var out []event.Event
		for _, entry := range entries {
			s.lsn, s.cursor = entry.Commit(), entry.Cursor
			if e, ok := event.Transaction(s.feed.App.Issuer, entry, s.tok); ok {
				out = append(out, e)
				if len(out) == limit {
					break
				}
			}
		}
		if len(out) > 0 {
			return out, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-expiry.C:
			return nil, ErrExpired
		case <-timeout.C:
			return []event.Event{event.Position(s.feed.App.Issuer, s.cursor, s.now())}, nil
		case <-changed:
		}
	}
}

func (s *Stream) check() error {
	now := s.now()
	if !now.Before(s.tok.Expiry) {
		return ErrExpired
	}
	if s.feed.Revoked.Revoked(s.tok.Session, now) {
		return ErrRevoked
	}
	return nil
}
