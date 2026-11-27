package relay

import (
	"errors"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/match"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
	"github.com/PrototeamHQ/protobase-changes/internal/visibility"
)

// registration is one live query of a page, as the relay registered it.
type registration struct {
	id     string
	subs   *match.Index
	fields []string
	// sid is the session that revokes it, empty for an anonymous one.
	sid string
	// seen is the snapshot the page read, whose transactions it already has;
	// nil when it resumes from cursor.
	seen   *visibility.Snapshot
	cursor string
	// lsn is where it got to in the feed, once placed.
	lsn pglogrepl.LSN
}

// newRegistration checks a register message. reason says why it is
// refused, when it is.
func newRegistration(m Message) (r *registration, reason string) {
	if len(m.Subs) == 0 {
		return nil, "subs lists no subscription"
	}
	subs, err := token.Subscriptions(m.Subs)
	if errors.Is(err, token.ErrConditions) {
		return nil, "invalid conditions"
	}
	if err != nil {
		return nil, "a subscription names no resource"
	}
	if (m.Sid == "") == !m.Anonymous {
		return nil, "a register names a sid or is anonymous, not both"
	}
	if (m.Snapshot == "") == (m.Cursor == "") {
		return nil, "a register starts from a snapshot or a cursor, not both"
	}
	r = &registration{id: m.ID, subs: subs, fields: m.Fields, sid: m.Sid, cursor: m.Cursor}
	if m.Snapshot != "" {
		seen, err := visibility.Parse(m.Snapshot)
		if err != nil {
			return nil, "invalid snapshot"
		}
		r.seen = &seen
	}
	return r, ""
}

// placement is where a registration starts in its app's feed.
type placement int

const (
	placed placement = iota
	// notReady: the feed's stream has not caught up; it is placed later.
	notReady
	// gap: the feed no longer holds every change after its start.
	gap
	revoked
	invalidCursor
)

// place sets where r starts in f: after its cursor, or at the floor when
// its snapshot saw everything the feed no longer holds.
func (r *registration) place(f *feed.Feed, now time.Time) placement {
	if r.sid != "" && f.Revoked.Revoked(r.sid, now) {
		return revoked
	}
	if r.seen != nil {
		// SnapshotStart fails only with feed.ErrNotReady.
		lsn, ok, err := f.SnapshotStart(*r.seen)
		switch {
		case err != nil:
			return notReady
		case !ok:
			return gap
		}
		r.lsn = lsn
		return placed
	}
	// Open fails only with cursor.ErrInvalid, and Head with feed.ErrNotReady.
	at, err := f.Cursors.Open(r.cursor)
	if err != nil {
		return invalidCursor
	}
	head, err := f.Head()
	if err != nil {
		return notReady
	}
	if at.Epoch != head.Epoch || at.LSN < f.Floor() {
		return gap
	}
	r.lsn = at.LSN
	return placed
}
