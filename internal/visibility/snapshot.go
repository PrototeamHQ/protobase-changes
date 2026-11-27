// Package visibility tells which transactions a Postgres snapshot saw.
package visibility

import (
	"errors"
	"strconv"
	"strings"
)

// ErrInvalid means a snapshot's text is not pg_current_snapshot()'s format.
var ErrInvalid = errors.New("invalid snapshot")

// maxInProgress bounds the transactions a snapshot may list as in progress,
// above any server's max_connections.
const maxInProgress = 1 << 18

// Snapshot is the set of transactions a reader saw, as pg_current_snapshot()
// states it: every one before xmin, and those before xmax that are not in
// progress. It keeps the full 64-bit xids, so two snapshots compare however
// far apart they are.
type Snapshot struct {
	xmin, xmax uint64
	inProgress map[uint64]bool
}

// Parse reads pg_current_snapshot()'s text, xmin:xmax:xip, the in-progress
// xids comma separated.
func Parse(text string) (Snapshot, error) {
	parts := strings.Split(text, ":")
	if len(parts) != 3 {
		return Snapshot{}, ErrInvalid
	}
	xmin, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return Snapshot{}, ErrInvalid
	}
	xmax, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || xmax < xmin || xmax > 1<<62 {
		return Snapshot{}, ErrInvalid
	}
	s := Snapshot{xmin: xmin, xmax: xmax, inProgress: map[uint64]bool{}}
	if parts[2] == "" {
		return s, nil
	}
	xip := strings.Split(parts[2], ",")
	if len(xip) > maxInProgress {
		return Snapshot{}, ErrInvalid
	}
	for _, raw := range xip {
		xid, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || xid < xmin || xid >= xmax {
			return Snapshot{}, ErrInvalid
		}
		s.inProgress[xid] = true
	}
	return s, nil
}

// Saw reports whether the snapshot saw the transaction of the 32-bit xid
// the WAL states: it committed, or aborted, before the snapshot was taken.
// The xid is read as the one nearest the snapshot's xmax, as Postgres reads
// a 32-bit xid modulo 2^32, which holds within the two billion transactions
// Postgres lets pass before it freezes old ones.
func (s Snapshot) Saw(xid uint32) bool {
	full := s.widen(xid)
	if full < int64(s.xmin) {
		return true
	}
	return full < int64(s.xmax) && !s.inProgress[uint64(full)]
}

// saw reports whether the snapshot saw the transaction of the 64-bit xid.
func (s Snapshot) saw(xid uint64) bool {
	return xid < s.xmin || xid < s.xmax && !s.inProgress[xid]
}

// SawAll reports whether s saw every transaction b saw.
func (s Snapshot) SawAll(b Snapshot) bool {
	if s.xmax < b.xmax {
		return false
	}
	for xid := range s.inProgress {
		if xid < b.xmax && b.saw(xid) {
			return false
		}
	}
	return true
}

// InProgressUpTo reports whether the snapshot lists as in progress any
// transaction at or before the 32-bit xid, read as Saw reads it.
func (s Snapshot) InProgressUpTo(xid uint32) bool {
	full := s.widen(xid)
	for running := range s.inProgress {
		if int64(running) <= full {
			return true
		}
	}
	return false
}

// widen reads a 32-bit xid as the 64-bit one nearest the snapshot's xmax.
func (s Snapshot) widen(xid uint32) int64 {
	return int64(s.xmax) + int64(int32(xid-uint32(s.xmax)))
}
