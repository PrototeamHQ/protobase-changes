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
// progress. It keeps the low 32 bits of each xid, as the WAL states them, and
// compares them as Postgres does, modulo 2^32, which holds within the two
// billion transactions Postgres lets pass before it freezes old ones.
type Snapshot struct {
	xmin, xmax uint32
	inProgress map[uint32]bool
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
	if err != nil || xmax < xmin {
		return Snapshot{}, ErrInvalid
	}
	s := Snapshot{xmin: uint32(xmin), xmax: uint32(xmax), inProgress: map[uint32]bool{}}
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
		s.inProgress[uint32(xid)] = true
	}
	return s, nil
}

// Saw reports whether the snapshot saw the transaction xid: it committed, or
// aborted, before the snapshot was taken.
func (s Snapshot) Saw(xid uint32) bool {
	if precedes(xid, s.xmin) {
		return true
	}
	return precedes(xid, s.xmax) && !s.inProgress[xid]
}

// SawAll reports whether s saw every transaction b saw.
func (s Snapshot) SawAll(b Snapshot) bool {
	if precedes(s.xmax, b.xmax) {
		return false
	}
	for xid := range s.inProgress {
		if precedes(xid, b.xmax) && b.Saw(xid) {
			return false
		}
	}
	return true
}

// precedes compares xids as Postgres does, modulo 2^32.
func precedes(a, b uint32) bool { return int32(a-b) < 0 }
