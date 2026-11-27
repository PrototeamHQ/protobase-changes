package feed

import "github.com/PrototeamHQ/protobase-changes/internal/visibility"

// horizon tells whether a snapshot saw every transaction committed before
// the feed's floor, so the feed holds every one it did not see. Those are
// what committed before the stream started, which start saw, and what the
// tail dropped since. The horizon keeps the xids of as many dropped
// transactions as the tail holds, each checked on its own. Of the older
// ones it keeps only the newest, forgotten: a snapshot that lists any
// transaction up to it as in progress may have missed one of them, so it
// does not pass. The newest xid dropped bounds every dropped one from above:
// a snapshot must have seen it too.
type horizon struct {
	// start saw every transaction committed before the stream started; nil
	// until the stream reports one, when no snapshot passes.
	start   *visibility.Snapshot
	dropped []uint32
	next    int
	full    bool
	// newest is the latest xid dropped, and forgotten the latest xid the
	// ring let go of, when any was.
	newest, forgotten       uint32
	anyNewest, anyForgotten bool
}

func newHorizon(size int) horizon {
	return horizon{dropped: make([]uint32, size)}
}

// reset starts the horizon again at a stream's start, which start saw all of.
func (h *horizon) reset(start *visibility.Snapshot) {
	*h = horizon{start: start, dropped: h.dropped}
}

// drop records a transaction the tail no longer holds.
func (h *horizon) drop(xid uint32) {
	if h.full {
		h.forgotten, h.anyForgotten = later(h.forgotten, h.anyForgotten, h.dropped[h.next]), true
	}
	h.dropped[h.next] = xid
	h.next = (h.next + 1) % len(h.dropped)
	h.full = h.full || h.next == 0
	h.newest, h.anyNewest = later(h.newest, h.anyNewest, xid), true
}

// later returns the later of xid and current, compared modulo 2^32, or xid
// when there is no current.
func later(current uint32, any bool, xid uint32) uint32 {
	if !any || int32(current-xid) < 0 {
		return xid
	}
	return current
}

// sawAll reports whether s saw every transaction committed before the floor.
func (h *horizon) sawAll(s visibility.Snapshot) bool {
	if h.start == nil || !s.SawAll(*h.start) {
		return false
	}
	if h.anyNewest && !s.Saw(h.newest) {
		return false
	}
	if h.anyForgotten && s.InProgressUpTo(h.forgotten) {
		return false
	}
	kept := h.next
	if h.full {
		kept = len(h.dropped)
	}
	for _, xid := range h.dropped[:kept] {
		if !s.Saw(xid) {
			return false
		}
	}
	return true
}
