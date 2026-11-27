package feed

import "github.com/PrototeamHQ/protobase-changes/internal/visibility"

// horizon tells whether a snapshot saw every transaction committed before
// the feed's floor, so the feed holds every one it did not see. Those are
// what committed before the stream started, which start saw, and what the
// tail dropped since. A snapshot that missed a dropped transaction missed
// the newest dropped ones too, as those committed after it: all but the few
// whose commits ran at the same time and became visible first. So the
// horizon keeps the xids of as many dropped transactions as the tail holds,
// and the newest xid dropped, which a snapshot must also have seen.
type horizon struct {
	// start saw every transaction committed before the stream started; nil
	// until the stream reports one, when no snapshot passes.
	start   *visibility.Snapshot
	dropped []uint32
	next    int
	full    bool
	// newest is the latest xid dropped, when any was.
	newest    uint32
	anyNewest bool
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
	h.dropped[h.next] = xid
	h.next = (h.next + 1) % len(h.dropped)
	h.full = h.full || h.next == 0
	if !h.anyNewest || int32(h.newest-xid) < 0 {
		h.newest, h.anyNewest = xid, true
	}
}

// sawAll reports whether s saw every transaction committed before the floor.
func (h *horizon) sawAll(s visibility.Snapshot) bool {
	if h.start == nil || !s.SawAll(*h.start) {
		return false
	}
	if h.anyNewest && !s.Saw(h.newest) {
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
