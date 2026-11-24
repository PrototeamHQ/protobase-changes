package event

import (
	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/match"
)

// visible picks what of a change its subscriptions may read: the old row
// only when it was in one of them, the new row only when it is in one now,
// so a row that entered shows nothing of where it came from and a row that
// left nothing of where it went. A row that only left goes by its old key
// when the WAL states it.
func visible(c *change.Change, tags []match.Tag) (key, oldRow, newRow change.Row) {
	wasIn, isIn := false, false
	for _, tag := range tags {
		wasIn = wasIn || tag.Effect != match.Enter
		isIn = isIn || tag.Effect != match.Leave
	}
	key, oldRow, newRow = c.Key, c.OldRow, c.NewRow
	if !wasIn {
		oldRow = nil
	}
	if !isIn {
		newRow = nil
		if c.OldRowKind == change.OldKeyOnly {
			key = c.OldRow
		}
	}
	return key, oldRow, newRow
}
