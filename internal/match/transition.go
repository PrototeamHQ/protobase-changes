package match

import "github.com/PrototeamHQ/protobase-changes/internal/change"

// Transition holds conditions on a row before and after a change, such as an
// order whose status was not paid and now is.
type Transition struct {
	Old *Condition
	New *Condition
}

// Holds reports whether the change meets the transition: the row before it
// meets Old where the change has a row before it, and the row after it meets
// New where it has one after it. A column the WAL lacks fails the condition
// it is in, as does a truncate, which states no rows.
func (t Transition) Holds(c *change.Change) bool {
	if c.Operation == change.Truncate {
		return false
	}
	if before := oldView(c); before != nil && t.Old != nil && t.Old.eval(*before) != yes {
		return false
	}
	if after := newView(c); after != nil && t.New != nil && t.New.eval(*after) != yes {
		return false
	}
	return true
}
