package match

import "github.com/PrototeamHQ/protobase-changes/internal/change"

// Effect is what a change did to a subscription's rows.
type Effect string

const (
	// Enter: the row meets the conditions now and did not before.
	Enter Effect = "enter"
	// Update: the row meets the conditions now, and did before or the WAL
	// does not say: the consumer keeps the row with its new values.
	Update Effect = "update"
	// Leave: the row met the conditions and no longer does, or is gone. A
	// truncate leaves every subscription to its table.
	Leave Effect = "leave"
)

// Tag names a subscription a change concerns and what it did there.
type Tag struct {
	ID     int    `json:"id"`
	Effect Effect `json:"effect"`
}

type membership uint8

const (
	out membership = iota
	in
	// indeterminate: the WAL lacks a column the answer depends on.
	indeterminate
)

func (e *entry) membership(row *view) membership {
	if row == nil {
		return out
	}
	if e.Where == nil {
		return in
	}
	switch e.Where.eval(*row) {
	case yes:
		return in
	case missing:
		return indeterminate
	}
	return out
}

// Match tags the change of a row of the resource with each subscription it
// concerns, in the order the subscriptions were given, so one change goes
// out once for all of them. A row that may have left a subscription without
// the WAL saying so concerns it not at all: telling would show a row the
// subscription may not read.
func (ix *Index) Match(resource string, c *change.Change) []Tag {
	r := ix.resources[resource]
	if r == nil {
		return nil
	}
	var tags []Tag
	if c.Operation == change.Truncate {
		for _, e := range r.entries {
			tags = append(tags, Tag{ID: e.ID, Effect: Leave})
		}
		return tags
	}

	before, after := oldView(c), newView(c)
	for _, e := range r.candidates(before, after) {
		if effect, ok := effectOf(e.membership(before), e.membership(after)); ok {
			tags = append(tags, Tag{ID: e.ID, Effect: effect})
		}
	}
	return tags
}

func effectOf(before, after membership) (Effect, bool) {
	switch {
	case after == in && before == out:
		return Enter, true
	case after == in:
		return Update, true
	case after == out && before == in:
		return Leave, true
	}
	return "", false
}
