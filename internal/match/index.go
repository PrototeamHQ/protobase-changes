package match

import (
	"slices"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// Subscription asks for the changes of one resource's rows that meet Where,
// or of every row when Where is nil.
type Subscription struct {
	ID       int
	Resource string
	Where    *Condition
}

type entry struct {
	Subscription
	// position orders matches as the subscriptions were given.
	position int
}

// indexKey is one column's value as an equality states it: as text, or as a
// number in its canonical form.
type indexKey struct {
	column string
	number bool
	text   string
}

// resource holds one resource's subscriptions. One that requires a column to
// equal a constant is found by hash lookup of that column's value; only the
// rest are tested against every change.
type resource struct {
	entries []*entry
	scan    []*entry
	columns []string
	lookup  map[indexKey][]*entry
}

// Index finds the subscriptions a change concerns. It does not change once
// made, so it is safe for concurrent use.
type Index struct {
	resources map[string]*resource
}

func NewIndex(subs []Subscription) *Index {
	ix := &Index{resources: map[string]*resource{}}
	for i, sub := range subs {
		r := ix.resources[sub.Resource]
		if r == nil {
			r = &resource{lookup: map[indexKey][]*entry{}}
			ix.resources[sub.Resource] = r
		}
		e := &entry{Subscription: sub, position: i}
		r.entries = append(r.entries, e)

		column, values, ok := anchor(sub.Where)
		if !ok {
			r.scan = append(r.scan, e)
			continue
		}
		if !slices.Contains(r.columns, column) {
			r.columns = append(r.columns, column)
		}
		for _, v := range values {
			r.lookup[v.key(column)] = append(r.lookup[v.key(column)], e)
		}
	}
	return ix
}

// Has reports whether any subscription is to the resource.
func (ix *Index) Has(resource string) bool { return ix.resources[resource] != nil }

// anchor finds an equality or IN that the whole condition requires: the
// first one among the conditions joined by AND at its top.
func anchor(c *Condition) (string, []constant, bool) {
	if c == nil {
		return "", nil, false
	}
	var in *Condition
	for _, part := range conjuncts(c, nil) {
		if part.op == opEq {
			return part.column, part.values, true
		}
		if part.op == opIn && in == nil {
			in = part
		}
	}
	if in == nil {
		return "", nil, false
	}
	return in.column, in.values, true
}

func conjuncts(c *Condition, into []*Condition) []*Condition {
	if c.op != opAnd {
		return append(into, c)
	}
	for _, child := range c.children {
		into = conjuncts(child, into)
	}
	return into
}

// candidates are the subscriptions the old or the new row may meet: those
// without an equality, and those whose equality one of the rows meets.
func (r *resource) candidates(rows ...*view) []*entry {
	found := map[*entry]bool{}
	for _, e := range r.scan {
		found[e] = true
	}
	for _, column := range r.columns {
		for _, row := range rows {
			if row == nil {
				continue
			}
			v, ok := row.get(column)
			if !ok || v.Kind != change.Text {
				continue
			}
			for _, e := range r.lookup[indexKey{column: column, text: v.Text}] {
				found[e] = true
			}
			if n, ok := parseNumber(v.Text); ok {
				for _, e := range r.lookup[indexKey{column: column, number: true, text: n.RatString()}] {
					found[e] = true
				}
			}
		}
	}
	out := make([]*entry, 0, len(found))
	for e := range found {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b *entry) int { return a.position - b.position })
	return out
}
