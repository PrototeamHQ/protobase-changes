package match

import "github.com/PrototeamHQ/protobase-changes/internal/change"

// truth is SQL's three-valued logic, where comparing with null is unknown,
// plus missing: the WAL lacks a column the answer depends on.
type truth int8

const (
	no truth = iota
	unknown
	yes
	missing
)

func of(b bool) truth {
	if b {
		return yes
	}
	return no
}

// eval tests the row as a WHERE clause would: only yes lets the row in. A
// missing column makes the answer missing only where its value could change
// it: AND is no once one part is no, OR is yes once one part is yes.
func (c *Condition) eval(row view) truth {
	switch c.op {
	case opAnd:
		return combine(c.children, row, no)
	case opOr:
		return combine(c.children, row, yes)
	case opNot:
		t := c.children[0].eval(row)
		if t == missing {
			return missing
		}
		return yes - t
	}

	v, ok := row.get(c.column)
	if !ok {
		return missing
	}
	if c.op == opIsNull {
		return of(v.Kind == change.Null)
	}
	if v.Kind == change.Null {
		return unknown
	}
	switch c.op {
	case opIn:
		result := no
		for _, value := range c.values {
			result = max(result, compareWith(opEq, value, v.Text))
		}
		return result
	case opBetween:
		return min(compareWith(opGte, c.values[0], v.Text), compareWith(opLte, c.values[1], v.Text))
	}
	return compareWith(c.op, c.values[0], v.Text)
}

// combine joins the parts of an AND, whose decisive truth is no, or of an
// OR, whose decisive truth is yes. A decisive part settles the result
// whatever the missing parts hold; otherwise a missing part leaves it
// missing, an unknown one unknown, and else it is the other truth.
func combine(parts []*Condition, row view, decisive truth) truth {
	result := yes - decisive
	anyMissing := false
	for _, part := range parts {
		switch part.eval(row) {
		case decisive:
			return decisive
		case missing:
			anyMissing = true
		case unknown:
			result = unknown
		}
	}
	if anyMissing {
		return missing
	}
	return result
}

func compareWith(op operator, c constant, text string) truth {
	cmp, ok := c.compare(text)
	if !ok {
		return unknown
	}
	switch op {
	case opEq:
		return of(cmp == 0)
	case opNeq:
		return of(cmp != 0)
	case opLt:
		return of(cmp < 0)
	case opLte:
		return of(cmp <= 0)
	case opGt:
		return of(cmp > 0)
	case opGte:
		return of(cmp >= 0)
	}
	panic("not a comparison")
}
