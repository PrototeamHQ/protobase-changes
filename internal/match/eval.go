package match

import "github.com/PrototeamHQ/protobase-changes/internal/change"

// truth is SQL's three-valued logic, where comparing with null is unknown.
type truth int8

const (
	no truth = iota
	unknown
	yes
)

func of(b bool) truth {
	if b {
		return yes
	}
	return no
}

// eval tests the row, which states every column the condition reads, as a
// WHERE clause would: only yes lets the row in.
func (c *Condition) eval(row view) truth {
	switch c.op {
	case opAnd:
		result := yes
		for _, child := range c.children {
			result = min(result, child.eval(row))
		}
		return result
	case opOr:
		result := no
		for _, child := range c.children {
			result = max(result, child.eval(row))
		}
		return result
	case opNot:
		return yes - c.children[0].eval(row)
	case opIsNull:
		v, _ := row.get(c.column)
		return of(v.Kind == change.Null)
	}

	v, _ := row.get(c.column)
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
