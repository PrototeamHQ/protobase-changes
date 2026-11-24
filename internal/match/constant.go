package match

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

// constant is a condition's value. A number compares numerically with the
// column's text read as a number; any other constant compares with the text
// byte by byte, as under the C collation. A boolean is the text Postgres
// writes for it, t or f.
type constant struct {
	text   string
	number *big.Rat
}

// decimal is the shape of a number in Postgres' text format, with an exponent
// of at most four digits so reading one stays cheap. It leaves out NaN and
// Infinity, which compare as unknown.
var decimal = regexp.MustCompile(`^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]{1,4})?$`)

func parseNumber(s string) (*big.Rat, bool) {
	if !decimal.MatchString(s) {
		return nil, false
	}
	return new(big.Rat).SetString(s)
}

// parseConstant reads a string, number or boolean; ordered refuses a
// boolean, which has no order to compare by.
func parseConstant(raw json.RawMessage, ordered bool) (constant, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return constant{}, fmt.Errorf("%w: %s", ErrCondition, err)
	}
	switch v := v.(type) {
	case string:
		return constant{text: v}, nil
	case json.Number:
		n, ok := parseNumber(v.String())
		if !ok {
			return constant{}, fmt.Errorf("%w: number %s out of range", ErrCondition, v)
		}
		return constant{number: n}, nil
	case bool:
		if ordered {
			return constant{}, fmt.Errorf("%w: a boolean has no order", ErrCondition)
		}
		if v {
			return constant{text: "t"}, nil
		}
		return constant{text: "f"}, nil
	}
	return constant{}, fmt.Errorf("%w: a constant is a string, number or boolean; is_null tests for null", ErrCondition)
}

// compare orders text against c. ok is false when the text is no number
// while c is one.
func (c constant) compare(text string) (int, bool) {
	if c.number == nil {
		return strings.Compare(text, c.text), true
	}
	n, ok := parseNumber(text)
	if !ok {
		return 0, false
	}
	return n.Cmp(c.number), true
}

// key is the constant as the index looks it up under column.
func (c constant) key(column string) indexKey {
	if c.number == nil {
		return indexKey{column: column, text: c.text}
	}
	return indexKey{column: column, number: true, text: c.number.RatString()}
}
