// Package match finds the subscriptions a row change concerns, by
// conditions on that one row, and whether the row entered, stayed in or
// left each one.
package match

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrCondition means a condition does not compile.
var ErrCondition = errors.New("invalid condition")

type operator uint8

const (
	opEq operator = iota
	opNeq
	opLt
	opLte
	opGt
	opGte
	opIn
	opBetween
	opIsNull
	opAnd
	opOr
	opNot
)

// comparisons are the operators of a column and one constant.
var comparisons = map[string]operator{"eq": opEq, "neq": opNeq, "lt": opLt, "lte": opLte, "gt": opGt, "gte": opGte}

// Condition is a compiled condition on one row.
type Condition struct {
	op       operator
	column   string
	values   []constant
	children []*Condition
}

// Compile reads a condition in the JSON form tokens carry: an object of one
// operator, such as {"eq": ["channel_id", "xyz"]}, {"in": ["status", ["a", "b"]]},
// {"between": ["price", 10, 20]}, {"is_null": "deleted_at"}, {"and": [...]},
// {"or": [...]} or {"not": {...}}. Constants are strings, numbers or booleans.
func Compile(raw json.RawMessage) (*Condition, error) {
	var node map[string]json.RawMessage
	if err := json.Unmarshal(raw, &node); err != nil || len(node) != 1 {
		return nil, fmt.Errorf("%w: want an object of one operator", ErrCondition)
	}
	for name, args := range node {
		return compileOperator(name, args)
	}
	panic("unreachable")
}

func compileOperator(name string, args json.RawMessage) (*Condition, error) {
	if op, ok := comparisons[name]; ok {
		column, rest, err := columnAnd(name, args, 1)
		if err != nil {
			return nil, err
		}
		c, err := parseConstant(rest[0], op != opEq && op != opNeq)
		if err != nil {
			return nil, err
		}
		return &Condition{op: op, column: column, values: []constant{c}}, nil
	}

	switch name {
	case "in":
		column, rest, err := columnAnd(name, args, 1)
		if err != nil {
			return nil, err
		}
		var list []json.RawMessage
		if err := strictUnmarshal(rest[0], &list); err != nil || len(list) == 0 {
			return nil, fmt.Errorf("%w: in takes a column and a non-empty list", ErrCondition)
		}
		values := make([]constant, len(list))
		for i, raw := range list {
			if values[i], err = parseConstant(raw, false); err != nil {
				return nil, err
			}
		}
		return &Condition{op: opIn, column: column, values: values}, nil
	case "between":
		column, rest, err := columnAnd(name, args, 2)
		if err != nil {
			return nil, err
		}
		low, err := parseConstant(rest[0], true)
		if err != nil {
			return nil, err
		}
		high, err := parseConstant(rest[1], true)
		if err != nil {
			return nil, err
		}
		return &Condition{op: opBetween, column: column, values: []constant{low, high}}, nil
	case "is_null":
		var column string
		if err := strictUnmarshal(args, &column); err != nil || column == "" {
			return nil, fmt.Errorf("%w: is_null takes a column", ErrCondition)
		}
		return &Condition{op: opIsNull, column: column}, nil
	case "and", "or":
		var list []json.RawMessage
		if err := strictUnmarshal(args, &list); err != nil || len(list) == 0 {
			return nil, fmt.Errorf("%w: %s takes a non-empty list of conditions", ErrCondition, name)
		}
		c := &Condition{op: opAnd}
		if name == "or" {
			c.op = opOr
		}
		for _, raw := range list {
			child, err := Compile(raw)
			if err != nil {
				return nil, err
			}
			c.children = append(c.children, child)
		}
		return c, nil
	case "not":
		child, err := Compile(args)
		if err != nil {
			return nil, err
		}
		return &Condition{op: opNot, children: []*Condition{child}}, nil
	}
	return nil, fmt.Errorf("%w: unknown operator %q", ErrCondition, name)
}

// columnAnd reads the arguments [column, constant...] with n constants.
func columnAnd(name string, args json.RawMessage, n int) (string, []json.RawMessage, error) {
	var list []json.RawMessage
	var column string
	if strictUnmarshal(args, &list) != nil || len(list) != n+1 || strictUnmarshal(list[0], &column) != nil || column == "" {
		return "", nil, fmt.Errorf("%w: %s takes a column and %d constant(s)", ErrCondition, name, n)
	}
	return column, list[1:], nil
}

// strictUnmarshal refuses a JSON null, which Unmarshal would take as nothing.
func strictUnmarshal(raw json.RawMessage, v any) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ErrCondition
	}
	return json.Unmarshal(raw, v)
}

// columns lists every column the condition reads, once each.
func (c *Condition) columns(into map[string]bool) {
	if c.column != "" {
		into[c.column] = true
	}
	for _, child := range c.children {
		child.columns(into)
	}
}
