package match

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

func compile(t *testing.T, where string) *Condition {
	t.Helper()
	c, err := Compile(json.RawMessage(where))
	if err != nil {
		t.Fatalf("%s: %v", where, err)
	}
	return c
}

func text(s string) change.Value { return change.Value{Kind: change.Text, Text: s} }

var null = change.Value{Kind: change.Null}

func TestEval(t *testing.T) {
	row := view{row: change.Row{"status": text("open"), "price": text("12.50"), "flag": text("t"), "note": null, "big": text("1e400")}}
	cases := map[string]truth{
		`{"eq": ["status", "open"]}`:                                           yes,
		`{"eq": ["status", "closed"]}`:                                         no,
		`{"neq": ["status", "closed"]}`:                                        yes,
		`{"eq": ["price", 12.5]}`:                                              yes,
		`{"eq": ["price", "12.5"]}`:                                            no,
		`{"gt": ["price", 9]}`:                                                 yes,
		`{"gt": ["price", "9"]}`:                                               no,
		`{"lte": ["price", 12.5]}`:                                             yes,
		`{"lt": ["price", 12.5]}`:                                              no,
		`{"gte": ["big", 1e300]}`:                                              yes,
		`{"between": ["price", 10, 20]}`:                                       yes,
		`{"between": ["price", 13, 20]}`:                                       no,
		`{"in": ["status", ["new", "open"]]}`:                                  yes,
		`{"in": ["status", ["new"]]}`:                                          no,
		`{"eq": ["flag", true]}`:                                               yes,
		`{"eq": ["status", 1]}`:                                                unknown,
		`{"is_null": "note"}`:                                                  yes,
		`{"not": {"is_null": "status"}}`:                                       yes,
		`{"eq": ["note", "x"]}`:                                                unknown,
		`{"neq": ["note", "x"]}`:                                               unknown,
		`{"not": {"eq": ["note", "x"]}}`:                                       unknown,
		`{"in": ["note", ["x"]]}`:                                              unknown,
		`{"and": [{"eq": ["status", "open"]}, {"eq": ["note", "x"]}]}`:         unknown,
		`{"and": [{"eq": ["status", "closed"]}, {"eq": ["note", "x"]}]}`:       no,
		`{"or": [{"eq": ["status", "open"]}, {"eq": ["note", "x"]}]}`:          yes,
		`{"or": [{"eq": ["status", "closed"]}, {"eq": ["note", "x"]}]}`:        unknown,
		`{"not": {"or": [{"eq": ["status", "closed"]}, {"is_null": "note"}]}}`: no,
	}
	for where, want := range cases {
		if got := compile(t, where).eval(row); got != want {
			t.Errorf("%s: %d, want %d", where, got, want)
		}
	}
}

func TestMissingColumnsMatterOnlyWhereTheyCouldChangeTheAnswer(t *testing.T) {
	// The WAL states id and note, a null, but not body.
	row := view{row: change.Row{"id": text("1"), "note": null}}
	cases := map[string]truth{
		`{"eq": ["body", "x"]}`:                                        missing,
		`{"is_null": "body"}`:                                          missing,
		`{"not": {"eq": ["body", "x"]}}`:                               missing,
		`{"or": [{"eq": ["id", 1]}, {"eq": ["body", "x"]}]}`:           yes,
		`{"or": [{"eq": ["id", 2]}, {"eq": ["body", "x"]}]}`:           missing,
		`{"and": [{"eq": ["id", 2]}, {"eq": ["body", "x"]}]}`:          no,
		`{"and": [{"eq": ["id", 1]}, {"eq": ["body", "x"]}]}`:          missing,
		`{"not": {"and": [{"eq": ["id", 2]}, {"eq": ["body", "x"]}]}}`: yes,
		// Null and missing together: NOT could turn the missing part either way.
		`{"and": [{"eq": ["note", "x"]}, {"eq": ["body", "x"]}]}`: missing,
		`{"or": [{"eq": ["note", "x"]}, {"eq": ["body", "x"]}]}`:  missing,
	}
	for where, want := range cases {
		if got := compile(t, where).eval(row); got != want {
			t.Errorf("%s: %d, want %d", where, got, want)
		}
	}
}

func TestNumbersAreReadAsPostgresWritesThem(t *testing.T) {
	gt := compile(t, `{"gt": ["n", 0]}`)
	for _, s := range []string{"NaN", "Infinity", "1/2", "0x10", "1e99999", ""} {
		if got := gt.eval(view{row: change.Row{"n": text(s)}}); got != unknown {
			t.Errorf("%q: %d, want unknown", s, got)
		}
	}
	for _, s := range []string{"1", "+1", "1.", ".5", "1.5e3", "-0.5E-2"} {
		if _, ok := parseNumber(s); !ok {
			t.Errorf("%q is a number", s)
		}
	}
}

func TestCompileRefuses(t *testing.T) {
	for _, where := range []string{
		`null`, `[]`, `"x"`, `{}`,
		`{"eq": ["a", 1], "neq": ["a", 2]}`,
		`{"like": ["a", "x%"]}`,
		`{"eq": ["a"]}`, `{"eq": ["", 1]}`, `{"eq": [1, 1]}`, `{"eq": ["a", null]}`, `{"eq": ["a", [1]]}`, `{"eq": ["a", {}]}`,
		`{"lt": ["a", true]}`, `{"between": ["a", 1]}`, `{"between": ["a", false, 2]}`,
		`{"in": ["a", []]}`, `{"in": ["a", null]}`, `{"in": ["a", [null]]}`, `{"in": ["a", "x"]}`,
		`{"is_null": ""}`, `{"is_null": null}`, `{"is_null": ["a"]}`,
		`{"and": []}`, `{"or": null}`, `{"and": [{"eq": ["a", 1]}, {}]}`, `{"not": null}`, `{"not": []}`,
		`{"eq": ["a", 1e99999]}`,
	} {
		if _, err := Compile(json.RawMessage(where)); !errors.Is(err, ErrCondition) {
			t.Errorf("%s: %v, want ErrCondition", where, err)
		}
	}
}
