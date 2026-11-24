package revoke

import (
	"testing"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

func row(sid, exp string) change.Row {
	return change.Row{"sid": {Kind: change.Text, Text: sid}, "exp": {Kind: change.Text, Text: exp}}
}

func TestAnInsertIntoTheTableRevokes(t *testing.T) {
	r, ok, err := FromChange(change.Change{Table: Table, Operation: change.Insert, NewRow: row("s9", "2000")})
	if err != nil || !ok || r != (Revocation{Sid: "s9", Until: time.Unix(2000, 0)}) {
		t.Fatalf("got %+v %v %v", r, ok, err)
	}
}

// Only an insert revokes: nothing the app or anyone else does to the rows
// later lifts a revocation, and a table of that name elsewhere is no revocation.
func TestOtherChangesDoNotRevoke(t *testing.T) {
	cases := map[string]change.Change{
		"update":       {Table: Table, Operation: change.Update, NewRow: row("s9", "2000")},
		"delete":       {Table: Table, Operation: change.Delete, OldRow: row("s9", "2000")},
		"truncate":     {Table: Table, Operation: change.Truncate},
		"other schema": {Table: change.Table{Schema: "public", Name: "revocations"}, Operation: change.Insert, NewRow: row("s9", "2000")},
		"other table":  {Table: change.Table{Schema: "changes", Name: "sessions"}, Operation: change.Insert, NewRow: row("s9", "2000")},
	}
	for name, c := range cases {
		if _, ok, err := FromChange(c); ok || err != nil {
			t.Errorf("%s: revoked %v, error %v", name, ok, err)
		}
	}
}

func TestAMalformedRowIsAnError(t *testing.T) {
	cases := map[string]change.Row{
		"empty sid":   row("", "2000"),
		"no exp":      {"sid": {Kind: change.Text, Text: "s9"}},
		"null exp":    {"sid": {Kind: change.Text, Text: "s9"}, "exp": {Kind: change.Null}},
		"exp no time": row("s9", "soon"),
	}
	for name, r := range cases {
		if _, ok, err := FromChange(change.Change{Table: Table, Operation: change.Insert, NewRow: r}); ok || err == nil {
			t.Errorf("%s: revoked %v, error %v", name, ok, err)
		}
	}
}
