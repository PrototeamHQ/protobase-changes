package revoke

import (
	"errors"
	"strconv"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// Table is where the app revokes a session: one row of sid and exp, the Unix
// time its last token expires, inserted in the transaction that ends the
// session. Postgres keeps the rows when a slot loses its WAL, so a stream of
// a new slot reads them before it serves. It replaced a protobase_revoke
// logical message before any release and before any app emitted one, so no
// revocation in force exists only as a message, and none is read: a message
// alone would be lost with the WAL again.
var Table = change.Table{Schema: "changes", Name: "revocations"}

// Revocation is one row of Table.
type Revocation struct {
	Sid   string
	Until time.Time
}

// FromChange returns the revocation a change records. Only an insert into
// Table revokes: an update, a delete or a truncate of it revokes nothing and
// lifts nothing.
func FromChange(c change.Change) (Revocation, bool, error) {
	if c.Table != Table || c.Operation != change.Insert {
		return Revocation{}, false, nil
	}
	sid, exp := c.NewRow["sid"], c.NewRow["exp"]
	if sid.Kind != change.Text || sid.Text == "" || exp.Kind != change.Text {
		return Revocation{}, false, errors.New("revocation lacks sid or exp")
	}
	unix, err := strconv.ParseInt(exp.Text, 10, 64)
	if err != nil {
		return Revocation{}, false, errors.New("revocation's exp is not a Unix time")
	}
	return Revocation{Sid: sid.Text, Until: time.Unix(unix, 0)}, true, nil
}
