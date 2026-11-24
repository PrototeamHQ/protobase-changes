package revoke

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Load returns the revocations in Table whose tokens may still be valid at
// now, as tx sees them. It fails with ErrSchema when Table is missing.
func Load(ctx context.Context, tx pgx.Tx, now time.Time) ([]Revocation, error) {
	if err := check(ctx, tx); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, "SELECT sid, exp FROM changes.revocations WHERE exp > $1", now.Unix())
	if err != nil {
		return nil, fmt.Errorf("read revocations: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Revocation, error) {
		var sid string
		var exp int64
		err := row.Scan(&sid, &exp)
		return Revocation{Sid: sid, Until: time.Unix(exp, 0)}, err
	})
	if err != nil {
		return nil, fmt.Errorf("read revocations: %w", err)
	}
	return out, nil
}
