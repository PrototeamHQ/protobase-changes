package slot

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// BeginSnapshot opens a read-only transaction on a regular connection that
// sees the database as the exported snapshot does. Once it returns, the
// snapshot stays valid for as long as the transaction is open.
func BeginSnapshot(ctx context.Context, conn *pgx.Conn, snapshot string) (pgx.Tx, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+literal(snapshot)); err != nil {
		return nil, fmt.Errorf("import snapshot %s: %w", snapshot, errors.Join(err, tx.Rollback(ctx)))
	}
	return tx, nil
}
