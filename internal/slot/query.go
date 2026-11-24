package slot

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// query runs one statement over the simple query protocol, the only one a
// replication connection speaks, and returns its rows in text format.
func query(ctx context.Context, conn *pgconn.PgConn, sql string) ([][][]byte, error) {
	results, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}
	return results[0].Rows, nil
}

// literal quotes a string as an SQL literal.
func literal(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
