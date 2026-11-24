// Package slot prepares the publication and replication slot of one app database.
package slot

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// CheckPublication fails unless the publication publishes every insert,
// update, delete and truncate of all tables.
func CheckPublication(ctx context.Context, conn *pgconn.PgConn, name string) error {
	rows, err := query(ctx, conn, fmt.Sprintf(
		"SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = %s",
		literal(name)))
	if err != nil {
		return fmt.Errorf("read publication %s: %w", name, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("publication %s does not exist: run CREATE PUBLICATION %s FOR ALL TABLES", name, name)
	}
	labels := []string{"FOR ALL TABLES", "insert", "update", "delete", "truncate"}
	for i, label := range labels {
		if string(rows[0][i]) != "t" {
			return fmt.Errorf("publication %s does not publish %s", name, label)
		}
	}
	return nil
}
