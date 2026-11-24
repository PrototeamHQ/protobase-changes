package confirm

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// readStep reads the confirmer's step from the app's server: a WAL segment,
// or half of max_slot_wal_keep_size when that is smaller, so the slot is
// confirmed well before Postgres would invalidate it.
func readStep(ctx context.Context, url string) (pglogrepl.LSN, error) {
	db, err := pgx.Connect(ctx, url)
	if err != nil {
		return 0, fmt.Errorf("connect to read the WAL settings: %w", err)
	}
	defer db.Close(context.Background())
	var segment, keep int64
	err = db.QueryRow(ctx, `SELECT pg_size_bytes(current_setting('wal_segment_size')),
		pg_size_bytes(current_setting('max_slot_wal_keep_size'))`).Scan(&segment, &keep)
	if err != nil {
		return 0, fmt.Errorf("read the WAL settings: %w", err)
	}
	return step(segment, keep), nil
}

// step is a WAL segment, or half of keep when that is smaller. A keep below
// one, such as -1, keeps WAL without bound.
func step(segment, keep int64) pglogrepl.LSN {
	if keep > 0 {
		return pglogrepl.LSN(min(segment, keep/2))
	}
	return pglogrepl.LSN(segment)
}
