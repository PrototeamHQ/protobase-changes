package slot

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// Plugin is the logical decoding output plugin every slot uses.
const Plugin = "pgoutput"

// Slot is a logical replication slot ready to stream from.
type Slot struct {
	Name string
	// Confirmed is the position the slot was confirmed up to when it was opened.
	Confirmed pglogrepl.LSN
	// Snapshot is set only when the slot was just created: it names the
	// exported snapshot that shows the database exactly at Confirmed.
	Snapshot string
}

// Ensure reuses the slot when it exists, and creates it with an exported
// snapshot when it does not. A slot of another plugin or database is never
// dropped, since that would lose its position: Ensure fails instead.
func Ensure(ctx context.Context, conn *pgconn.PgConn, name string) (Slot, error) {
	rows, err := query(ctx, conn, fmt.Sprintf(
		"SELECT slot_type, plugin, database = current_database(), confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = %s",
		literal(name)))
	if err != nil {
		return Slot{}, fmt.Errorf("read slot %s: %w", name, err)
	}
	if len(rows) == 0 {
		return create(ctx, conn, name)
	}
	row := rows[0]
	if string(row[0]) != "logical" || string(row[1]) != Plugin {
		return Slot{}, fmt.Errorf("slot %s exists but is not a logical %s slot", name, Plugin)
	}
	if string(row[2]) != "t" {
		return Slot{}, fmt.Errorf("slot %s exists on another database", name)
	}
	confirmed, err := pglogrepl.ParseLSN(string(row[3]))
	if err != nil {
		return Slot{}, fmt.Errorf("slot %s confirmed_flush_lsn: %w", name, err)
	}
	return Slot{Name: name, Confirmed: confirmed}, nil
}

func create(ctx context.Context, conn *pgconn.PgConn, name string) (Slot, error) {
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, name, Plugin, pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		return Slot{}, fmt.Errorf("create slot %s: %w", name, err)
	}
	confirmed, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return Slot{}, fmt.Errorf("slot %s consistent point: %w", name, err)
	}
	return Slot{Name: name, Confirmed: confirmed, Snapshot: res.SnapshotName}, nil
}

// Drop removes a slot this connection does not stream from.
func Drop(ctx context.Context, conn *pgconn.PgConn, name string) error {
	if err := pglogrepl.DropReplicationSlot(ctx, conn, name, pglogrepl.DropReplicationSlotOptions{}); err != nil {
		return fmt.Errorf("drop slot %s: %w", name, err)
	}
	return nil
}
