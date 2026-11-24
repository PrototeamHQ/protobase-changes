// Package reader follows one app database through its logical replication slot.
package reader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/decode"
	"github.com/PrototeamHQ/protobase-changes/internal/slot"
	"github.com/PrototeamHQ/protobase-changes/internal/tail"
)

// statusInterval keeps the connection well inside the default wal_sender_timeout of 60s.
const statusInterval = 10 * time.Second

// Snapshot is the database's initial state, exactly at the slot's first position.
type Snapshot struct {
	Name string
	LSN  pglogrepl.LSN
}

// SnapshotHook receives a new slot's exported snapshot before streaming
// starts, the only time it can be imported. It returns once it has imported
// the snapshot, such as with slot.BeginSnapshot, not after reading the data.
type SnapshotHook func(ctx context.Context, s Snapshot) error

type Reader struct {
	app        config.App
	tail       *tail.Tail
	log        *slog.Logger
	onSnapshot SnapshotHook
	// confirmed is what status updates report as flushed. It only moves through
	// Confirm, so reading alone never lets Postgres drop WAL.
	confirmed atomic.Uint64
}

// New makes a reader of app into t. onSnapshot may be nil.
func New(app config.App, t *tail.Tail, log *slog.Logger, onSnapshot SnapshotHook) *Reader {
	return &Reader{app: app, tail: t, log: log.With("app", app.Name), onSnapshot: onSnapshot}
}

// Confirm lets Postgres drop the WAL up to lsn, once what it holds is stored
// elsewhere. Postgres ignores a position below the slot's current one.
func (r *Reader) Confirm(lsn pglogrepl.LSN) {
	for {
		old := r.confirmed.Load()
		if uint64(lsn) <= old || r.confirmed.CompareAndSwap(old, uint64(lsn)) {
			return
		}
	}
}

// Run streams committed transactions into the tail until ctx ends or the
// connection fails. Run again after a failure resumes after the tail's newest
// transaction, so the tail gets no duplicates.
func (r *Reader) Run(ctx context.Context) error {
	conn, err := connect(ctx, r.app.DatabaseURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if err := slot.CheckPublication(ctx, conn, r.app.Publication); err != nil {
		return err
	}
	s, err := slot.Ensure(ctx, conn, r.app.Slot)
	if err != nil {
		return err
	}
	r.Confirm(s.Confirmed)
	if s.Snapshot != "" && r.onSnapshot != nil {
		if err := r.onSnapshot(ctx, Snapshot{Name: s.Snapshot, LSN: s.Confirmed}); err != nil {
			// A reused slot exports no snapshot, so the new slot goes again and
			// the next run takes the initial state afresh. Nothing streamed from it.
			dropErr := slot.Drop(context.Background(), conn, s.Name)
			return fmt.Errorf("initial snapshot: %w", errors.Join(err, dropErr))
		}
	}

	start := s.Confirmed
	if newest := r.tail.Newest(); newest != nil {
		start = newest.EndLSN
	}
	err = pglogrepl.StartReplication(ctx, conn, s.Name, start, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names '" + r.app.Publication + "'",
			"messages 'true'",
		},
	})
	if err != nil {
		return fmt.Errorf("start replication on slot %s: %w", s.Name, err)
	}
	return r.stream(ctx, conn, start)
}

func connect(ctx context.Context, url string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("database_url: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect for replication: %w", err)
	}
	return conn, nil
}

func (r *Reader) stream(ctx context.Context, conn *pgconn.PgConn, received pglogrepl.LSN) error {
	dec := decode.New(r.log)
	nextStatus := time.Now()
	for {
		if !time.Now().Before(nextStatus) {
			if err := r.sendStatus(ctx, conn, received); err != nil {
				return err
			}
			nextStatus = time.Now().Add(statusInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStatus)
		msg, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(m)
		case *pgproto3.NoticeResponse:
			// Informational, such as logical decoding progress at a verbose client_min_messages.
		case *pgproto3.CopyData:
			pos, reply, err := r.copyData(dec, m.Data)
			if err != nil {
				return err
			}
			received = max(received, pos)
			if reply {
				nextStatus = time.Now()
			}
		default:
			return fmt.Errorf("unexpected replication message %T", msg)
		}
	}
}

// copyData handles one message of the stream, returning the WAL position it
// reaches and whether the server asked for a status update.
func (r *Reader) copyData(dec *decode.Decoder, data []byte) (pglogrepl.LSN, bool, error) {
	if len(data) == 0 {
		return 0, false, errors.New("empty replication message")
	}
	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return 0, false, err
		}
		return ka.ServerWALEnd, ka.ReplyRequested, nil
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return 0, false, err
		}
		msg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return 0, false, fmt.Errorf("parse pgoutput message: %w", err)
		}
		tx, err := dec.Decode(msg)
		if err != nil {
			return 0, false, err
		}
		if tx != nil {
			r.tail.Append(tx)
		}
		return xld.WALStart + pglogrepl.LSN(len(xld.WALData)), false, nil
	}
	return 0, false, fmt.Errorf("unknown replication message type %q", data[0])
}

// sendStatus reports what was received, but as flushed only what Confirm allowed.
// Ending ctx interrupts a blocked write; Run then returns and closes the
// connection, so a partly written frame is never followed by another.
func (r *Reader) sendStatus(ctx context.Context, conn *pgconn.PgConn, received pglogrepl.LSN) error {
	stop := context.AfterFunc(ctx, func() { conn.Conn().SetWriteDeadline(time.Unix(1, 0)) })
	defer stop()
	confirmed := pglogrepl.LSN(r.confirmed.Load())
	err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: max(received, confirmed),
		WALFlushPosition: confirmed,
		WALApplyPosition: confirmed,
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("send status: %w", err)
	}
	return nil
}
