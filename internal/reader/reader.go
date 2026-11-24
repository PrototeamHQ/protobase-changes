// Package reader follows one app database through its logical replication slot.
package reader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/config"
	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/decode"
	"github.com/PrototeamHQ/protobase-changes/internal/slot"
)

// statusInterval keeps the connection well inside the default wal_sender_timeout
// of 60s; a shorter timeout gets a status update every half of it.
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

// Sink takes in the committed transactions a reader streams.
type Sink interface {
	// Resume returns where a stream of epoch starts, given the slot's
	// position: after what the sink already holds, so it gets no duplicates.
	// caughtUp is where the server's WAL ended when the stream connected.
	Resume(epoch cursor.Epoch, from, caughtUp pglogrepl.LSN) (pglogrepl.LSN, error)
	// Reset drops what the sink holds and starts it at from, the start of a
	// new slot: positions before it cannot be resumed from.
	Reset(from pglogrepl.LSN)
	Append(tx *change.Transaction)
	// Decoded reports that the server sent every transaction committed before lsn.
	Decoded(lsn pglogrepl.LSN)
}

type Reader struct {
	app        config.App
	sink       Sink
	log        *slog.Logger
	onSnapshot SnapshotHook
	throttle   throttle
	// confirmed is what status updates report as flushed. It only moves through
	// Confirm, so reading alone never lets Postgres drop WAL.
	confirmed atomic.Uint64
}

// New makes a reader of app into sink. onSnapshot may be nil.
func New(app config.App, sink Sink, log *slog.Logger, onSnapshot SnapshotHook) *Reader {
	return &Reader{
		app: app, sink: sink, log: log.With("app", app.Name), onSnapshot: onSnapshot,
		throttle: newThrottle(app.Quotas.ChangesPerSecond),
	}
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

// Run streams committed transactions into the sink until ctx ends or the
// connection fails. Run again after a failure resumes where the sink says.
func (r *Reader) Run(ctx context.Context) error {
	conn, err := connect(ctx, r.app.DatabaseURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if err := slot.CheckPublication(ctx, conn, r.app.Publication); err != nil {
		return err
	}
	epoch, caughtUp, err := identify(ctx, conn)
	if err != nil {
		return err
	}
	s, err := slot.Ensure(ctx, conn, r.app.Slot)
	if err != nil {
		return err
	}
	if s.Recreated {
		r.log.Warn("the slot was invalidated and is created again; earlier cursors can no longer resume", "slot", s.Name, "lsn", s.Confirmed.String())
	}
	if s.Snapshot != "" {
		r.sink.Reset(s.Confirmed)
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

	interval, err := statusEvery(ctx, conn)
	if err != nil {
		return err
	}
	start, err := r.sink.Resume(epoch, s.Confirmed, caughtUp)
	if err != nil {
		return err
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
	return r.stream(ctx, conn, start, interval)
}

// identify returns the epoch of the database's WAL, its cluster and
// timeline, and where its WAL ends now.
func identify(ctx context.Context, conn *pgconn.PgConn) (cursor.Epoch, pglogrepl.LSN, error) {
	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return cursor.Epoch{}, 0, fmt.Errorf("identify system: %w", err)
	}
	id, err := strconv.ParseUint(sys.SystemID, 10, 64)
	if err != nil {
		return cursor.Epoch{}, 0, fmt.Errorf("identify system: system id %q: %w", sys.SystemID, err)
	}
	return cursor.Epoch{SystemID: id, Timeline: uint32(sys.Timeline)}, sys.XLogPos, nil
}

// statusEvery returns how often to send status updates: every statusInterval,
// or every half of a shorter wal_sender_timeout, so the server keeps the
// connection even while the stream reads nothing.
func statusEvery(ctx context.Context, conn *pgconn.PgConn) (time.Duration, error) {
	results, err := conn.Exec(ctx, "SELECT setting FROM pg_settings WHERE name = 'wal_sender_timeout'").ReadAll()
	if err != nil {
		return 0, fmt.Errorf("read wal_sender_timeout: %w", err)
	}
	ms, err := strconv.Atoi(string(results[0].Rows[0][0]))
	if err != nil {
		return 0, fmt.Errorf("read wal_sender_timeout: %w", err)
	}
	if ms <= 0 {
		return statusInterval, nil
	}
	return min(statusInterval, time.Duration(ms)*time.Millisecond/2), nil
}

func connect(ctx context.Context, url string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("database_url: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	// Values arrive in the session's text format: ISO dates, times in UTC and
	// bytea in hex, whatever the server's defaults.
	cfg.RuntimeParams["DateStyle"] = "ISO, MDY"
	cfg.RuntimeParams["TimeZone"] = "UTC"
	cfg.RuntimeParams["bytea_output"] = "hex"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect for replication: %w", err)
	}
	return conn, nil
}

func (r *Reader) stream(ctx context.Context, conn *pgconn.PgConn, received pglogrepl.LSN, interval time.Duration) error {
	dec := decode.New(r.log)
	nextStatus := time.Now()
	// pending is a transaction the app's quota holds back until readOn, when
	// the feed takes it in whole and the stream reads on.
	var pending *change.Transaction
	readOn := time.Now()
	for {
		if !time.Now().Before(nextStatus) {
			if err := r.sendStatus(ctx, conn, received); err != nil {
				return err
			}
			nextStatus = time.Now().Add(interval)
		}
		if wait := time.Until(readOn); wait > 0 {
			// Held back by the quota, so the server's requests for a status
			// update go unread: the interval alone keeps the connection.
			// Postgres keeps the WAL meanwhile.
			if err := sleep(ctx, min(wait, time.Until(nextStatus))); err != nil {
				return err
			}
			continue
		}
		if pending != nil {
			r.sink.Append(pending)
			pending = nil
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
			pos, reply, tx, err := r.copyData(dec, m.Data)
			if err != nil {
				return err
			}
			if tx != nil {
				pending = tx
				readOn = r.throttle.take(len(tx.Changes), time.Now())
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
// reaches, whether the server asked for a status update and the transaction
// it completed, if any.
func (r *Reader) copyData(dec *decode.Decoder, data []byte) (pglogrepl.LSN, bool, *change.Transaction, error) {
	if len(data) == 0 {
		return 0, false, nil, errors.New("empty replication message")
	}
	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return 0, false, nil, err
		}
		// A logical walsender's keepalive carries where it has decoded and
		// sent up to, so every transaction committed before it has come.
		r.sink.Decoded(ka.ServerWALEnd)
		return ka.ServerWALEnd, ka.ReplyRequested, nil, nil
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return 0, false, nil, err
		}
		msg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return 0, false, nil, fmt.Errorf("parse pgoutput message: %w", err)
		}
		tx, err := dec.Decode(msg)
		if err != nil {
			return 0, false, nil, err
		}
		return xld.WALStart + pglogrepl.LSN(len(xld.WALData)), false, tx, nil
	}
	return 0, false, nil, fmt.Errorf("unknown replication message type %q", data[0])
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

// sleep waits for d, or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
