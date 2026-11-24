package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
)

// Definitions returns the app's set token, or "" when it has none.
func (s *SQL) Definitions(ctx context.Context, app string) (string, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, "SELECT token FROM "+s.table("definitions")+" WHERE app = $1", app).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("%s store: %w", s.d.name, err)
	}
	return raw, nil
}

func (s *SQL) PutDefinitions(ctx context.Context, app, raw string) error {
	return s.exec(ctx, "INSERT INTO "+s.table("definitions")+" (app, token) VALUES ($1, $2) "+
		"ON CONFLICT (app) DO UPDATE SET token = excluded.token", app, raw)
}

func (s *SQL) States(ctx context.Context, app string) (map[string]destination.State, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT destination, system_id, timeline, lsn, status, "+
		"batch_end, batch_id, batch_tried, batch_next_ms, batch_error FROM "+s.table("states")+" WHERE app = $1", app)
	if err != nil {
		return nil, fmt.Errorf("%s store: %w", s.d.name, err)
	}
	defer rows.Close()
	out := map[string]destination.State{}
	for rows.Next() {
		name, state, err := scanState(rows)
		if err != nil {
			return nil, fmt.Errorf("%s store: %w", s.d.name, err)
		}
		out[name] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s store: %w", s.d.name, err)
	}
	return out, nil
}

func scanState(rows *sql.Rows) (string, destination.State, error) {
	var name, systemID, lsn, status string
	var timeline int64
	var end, id, batchErr sql.NullString
	var tried, nextMS sql.NullInt64
	if err := rows.Scan(&name, &systemID, &timeline, &lsn, &status, &end, &id, &tried, &nextMS, &batchErr); err != nil {
		return "", destination.State{}, err
	}
	system, err := strconv.ParseUint(systemID, 10, 64)
	if err != nil {
		return "", destination.State{}, fmt.Errorf("destination %s: system_id %q is not a number", name, systemID)
	}
	at, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return "", destination.State{}, fmt.Errorf("destination %s: lsn %q is not an LSN", name, lsn)
	}
	state := destination.State{
		Position: cursor.Position{Epoch: cursor.Epoch{SystemID: system, Timeline: uint32(timeline)}, LSN: at},
		Status:   destination.Status(status),
	}
	if !end.Valid {
		return name, state, nil
	}
	batchEnd, err := pglogrepl.ParseLSN(end.String)
	if err != nil {
		return "", destination.State{}, fmt.Errorf("destination %s: batch_end %q is not an LSN", name, end.String)
	}
	state.Batch = &destination.Batch{End: batchEnd, ID: id.String, Tried: int(tried.Int64), Error: batchErr.String}
	if nextMS.Valid {
		state.Batch.Next = time.UnixMilli(nextMS.Int64)
	}
	return name, state, nil
}

func (s *SQL) PutState(ctx context.Context, app, name string, st destination.State) error {
	var end, id, batchErr, tried, nextMS any
	if b := st.Batch; b != nil {
		end, id, batchErr, tried = b.End.String(), b.ID, b.Error, b.Tried
		if !b.Next.IsZero() {
			nextMS = b.Next.UnixMilli()
		}
	}
	return s.exec(ctx, "INSERT INTO "+s.table("states")+" (app, destination, system_id, timeline, lsn, status, "+
		"batch_end, batch_id, batch_tried, batch_next_ms, batch_error) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) "+
		"ON CONFLICT (app, destination) DO UPDATE SET system_id = excluded.system_id, timeline = excluded.timeline, "+
		"lsn = excluded.lsn, status = excluded.status, batch_end = excluded.batch_end, batch_id = excluded.batch_id, "+
		"batch_tried = excluded.batch_tried, batch_next_ms = excluded.batch_next_ms, batch_error = excluded.batch_error",
		app, name, strconv.FormatUint(st.Position.Epoch.SystemID, 10), int64(st.Position.Epoch.Timeline),
		st.Position.LSN.String(), string(st.Status), end, id, tried, nextMS, batchErr)
}

func (s *SQL) DeleteState(ctx context.Context, app, name string) error {
	return s.exec(ctx, "DELETE FROM "+s.table("states")+" WHERE app = $1 AND destination = $2", app, name)
}

func (s *SQL) PutLetter(ctx context.Context, app, name string, l destination.Letter) error {
	return s.exec(ctx, "INSERT INTO "+s.table("dead_letters")+" (app, destination, batch_id, events, error, tried, failed_at_ms) "+
		"VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (app, destination, batch_id) DO NOTHING",
		app, name, l.BatchID, string(l.Events), l.Error, l.Tried, l.At.UnixMilli())
}

// Letters returns the dead letters of a destination, oldest first.
func (s *SQL) Letters(ctx context.Context, app, name string) ([]destination.Letter, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT batch_id, events, error, tried, failed_at_ms FROM "+s.table("dead_letters")+
		" WHERE app = $1 AND destination = $2 ORDER BY failed_at_ms, batch_id", app, name)
	if err != nil {
		return nil, fmt.Errorf("%s store: %w", s.d.name, err)
	}
	defer rows.Close()
	var out []destination.Letter
	for rows.Next() {
		var l destination.Letter
		var events string
		var at int64
		if err := rows.Scan(&l.BatchID, &events, &l.Error, &l.Tried, &at); err != nil {
			return nil, fmt.Errorf("%s store: %w", s.d.name, err)
		}
		l.Events, l.At = []byte(events), time.UnixMilli(at)
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s store: %w", s.d.name, err)
	}
	return out, nil
}

var _ destination.Store = (*SQL)(nil)
