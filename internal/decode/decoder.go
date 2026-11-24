// Package decode turns pgoutput messages into committed transactions.
package decode

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// MessagePrefix starts the prefix of every logical message the reader keeps,
// such as protobase_actor and protobase_schema.
const MessagePrefix = "protobase_"

// Decoder follows one pgoutput stream. It is not safe for concurrent use.
type Decoder struct {
	log       *slog.Logger
	relations map[uint32]relationDef
	open      *change.Transaction
}

// relationDef is a table definition with the columns every change under it shares.
type relationDef struct {
	*pglogrepl.RelationMessage
	columns []change.Column
}

func New(log *slog.Logger) *Decoder {
	return &Decoder{log: log, relations: map[uint32]relationDef{}}
}

// Open reports whether a transaction has begun and not yet committed.
func (d *Decoder) Open() bool { return d.open != nil }

// Decode takes the next message of the stream and returns the transaction it
// commits, or nil while a transaction is still open.
func (d *Decoder) Decode(msg pglogrepl.Message) (*change.Transaction, error) {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		d.relations[m.RelationID] = relationDef{RelationMessage: m, columns: columns(m)}
		return nil, nil
	case *pglogrepl.BeginMessage:
		if d.open != nil {
			return nil, fmt.Errorf("begin of transaction %d inside transaction %d", m.Xid, d.open.Xid)
		}
		d.open = &change.Transaction{Xid: m.Xid, CommitLSN: m.FinalLSN, CommitTime: m.CommitTime}
		return nil, nil
	case *pglogrepl.CommitMessage:
		return d.commit(m)
	case *pglogrepl.LogicalDecodingMessage:
		return nil, d.message(m)
	case *pglogrepl.InsertMessage:
		return nil, d.insert(m)
	case *pglogrepl.UpdateMessage:
		return nil, d.update(m)
	case *pglogrepl.DeleteMessage:
		return nil, d.delete(m)
	case *pglogrepl.TruncateMessage:
		return nil, d.truncate(m)
	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// Values arrive in text format, so type and origin details add nothing.
		return nil, nil
	}
	// Streamed in-progress transactions are never requested.
	return nil, fmt.Errorf("unexpected pgoutput message %s", msg.Type())
}

func (d *Decoder) commit(m *pglogrepl.CommitMessage) (*change.Transaction, error) {
	if d.open == nil {
		return nil, fmt.Errorf("commit at %s outside a transaction", m.CommitLSN)
	}
	tx := d.open
	d.open = nil
	tx.CommitLSN = m.CommitLSN
	tx.EndLSN = m.TransactionEndLSN
	tx.CommitTime = m.CommitTime
	return tx, nil
}

func (d *Decoder) message(m *pglogrepl.LogicalDecodingMessage) error {
	if !strings.HasPrefix(m.Prefix, MessagePrefix) {
		return nil
	}
	if !m.Transactional {
		// It arrives even when its transaction rolls back, so it cannot be attached.
		d.log.Warn("ignored a non-transactional logical message", "prefix", m.Prefix, "lsn", m.LSN.String())
		return nil
	}
	tx, err := d.transaction("message " + m.Prefix)
	if err != nil {
		return err
	}
	content := append([]byte(nil), m.Content...)
	tx.Messages = append(tx.Messages, change.Message{Prefix: m.Prefix, Content: content, Position: len(tx.Changes)})
	return nil
}

func (d *Decoder) insert(m *pglogrepl.InsertMessage) error {
	rel, tx, err := d.target(m.RelationID, "insert")
	if err != nil {
		return err
	}
	newRow := row(rel.RelationMessage, m.Tuple, false)
	tx.Changes = append(tx.Changes, change.Change{
		Table: table(rel.RelationMessage), Operation: change.Insert, Columns: rel.columns, Key: key(rel.RelationMessage, newRow), NewRow: newRow,
	})
	return nil
}

func (d *Decoder) update(m *pglogrepl.UpdateMessage) error {
	rel, tx, err := d.target(m.RelationID, "update")
	if err != nil {
		return err
	}
	c := change.Change{Table: table(rel.RelationMessage), Operation: change.Update, Columns: rel.columns, NewRow: row(rel.RelationMessage, m.NewTuple, false)}
	c.OldRowKind, c.OldRow = oldRow(rel.RelationMessage, m.OldTupleType, m.OldTuple)
	c.Key = key(rel.RelationMessage, c.NewRow)
	if c.OldRowKind != change.NoOldRow {
		c.Key = key(rel.RelationMessage, c.OldRow)
	}
	tx.Changes = append(tx.Changes, c)
	return nil
}

func (d *Decoder) delete(m *pglogrepl.DeleteMessage) error {
	rel, tx, err := d.target(m.RelationID, "delete")
	if err != nil {
		return err
	}
	c := change.Change{Table: table(rel.RelationMessage), Operation: change.Delete, Columns: rel.columns}
	c.OldRowKind, c.OldRow = oldRow(rel.RelationMessage, m.OldTupleType, m.OldTuple)
	c.Key = key(rel.RelationMessage, c.OldRow)
	tx.Changes = append(tx.Changes, c)
	return nil
}

func (d *Decoder) truncate(m *pglogrepl.TruncateMessage) error {
	tx, err := d.transaction("truncate")
	if err != nil {
		return err
	}
	for _, id := range m.RelationIDs {
		rel, ok := d.relations[id]
		if !ok {
			return fmt.Errorf("truncate of unknown relation %d", id)
		}
		tx.Changes = append(tx.Changes, change.Change{
			Table:           table(rel.RelationMessage),
			Operation:       change.Truncate,
			Columns:         rel.columns,
			Cascade:         m.Option&pglogrepl.TruncateOptionCascade != 0,
			RestartIdentity: m.Option&pglogrepl.TruncateOptionRestartIdentity != 0,
		})
	}
	return nil
}

func (d *Decoder) transaction(what string) (*change.Transaction, error) {
	if d.open == nil {
		return nil, fmt.Errorf("%s outside a transaction", what)
	}
	return d.open, nil
}

func (d *Decoder) target(relationID uint32, what string) (relationDef, *change.Transaction, error) {
	tx, err := d.transaction(what)
	if err != nil {
		return relationDef{}, nil, err
	}
	rel, ok := d.relations[relationID]
	if !ok {
		return relationDef{}, nil, fmt.Errorf("%s of unknown relation %d", what, relationID)
	}
	return rel, tx, nil
}
