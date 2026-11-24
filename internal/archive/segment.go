package archive

import (
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// changeOverhead is what a change costs in a segment besides its values.
const changeOverhead = 64

// segment holds the transactions read since the last commit to the archive.
type segment struct {
	started time.Time
	bytes   int64
	txs     []*change.Transaction
}

func (s *segment) add(tx *change.Transaction, now time.Time) {
	if len(s.txs) == 0 {
		s.started = now
	}
	s.txs = append(s.txs, tx)
	for _, c := range tx.Changes {
		s.bytes += changeOverhead + rowBytes(c.Key) + rowBytes(c.OldRow) + rowBytes(c.NewRow)
	}
}

func rowBytes(r change.Row) int64 {
	var n int64
	for name, v := range r {
		n += int64(len(name) + len(v.Text))
	}
	return n
}

// end is the WAL position after the segment's last transaction.
func (s *segment) end() pglogrepl.LSN {
	return s.txs[len(s.txs)-1].EndLSN
}

// event is one change of a table, or a rename of one of its columns, in
// commit order.
type event struct {
	tx *change.Transaction
	// seq is the change's position in its transaction.
	seq    int
	change *change.Change
	// actor is the content of the last protobase_actor message before the change.
	actor  *string
	rename *rename
}

// tables groups the segment's events by table, in commit order.
func (s *segment) tables() (map[change.Table][]event, error) {
	byTable := map[change.Table][]event{}
	for _, tx := range s.txs {
		var actor *string
		next := 0
		for seq := range len(tx.Changes) + 1 {
			for ; next < len(tx.Messages) && tx.Messages[next].Position == seq; next++ {
				m := tx.Messages[next]
				switch m.Prefix {
				case actorPrefix:
					a := string(m.Content)
					actor = &a
				case schemaPrefix:
					r, err := parseRename(m.Content)
					if err != nil {
						return nil, err
					}
					if r != nil {
						byTable[r.table()] = append(byTable[r.table()], event{tx: tx, seq: seq, rename: r})
					}
				}
			}
			if seq < len(tx.Changes) {
				c := &tx.Changes[seq]
				byTable[c.Table] = append(byTable[c.Table], event{tx: tx, seq: seq, change: c, actor: actor})
			}
		}
	}
	return byTable, nil
}
