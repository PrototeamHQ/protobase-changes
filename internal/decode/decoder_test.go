package decode

import (
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

func relation(identity uint8) *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID: 7, Namespace: "public", RelationName: "messages", ReplicaIdentity: identity,
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", Flags: 1}, {Name: "body"}, {Name: "note"}},
	}
}

func tuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{ColumnNum: uint16(len(cols)), Columns: cols}
}

func text(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}

var (
	null      = &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
	unchanged = &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
)

func v(s string) change.Value { return change.Value{Kind: change.Text, Text: s} }

func feed(t *testing.T, msgs ...pglogrepl.Message) []*change.Transaction {
	t.Helper()
	d := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	var txs []*change.Transaction
	for _, m := range msgs {
		tx, err := d.Decode(m)
		if err != nil {
			t.Fatal(err)
		}
		if tx != nil {
			txs = append(txs, tx)
		}
	}
	return txs
}

var commitTime = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

func begin() *pglogrepl.BeginMessage {
	return &pglogrepl.BeginMessage{Xid: 42, FinalLSN: 100, CommitTime: commitTime}
}

func commit() *pglogrepl.CommitMessage {
	return &pglogrepl.CommitMessage{CommitLSN: 100, TransactionEndLSN: 120, CommitTime: commitTime}
}

func TestDecodesATransaction(t *testing.T) {
	txs := feed(t,
		relation('d'),
		begin(),
		&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: "protobase_actor", Content: []byte(`{"sub":"u42"}`)},
		&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: "other_tool", Content: []byte("x")},
		&pglogrepl.InsertMessage{RelationID: 7, Tuple: tuple(text("1"), text("hi"), null)},
		&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: "protobase_schema", Content: []byte(`{"rename":"x"}`)},
		&pglogrepl.UpdateMessage{RelationID: 7, NewTuple: tuple(text("1"), text("hey"), unchanged)},
		&pglogrepl.UpdateMessage{RelationID: 7, OldTupleType: 'K', OldTuple: tuple(text("1"), null, null), NewTuple: tuple(text("2"), text("hey"), null)},
		&pglogrepl.DeleteMessage{RelationID: 7, OldTupleType: 'K', OldTuple: tuple(text("2"), null, null)},
		&pglogrepl.TruncateMessage{RelationNum: 1, Option: pglogrepl.TruncateOptionCascade, RelationIDs: []uint32{7}},
		commit(),
	)
	if len(txs) != 1 {
		t.Fatalf("got %d transactions, want 1", len(txs))
	}
	tx := txs[0]
	if tx.Xid != 42 || tx.CommitLSN != 100 || tx.EndLSN != 120 || !tx.CommitTime.Equal(commitTime) {
		t.Fatalf("transaction header %+v", tx)
	}
	table := change.Table{Schema: "public", Name: "messages"}
	want := []change.Change{
		{Table: table, Operation: change.Insert, Key: change.Row{"id": v("1")},
			NewRow: change.Row{"id": v("1"), "body": v("hi"), "note": {Kind: change.Null}}},
		{Table: table, Operation: change.Update, Key: change.Row{"id": v("1")},
			NewRow: change.Row{"id": v("1"), "body": v("hey"), "note": {Kind: change.Unchanged}}},
		{Table: table, Operation: change.Update, Key: change.Row{"id": v("1")},
			OldRowKind: change.OldKeyOnly, OldRow: change.Row{"id": v("1")},
			NewRow: change.Row{"id": v("2"), "body": v("hey"), "note": {Kind: change.Null}}},
		{Table: table, Operation: change.Delete, Key: change.Row{"id": v("2")},
			OldRowKind: change.OldKeyOnly, OldRow: change.Row{"id": v("2")}},
		{Table: table, Operation: change.Truncate, Cascade: true},
	}
	if !reflect.DeepEqual(tx.Changes, want) {
		t.Fatalf("changes\n got %+v\nwant %+v", tx.Changes, want)
	}
	wantMessages := []change.Message{
		{Prefix: "protobase_actor", Content: []byte(`{"sub":"u42"}`), Position: 0},
		{Prefix: "protobase_schema", Content: []byte(`{"rename":"x"}`), Position: 1},
	}
	if !reflect.DeepEqual(tx.Messages, wantMessages) {
		t.Fatalf("messages\n got %+v\nwant %+v", tx.Messages, wantMessages)
	}
}

func TestFullReplicaIdentityHasNoKey(t *testing.T) {
	rel := relation('f')
	for _, c := range rel.Columns {
		c.Flags = 1
	}
	txs := feed(t, rel, begin(),
		&pglogrepl.DeleteMessage{RelationID: 7, OldTupleType: 'O', OldTuple: tuple(text("3"), text("bye"), null)},
		commit())
	c := txs[0].Changes[0]
	if c.Key != nil || c.OldRowKind != change.OldFull || len(c.OldRow) != 3 {
		t.Fatalf("change %+v", c)
	}
}

func TestIgnoresNonTransactionalMessages(t *testing.T) {
	txs := feed(t, &pglogrepl.LogicalDecodingMessage{Prefix: "protobase_actor", Content: []byte("x")})
	if len(txs) != 0 {
		t.Fatalf("got %d transactions", len(txs))
	}
}

func TestRefusesChangesOutsideATransaction(t *testing.T) {
	d := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := d.Decode(relation('d')); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Decode(&pglogrepl.InsertMessage{RelationID: 7, Tuple: tuple(text("1"), null, null)}); err == nil {
		t.Fatal("insert outside a transaction decoded")
	}
	if _, err := d.Decode(commit()); err == nil {
		t.Fatal("commit outside a transaction decoded")
	}
}

func TestRefusesUnknownRelations(t *testing.T) {
	d := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := d.Decode(begin()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Decode(&pglogrepl.DeleteMessage{RelationID: 9}); err == nil {
		t.Fatal("delete of an unknown relation decoded")
	}
}
