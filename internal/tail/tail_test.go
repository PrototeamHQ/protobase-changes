package tail

import (
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

func tx(lsn pglogrepl.LSN) *change.Transaction {
	return &change.Transaction{CommitLSN: lsn, EndLSN: lsn + 1}
}

func lsns(txs []*change.Transaction) []pglogrepl.LSN {
	var out []pglogrepl.LSN
	for _, t := range txs {
		out = append(out, t.CommitLSN)
	}
	return out
}

func TestSinceKeepsTheLatest(t *testing.T) {
	tl := New(3)
	if tl.Newest() != nil {
		t.Fatal("empty tail has a newest transaction")
	}
	for _, l := range []pglogrepl.LSN{10, 20, 30, 40} {
		tl.Append(tx(l))
	}
	if got := tl.Newest().CommitLSN; got != 40 {
		t.Fatalf("newest %v, want 40", got)
	}
	got, ok := tl.Since(10)
	if !ok || len(got) != 3 || got[0].CommitLSN != 20 || got[2].CommitLSN != 40 {
		t.Fatalf("Since(10) = %v %v", lsns(got), ok)
	}
	got, ok = tl.Since(30)
	if !ok || len(got) != 1 || got[0].CommitLSN != 40 {
		t.Fatalf("Since(30) = %v %v", lsns(got), ok)
	}
	if got, ok = tl.Since(40); !ok || len(got) != 0 {
		t.Fatalf("Since(40) = %v %v", lsns(got), ok)
	}
}

func TestSinceReportsDroppedTransactions(t *testing.T) {
	tl := New(2)
	for _, l := range []pglogrepl.LSN{10, 20, 30} {
		tl.Append(tx(l))
	}
	if _, ok := tl.Since(5); ok {
		t.Fatal("Since(5) is ok though 10 was dropped")
	}
}

func TestChangedClosesOnAppend(t *testing.T) {
	tl := New(1)
	ch := tl.Changed()
	select {
	case <-ch:
		t.Fatal("closed before any append")
	default:
	}
	tl.Append(tx(1))
	<-ch
}
