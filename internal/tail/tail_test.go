package tail

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

type item pglogrepl.LSN

func (i item) Commit() pglogrepl.LSN { return pglogrepl.LSN(i) }

func TestSinceKeepsTheLatest(t *testing.T) {
	tl := New[item](3)
	if _, ok := tl.Newest(); ok {
		t.Fatal("empty tail has a newest item")
	}
	for _, l := range []item{10, 20, 30, 40} {
		tl.Append(l)
	}
	if got, _ := tl.Newest(); got != 40 {
		t.Fatalf("newest %v, want 40", got)
	}
	got, ok := tl.Since(10)
	if !ok || len(got) != 3 || got[0] != 20 || got[2] != 40 {
		t.Fatalf("Since(10) = %v %v", got, ok)
	}
	got, ok = tl.Since(30)
	if !ok || len(got) != 1 || got[0] != 40 {
		t.Fatalf("Since(30) = %v %v", got, ok)
	}
	if got, ok = tl.Since(40); !ok || len(got) != 0 {
		t.Fatalf("Since(40) = %v %v", got, ok)
	}
}

func TestSinceReportsDroppedItems(t *testing.T) {
	tl := New[item](2)
	for _, l := range []item{10, 20, 30} {
		tl.Append(l)
	}
	if _, ok := tl.Since(5); ok {
		t.Fatal("Since(5) is ok though 10 was dropped")
	}
}

func TestRaiseRefusesWhatCameBefore(t *testing.T) {
	tl := New[item](5)
	tl.Raise(100)
	if _, ok := tl.Since(99); ok {
		t.Fatal("Since(99) is ok below the floor")
	}
	tl.Append(110)
	if got, ok := tl.Since(100); !ok || len(got) != 1 {
		t.Fatalf("Since(100) = %v %v", got, ok)
	}
	tl.Raise(50)
	if _, ok := tl.Since(100); !ok {
		t.Fatal("Raise lowered the floor")
	}
}

func TestChangedClosesOnAppend(t *testing.T) {
	tl := New[item](1)
	ch := tl.Changed()
	select {
	case <-ch:
		t.Fatal("closed before any append")
	default:
	}
	tl.Append(1)
	<-ch
}

func TestResetDropsEverythingAndWakesWaiters(t *testing.T) {
	tl := New[item](5)
	tl.Append(10)
	tl.Append(20)
	changed := tl.Changed()
	tl.Reset(100)
	select {
	case <-changed:
	default:
		t.Fatal("Reset did not wake a waiter")
	}
	if _, ok := tl.Since(20); ok {
		t.Fatal("Since(20) is ok below the floor Reset set")
	}
	if got, ok := tl.Since(100); !ok || len(got) != 0 {
		t.Fatalf("Since(100) = %v %v, want nothing held", got, ok)
	}
	if _, ok := tl.Newest(); ok {
		t.Fatal("Reset kept an item")
	}
	tl.Append(110)
	if got, ok := tl.Since(100); !ok || len(got) != 1 || got[0] != 110 {
		t.Fatalf("Since(100) after an append = %v %v", got, ok)
	}
}
