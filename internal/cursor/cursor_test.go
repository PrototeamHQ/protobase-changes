package cursor

import (
	"bytes"
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/keys"
)

func codec(t *testing.T, ring ...[]byte) *Codec {
	t.Helper()
	c, err := New(ring)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func key(b byte) []byte { return bytes.Repeat([]byte{b}, keys.Size) }

var at = Position{Epoch: Epoch{SystemID: 7412345678901234567, Timeline: 3}, LSN: 0x1A2B3C}

func TestSealOpens(t *testing.T) {
	c := codec(t, key(1))
	s := c.Seal(at)
	got, err := c.Open(s)
	if err != nil {
		t.Fatal(err)
	}
	if got != at {
		t.Fatalf("opened %+v, want %+v", got, at)
	}
	if c.Seal(at) == s {
		t.Fatal("two seals of one position are equal, so a nonce was reused")
	}
}

func TestOpenWithAnOlderKey(t *testing.T) {
	old := codec(t, key(1))
	rotated := codec(t, key(2), key(1))
	if got, err := rotated.Open(old.Seal(at)); err != nil || got != at {
		t.Fatalf("opened %+v, %v", got, err)
	}
	if _, err := old.Open(rotated.Seal(at)); err != ErrInvalid {
		t.Fatalf("a cursor of a key the codec lacks opened: %v", err)
	}
}

func TestOpenRefusesTampering(t *testing.T) {
	c := codec(t, key(1))
	s := []byte(c.Seal(at))
	s[len(s)/2] ^= 1
	for _, input := range []string{string(s), "", "!!", "AAAA"} {
		if _, err := c.Open(input); err != ErrInvalid {
			t.Errorf("%q: %v", input, err)
		}
	}
}

func TestIDIsStableAndOpaque(t *testing.T) {
	c := codec(t, key(1))
	if c.ID(at) != c.ID(at) {
		t.Fatal("the id of one position changed")
	}
	later := at
	later.LSN++
	if c.ID(at) == c.ID(later) {
		t.Fatal("two positions share an id")
	}
	if codec(t, key(2)).ID(at) == c.ID(at) {
		t.Fatal("another key gives the same id")
	}
}

func TestIDSurvivesAddingAKey(t *testing.T) {
	before := codec(t, key(1)).ID(at)
	if after := codec(t, key(2), key(1)).ID(at); after != before {
		t.Fatal("adding a new key changed the id of a position")
	}
}
