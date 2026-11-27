package visibility

import (
	"errors"
	"testing"
)

func parse(t *testing.T, text string) Snapshot {
	t.Helper()
	s, err := Parse(text)
	if err != nil {
		t.Fatalf("%s: %v", text, err)
	}
	return s
}

func TestSaw(t *testing.T) {
	s := parse(t, "100:110:103,107")
	for xid, want := range map[uint32]bool{99: true, 100: true, 103: false, 105: true, 107: false, 109: true, 110: false, 200: false} {
		if got := s.Saw(xid); got != want {
			t.Errorf("Saw(%d) = %v, want %v", xid, got, want)
		}
	}
}

func TestSawWrapsAround(t *testing.T) {
	// The xid8 4294967300 is xid 4 of the next epoch.
	s := parse(t, "4294967290:4294967300:4294967295")
	for xid, want := range map[uint32]bool{4294967289: true, 4294967295: false, 2: true, 4: false} {
		if got := s.Saw(xid); got != want {
			t.Errorf("Saw(%d) = %v, want %v", xid, got, want)
		}
	}
}

func TestSawAll(t *testing.T) {
	b := parse(t, "100:110:103")
	cases := map[string]bool{
		"100:110:103":     true,
		"103:120:103,115": true,
		"110:120:":        true,
		"100:109:":        false,
		"100:120:105":     false,
		"99:120:99":       false,
	}
	for text, want := range cases {
		if got := parse(t, text).SawAll(b); got != want {
			t.Errorf("%s SawAll %v, want %v", text, got, want)
		}
	}
}

func TestParseRefuses(t *testing.T) {
	for _, text := range []string{"", "1:2", "a:2:", "5:4:", "1:5:7", "1:5:0", "1:5:2,", "1:2:3:4"} {
		if _, err := Parse(text); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: %v", text, err)
		}
	}
}
