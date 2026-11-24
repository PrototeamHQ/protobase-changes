package revoke

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

func TestAddRevokesUntilExpiry(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	if err := s.Add([]byte(`{"sid":"s9","exp":2000}`), 0, now); err != nil {
		t.Fatal(err)
	}
	if !s.Revoked("s9", now) || s.Revoked("s8", now) {
		t.Fatal("revoked the wrong sessions")
	}
	if s.Revoked("s9", time.Unix(2000, 0)) {
		t.Fatal("still revoked once every token expired")
	}
}

func TestAddForgetsExpiredSessions(t *testing.T) {
	s := New()
	if err := s.Add([]byte(`{"sid":"a","exp":1500}`), 0, time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.Add([]byte(`{"sid":"b","exp":3000}`), 0, time.Unix(2000, 0)); err != nil {
		t.Fatal(err)
	}
	if s.Len() != 1 || !s.Revoked("b", time.Unix(2000, 0)) {
		t.Fatalf("holds %d sessions, want only b", s.Len())
	}
}

func TestAddKeepsTheLaterExpiry(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	for _, m := range []string{`{"sid":"a","exp":3000}`, `{"sid":"a","exp":2000}`} {
		if err := s.Add([]byte(m), 0, now); err != nil {
			t.Fatal(err)
		}
	}
	if !s.Revoked("a", time.Unix(2500, 0)) {
		t.Fatal("an earlier expiry shortened the revocation")
	}
}

func TestAddRefusesMalformed(t *testing.T) {
	for _, m := range []string{``, `[]`, `{"sid":"a"}`, `{"exp":1}`, `{"sid":"","exp":1}`, `{"sid":"a","exp":1,"x":1}`} {
		if err := New().Add([]byte(m), 0, time.Unix(0, 0)); err == nil {
			t.Errorf("%q: added without error", m)
		}
	}
}

func TestOldestIsTheEarliestUnexpiredRevocation(t *testing.T) {
	s := New()
	adds := []struct {
		m  string
		at pglogrepl.LSN
	}{{`{"sid":"a","exp":1500}`, 10}, {`{"sid":"b","exp":3000}`, 20}, {`{"sid":"c","exp":4000}`, 30}}
	for _, a := range adds {
		if err := s.Add([]byte(a.m), a.at, time.Unix(1000, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if at, ok := s.Oldest(time.Unix(1000, 0)); !ok || at != 10 {
		t.Fatalf("got %v, %v, want 10", at, ok)
	}
	if at, ok := s.Oldest(time.Unix(2000, 0)); !ok || at != 20 {
		t.Fatalf("after a expired: got %v, %v, want 20", at, ok)
	}
	if _, ok := s.Oldest(time.Unix(4000, 0)); ok {
		t.Fatal("held a position once every revocation expired")
	}
}
