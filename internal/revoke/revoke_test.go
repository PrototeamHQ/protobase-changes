package revoke

import (
	"testing"
	"time"
)

func TestAddRevokesUntilExpiry(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	if err := s.Add([]byte(`{"sid":"s9","exp":2000}`), now); err != nil {
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
	if err := s.Add([]byte(`{"sid":"a","exp":1500}`), time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.Add([]byte(`{"sid":"b","exp":3000}`), time.Unix(2000, 0)); err != nil {
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
		if err := s.Add([]byte(m), now); err != nil {
			t.Fatal(err)
		}
	}
	if !s.Revoked("a", time.Unix(2500, 0)) {
		t.Fatal("an earlier expiry shortened the revocation")
	}
}

func TestAddRefusesMalformed(t *testing.T) {
	for _, m := range []string{``, `[]`, `{"sid":"a"}`, `{"exp":1}`, `{"sid":"","exp":1}`, `{"sid":"a","exp":1,"x":1}`} {
		if err := New().Add([]byte(m), time.Unix(0, 0)); err == nil {
			t.Errorf("%q: added without error", m)
		}
	}
}
