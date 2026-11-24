package revoke

import (
	"testing"
	"time"
)

func TestAddRevokesUntilExpiry(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	s.Add("s9", time.Unix(2000, 0), now)
	if !s.Revoked("s9", now) || s.Revoked("s8", now) {
		t.Fatal("revoked the wrong sessions")
	}
	if s.Revoked("s9", time.Unix(2000, 0)) {
		t.Fatal("still revoked once every token expired")
	}
}

func TestAddForgetsExpiredSessions(t *testing.T) {
	s := New()
	s.Add("a", time.Unix(1500, 0), time.Unix(1000, 0))
	s.Add("b", time.Unix(3000, 0), time.Unix(2000, 0))
	if s.Len() != 1 || !s.Revoked("b", time.Unix(2000, 0)) {
		t.Fatalf("holds %d sessions, want only b", s.Len())
	}
}

func TestAddKeepsTheLaterExpiry(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	s.Add("a", time.Unix(3000, 0), now)
	s.Add("a", time.Unix(2000, 0), now)
	if !s.Revoked("a", time.Unix(2500, 0)) {
		t.Fatal("an earlier expiry shortened the revocation")
	}
}

func TestAddAllKeepsTheLaterExpiryAndForgetsExpired(t *testing.T) {
	s := New()
	s.Add("old", time.Unix(1500, 0), time.Unix(1000, 0))
	s.AddAll([]Revocation{
		{Sid: "a", Until: time.Unix(3000, 0)},
		{Sid: "a", Until: time.Unix(2500, 0)},
		{Sid: "b", Until: time.Unix(2500, 0)},
	}, time.Unix(2000, 0))
	if s.Len() != 2 || !s.Revoked("a", time.Unix(2800, 0)) || !s.Revoked("b", time.Unix(2400, 0)) {
		t.Fatalf("holds %d sessions, want a until 3000 and b", s.Len())
	}
}
