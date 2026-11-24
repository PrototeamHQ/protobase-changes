// Package revoke remembers the sessions an app revoked, until their last
// token expires.
package revoke

import (
	"sync"
	"time"
)

// Set holds revoked sessions, each until the expiry the app gave it, so it
// never outgrows the sessions whose tokens are still valid. It is safe for
// concurrent use.
type Set struct {
	mu       sync.Mutex
	sessions map[string]time.Time
}

func New() *Set {
	return &Set{sessions: map[string]time.Time{}}
}

// Add revokes the session sid until its last token expires. It also forgets
// every session whose tokens have all expired by now.
func (s *Set) Add(sid string, until, now time.Time) {
	s.AddAll([]Revocation{{Sid: sid, Until: until}}, now)
}

// AddAll revokes every session rs names, as Add would one by one, but
// forgets the expired sessions only once.
func (s *Set) AddAll(rs []Revocation, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sid, u := range s.sessions {
		if !u.After(now) {
			delete(s.sessions, sid)
		}
	}
	for _, r := range rs {
		if r.Until.After(s.sessions[r.Sid]) {
			s.sessions[r.Sid] = r.Until
		}
	}
}

// Revoked reports whether the session was revoked and its tokens may still be valid.
func (s *Set) Revoked(sid string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[sid].After(now)
}

// Len is the number of sessions held, expired ones included until the next Add.
func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}
