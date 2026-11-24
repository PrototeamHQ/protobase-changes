// Package revoke remembers the sessions an app revoked, until their last
// token expires.
package revoke

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
)

// Prefix is the logical message prefix the app revokes a session with, in
// the transaction that ends the session.
const Prefix = "protobase_revoke"

// Set holds revoked sessions, each until the expiry the app gave it, so it
// never outgrows the sessions whose tokens are still valid. It is safe for
// concurrent use.
type Set struct {
	mu       sync.Mutex
	sessions map[string]revocation
}

// revocation is a session's latest expiry and the commit of the message
// that gave it.
type revocation struct {
	until time.Time
	at    pglogrepl.LSN
}

func New() *Set {
	return &Set{sessions: map[string]revocation{}}
}

// Add revokes the session a message names, as {"sid":"s9","exp":1795480000}
// with exp the Unix time its last token expires, in the transaction
// committed at at. It also forgets every session whose tokens have all
// expired by now.
func (s *Set) Add(content []byte, at pglogrepl.LSN, now time.Time) error {
	var m struct {
		Sid string `json:"sid"`
		Exp *int64 `json:"exp"`
	}
	dec := json.NewDecoder(bytes.NewReader(content))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return errors.New("revocation is not a JSON object of sid and exp")
	}
	if m.Sid == "" || m.Exp == nil {
		return errors.New("revocation lacks sid or exp")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for sid, r := range s.sessions {
		if !r.until.After(now) {
			delete(s.sessions, sid)
		}
	}
	until := time.Unix(*m.Exp, 0)
	if until.After(s.sessions[m.Sid].until) {
		s.sessions[m.Sid] = revocation{until: until, at: at}
	}
	return nil
}

// Oldest returns the earliest commit of a revocation whose tokens may still
// be valid: a stream that starts there reads every such revocation again.
func (s *Set) Oldest(now time.Time) (pglogrepl.LSN, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldest pglogrepl.LSN
	found := false
	for _, r := range s.sessions {
		if r.until.After(now) && (!found || r.at < oldest) {
			oldest, found = r.at, true
		}
	}
	return oldest, found
}

// Revoked reports whether the session was revoked and its tokens may still be valid.
func (s *Set) Revoked(sid string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[sid].until.After(now)
}

// Len is the number of sessions held, expired ones included until the next Add.
func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}
