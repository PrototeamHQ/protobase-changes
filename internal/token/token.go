// Package token opens the encrypted tokens an app issues for reading its changes.
package token

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/match"
)

// Payload is the shape of each change a token's consumer gets.
type Payload string

const (
	// Thin carries the resource, operation and key: enough to refetch through the app.
	Thin Payload = "thin"
	// Raw adds the rows as the WAL states them and the actor, for replicas.
	Raw Payload = "raw"
)

var (
	// ErrInvalid covers every token that does not open or validate, so a
	// caller learns nothing about which check failed.
	ErrInvalid = errors.New("invalid token")
	// ErrConditions means a subscription's conditions do not compile.
	ErrConditions = errors.New("invalid conditions")
)

// Token is what an opened token grants.
type Token struct {
	Subject string
	Session string
	Expiry  time.Time
	Payload Payload
	// Cursor is the position the app read its snapshot at, if it gave one.
	Cursor *pglogrepl.LSN
	// Subscriptions holds the token's subscriptions, each with its position
	// in the token as its id.
	Subscriptions *match.Index
}

type claims struct {
	Sid     string  `json:"sid"`
	Payload Payload `json:"payload"`
	Cursor  string  `json:"cursor"`
	Subs    []struct {
		Resource string          `json:"resource"`
		Where    json.RawMessage `json:"where"`
	} `json:"subs"`
}

// Open decrypts a compact JWE of alg dir and enc A256GCM under any key of the
// ring, and checks it was issued by issuer, names a session and is valid at now.
func Open(raw string, ring keys.Ring, issuer string, now time.Time) (Token, error) {
	jwe, err := jwt.ParseEncrypted(raw, []jose.KeyAlgorithm{jose.DIRECT}, []jose.ContentEncryption{jose.A256GCM})
	if err != nil {
		return Token{}, ErrInvalid
	}
	var std jwt.Claims
	var own claims
	opened := false
	for _, key := range ring {
		if jwe.Claims(key, &std, &own) == nil {
			opened = true
			break
		}
	}
	// RFC 7519 wants now before exp, which the library's check lets equal.
	if !opened || std.Expiry == nil || !now.Before(std.Expiry.Time()) ||
		std.ValidateWithLeeway(jwt.Expected{Issuer: issuer, Time: now}, 0) != nil {
		return Token{}, ErrInvalid
	}
	if own.Sid == "" || (own.Payload != Thin && own.Payload != Raw) || len(own.Subs) == 0 {
		return Token{}, ErrInvalid
	}

	t := Token{Subject: std.Subject, Session: own.Sid, Expiry: std.Expiry.Time(), Payload: own.Payload}
	subs := make([]match.Subscription, len(own.Subs))
	for i, sub := range own.Subs {
		if sub.Resource == "" {
			return Token{}, ErrInvalid
		}
		subs[i] = match.Subscription{ID: i, Resource: sub.Resource}
		if len(sub.Where) == 0 || string(sub.Where) == "null" {
			continue
		}
		where, err := match.Compile(sub.Where)
		if err != nil {
			return Token{}, ErrConditions
		}
		subs[i].Where = where
	}
	t.Subscriptions = match.NewIndex(subs)
	if own.Cursor != "" {
		lsn, err := pglogrepl.ParseLSN(own.Cursor)
		if err != nil {
			return Token{}, ErrInvalid
		}
		t.Cursor = &lsn
	}
	return t, nil
}
