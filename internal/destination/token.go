package destination

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

var (
	// ErrInvalidToken covers every destination token that does not open or
	// validate, so a caller learns nothing about which check failed.
	ErrInvalidToken = errors.New("invalid token")
	// ErrDefinitions means the token opened but its destinations are invalid.
	ErrDefinitions = errors.New("invalid destinations")
)

// Set is the app's destinations, as one token defines them all.
type Set struct {
	// IssuedAt orders sets: a set issued before the one kept never replaces it.
	IssuedAt     time.Time
	Destinations []*Definition
}

// commandClaims are what destination tokens hold beside the standard claims.
// A live token's sid never appears, so neither kind opens as the other.
type commandClaims struct {
	Sid          *string          `json:"sid"`
	Destinations *json.RawMessage `json:"destinations"`
	Resume       string           `json:"resume"`
}

// OpenSet opens a token that defines the app's destinations: issued by
// issuer, with an iat and without an expiry, since it stays until a later
// set replaces it.
func OpenSet(raw string, ring keys.Ring, issuer string) (Set, error) {
	var std jwt.Claims
	var own commandClaims
	if !token.Decrypt(raw, ring, &std, &own) || std.Issuer != issuer || std.IssuedAt == nil || std.Expiry != nil ||
		own.Sid != nil || own.Destinations == nil || own.Resume != "" {
		return Set{}, ErrInvalidToken
	}
	defs, err := parseDefinitions(*own.Destinations)
	if err != nil {
		return Set{}, fmt.Errorf("%w: %w", ErrDefinitions, err)
	}
	return Set{IssuedAt: std.IssuedAt.Time(), Destinations: defs}, nil
}

// OpenResume opens a token that resumes one destination and returns its
// name: issued by issuer and valid at now.
func OpenResume(raw string, ring keys.Ring, issuer string, now time.Time) (string, error) {
	var std jwt.Claims
	var own commandClaims
	// RFC 7519 wants now before exp, which the library's check lets equal.
	if !token.Decrypt(raw, ring, &std, &own) || std.Expiry == nil || !now.Before(std.Expiry.Time()) ||
		std.ValidateWithLeeway(jwt.Expected{Issuer: issuer, Time: now}, 0) != nil ||
		own.Sid != nil || own.Destinations != nil || own.Resume == "" {
		return "", ErrInvalidToken
	}
	return own.Resume, nil
}
