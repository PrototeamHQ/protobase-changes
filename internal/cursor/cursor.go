// Package cursor seals positions in an app's change stream into opaque
// cursors, so consumers can resume from them but can neither read nor forge them.
package cursor

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"

	"github.com/jackc/pglogrepl"

	"github.com/PrototeamHQ/protobase-changes/internal/keys"
)

// Epoch names the WAL history positions belong to: the database cluster and
// its timeline. A position of another epoch means nothing in this one.
type Epoch struct {
	SystemID uint64
	Timeline uint32
}

// Position is a place in the stream: after the transaction that committed at LSN.
type Position struct {
	Epoch Epoch
	LSN   pglogrepl.LSN
}

// version leads the sealed plaintext, so the format can change later.
const version = 1

const plaintextSize = 1 + 8 + 4 + 8

// additionalData binds a sealed cursor to its purpose.
var additionalData = []byte("protobase-changes cursor")

var ErrInvalid = errors.New("invalid cursor")

// Codec seals and opens one app's cursors. It is safe for concurrent use.
type Codec struct {
	// aeads holds one cipher per key of the ring, the first one sealing.
	aeads []cipher.AEAD
	idKey []byte
}

// New derives the cursor keys from the app's keys, so they never reuse the
// key that encrypts tokens.
func New(ring keys.Ring) (*Codec, error) {
	if len(ring) == 0 {
		return nil, errors.New("no keys")
	}
	c := &Codec{}
	for _, key := range ring {
		sealKey, err := hkdf.Key(sha256.New, key, nil, "protobase-changes cursor seal", 32)
		if err != nil {
			return nil, err
		}
		block, err := aes.NewCipher(sealKey)
		if err != nil {
			return nil, err
		}
		aead, err := cipher.NewGCMWithRandomNonce(block)
		if err != nil {
			return nil, err
		}
		c.aeads = append(c.aeads, aead)
	}
	// The oldest key makes ids, so adding a new key first keeps every
	// transaction's id; ids change only once that oldest key is retired.
	idKey, err := hkdf.Key(sha256.New, ring[len(ring)-1], nil, "protobase-changes event id", 32)
	if err != nil {
		return nil, err
	}
	c.idKey = idKey
	return c, nil
}

// Seal returns the cursor of p. Each call draws a fresh random nonce.
func (c *Codec) Seal(p Position) string {
	sealed := c.aeads[0].Seal(nil, nil, plaintext(p), additionalData)
	return base64.RawURLEncoding.EncodeToString(sealed)
}

// Open returns the position a cursor of this app holds.
func (c *Codec) Open(s string) (Position, error) {
	sealed, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Position{}, ErrInvalid
	}
	for _, aead := range c.aeads {
		pt, err := aead.Open(nil, nil, sealed, additionalData)
		if err != nil {
			continue
		}
		if len(pt) != plaintextSize || pt[0] != version {
			return Position{}, ErrInvalid
		}
		return Position{
			Epoch: Epoch{SystemID: binary.BigEndian.Uint64(pt[1:9]), Timeline: binary.BigEndian.Uint32(pt[9:13])},
			LSN:   pglogrepl.LSN(binary.BigEndian.Uint64(pt[13:21])),
		}, nil
	}
	return Position{}, ErrInvalid
}

// ID returns a stable, opaque id for the transaction at p: the same on every
// replay, and revealing nothing of the position.
func (c *Codec) ID(p Position) string {
	mac := hmac.New(sha256.New, c.idKey)
	mac.Write(plaintext(p))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:16])
}

func plaintext(p Position) []byte {
	pt := make([]byte, plaintextSize)
	pt[0] = version
	binary.BigEndian.PutUint64(pt[1:9], p.Epoch.SystemID)
	binary.BigEndian.PutUint32(pt[9:13], p.Epoch.Timeline)
	binary.BigEndian.PutUint64(pt[13:21], uint64(p.LSN))
	return pt
}
