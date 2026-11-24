// Package keys loads the keys an app shares with protobase-changes.
package keys

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
)

// Size is the length of every key: AES-256 for tokens, and the secret the
// cursor keys derive from.
const Size = 32

// Ring holds an app's keys, newest first: the first encrypts and every one
// decrypts, so a key can be replaced without cutting off what the old one made.
type Ring [][]byte

// Load reads each file as one base64url key without padding, of Size bytes.
func Load(files []string) (Ring, error) {
	ring := make(Ring, 0, len(files))
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		key, err := Parse(data)
		if err != nil {
			return nil, fmt.Errorf("key file %s: %w", file, err)
		}
		ring = append(ring, key)
	}
	return ring, nil
}

// Parse decodes one key, ignoring the whitespace around it.
func Parse(data []byte) ([]byte, error) {
	key, err := base64.RawURLEncoding.DecodeString(string(bytes.TrimSpace(data)))
	if err != nil {
		return nil, fmt.Errorf("not base64url without padding")
	}
	if len(key) != Size {
		return nil, fmt.Errorf("holds %d bytes, want %d", len(key), Size)
	}
	return key, nil
}
