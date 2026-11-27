// Package relay serves the relay's one connection: it registers the live
// subscriptions of many apps' pages and sends each the changes it matches.
package relay

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/PrototeamHQ/protobase-changes/internal/event"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

// The types of the messages on the connection, besides the CloudEvents of
// type event.TransactionType that carry the changes.
const (
	RegisterType   = "register"
	UnregisterType = "unregister"
	GapType        = "gap"
	RevokedType    = "revoked"
	RefusedType    = "refused"
)

// errMessage means a message is not one the relay sends.
var errMessage = errors.New("invalid message")

// Message is one message from the relay.
type Message struct {
	Type string `json:"type"`
	App  string `json:"app"`
	// ID is the relay's id of the registration, unique within the app.
	ID string `json:"id"`
	// The rest is for a register: the live token's subscriptions, the
	// columns each change carries, the session or that it has none, and the
	// snapshot the page read, or the cursor it resumes after.
	Subs      []token.Sub `json:"subs"`
	Fields    []string    `json:"fields"`
	Sid       string      `json:"sid"`
	Anonymous bool        `json:"anonymous"`
	Snapshot  string      `json:"snapshot"`
	Cursor    string      `json:"cursor"`
}

// parse reads one message, refusing unknown fields and types.
func parse(data []byte) (Message, error) {
	var m Message
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return Message{}, errMessage
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return Message{}, errMessage
	}
	if (m.Type != RegisterType && m.Type != UnregisterType) || m.App == "" || m.ID == "" {
		return Message{}, errMessage
	}
	return m, nil
}

// Notice is a message to the relay about one registration.
type Notice struct {
	Type string `json:"type"`
	App  string `json:"app"`
	ID   string `json:"id"`
	// Reason says why a register was refused.
	Reason string `json:"reason,omitempty"`
}

// Event is one CloudEvent: a transaction's changes, with an entry for each
// registration they concern.
type Event struct {
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Source          string `json:"source"`
	Type            string `json:"type"`
	Time            string `json:"time"`
	DataContentType string `json:"datacontenttype"`
	// App is the extension attribute naming the app.
	App  string `json:"app"`
	Data Data   `json:"data"`
}

// Data is a transaction event's body.
type Data struct {
	Registrations []Entry `json:"registrations"`
}

// Entry is what one registration gets of a transaction: its changes, each
// with its subscriptions, and the cursor the page resumes after it with.
type Entry struct {
	ID      string         `json:"id"`
	Cursor  string         `json:"cursor"`
	Changes []event.Change `json:"changes"`
}
