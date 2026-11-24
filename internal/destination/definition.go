// Package destination delivers an app's changes to webhooks and API
// functions, each destination from a cursor of its own.
package destination

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/match"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

// AtLimit is what a destination does with a batch that failed its last try.
type AtLimit string

const (
	// Pause stops the destination at the batch, which it tries again once resumed.
	Pause AtLimit = "pause"
	// DeadLetter keeps the batch in the store and goes on after it.
	DeadLetter AtLimit = "dead_letter"
)

const (
	// defaultBatchSize and maxBatchSize bound the events of one batch.
	defaultBatchSize = 100
	maxBatchSize     = 500
)

// Definition is one destination as the app defines it.
type Definition struct {
	Name string
	// URL receives each batch, or what Function returns for it.
	URL string
	// Function is the app's API function that gets each batch first.
	Function string
	// Secret signs every request, as whsec_ and base64.
	Secret  string
	Payload token.Payload
	// Subscriptions picks the transaction events, as a live token's subs do.
	Subscriptions *match.Index
	Events        []NamedEvent
	BatchSize     int
	// Tries bounds how often a batch is sent before AtLimit applies.
	Tries   int
	AtLimit AtLimit
	// digest identifies this exact definition, secret included.
	digest [32]byte
}

// NamedEvent raises an event of Type for each change of Resource that is one
// of Operations, or any when none are listed, and meets When.
type NamedEvent struct {
	Type       string
	Resource   string
	Operations []change.Operation
	When       match.Transition
}

type definitionJSON struct {
	Name     string        `json:"name"`
	URL      string        `json:"url"`
	Function string        `json:"function"`
	Secret   string        `json:"secret"`
	Payload  token.Payload `json:"payload"`
	Subs     []struct {
		Resource string          `json:"resource"`
		Where    json.RawMessage `json:"where"`
	} `json:"subs"`
	Events []struct {
		Type       string             `json:"type"`
		Resource   string             `json:"resource"`
		Operations []change.Operation `json:"operations"`
		Old        json.RawMessage    `json:"old"`
		New        json.RawMessage    `json:"new"`
	} `json:"events"`
	BatchSize int `json:"batch_size"`
	Retry     struct {
		Tries   int     `json:"tries"`
		AtLimit AtLimit `json:"at_limit"`
	} `json:"retry"`
}

var (
	// namePattern matches destination and function names, as URL segments.
	namePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,62}$`)
	// typePattern matches event types such as orders.paid.
	typePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)+$`)
)

// parseDefinitions reads a JSON array of definitions, refusing unknown
// fields and invalid or repeated destinations.
func parseDefinitions(raw json.RawMessage) ([]*Definition, error) {
	var defs []json.RawMessage
	if err := json.Unmarshal(raw, &defs); err != nil {
		return nil, fmt.Errorf("destinations is not an array")
	}
	out := make([]*Definition, 0, len(defs))
	names := map[string]bool{}
	for i, one := range defs {
		d, err := parseDefinition(one)
		if err != nil {
			return nil, fmt.Errorf("destinations[%d]: %w", i, err)
		}
		if names[d.Name] {
			return nil, fmt.Errorf("destination %q is listed twice", d.Name)
		}
		names[d.Name] = true
		out = append(out, d)
	}
	return out, nil
}

func parseDefinition(raw json.RawMessage) (*Definition, error) {
	var in definitionJSON
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return nil, fmt.Errorf("not a destination: %w", err)
	}
	d := &Definition{
		Name: in.Name, URL: in.URL, Function: in.Function, Secret: in.Secret, Payload: in.Payload,
		BatchSize: in.BatchSize, Tries: in.Retry.Tries, AtLimit: in.Retry.AtLimit,
		digest: sha256.Sum256(raw),
	}
	if err := d.checkTarget(); err != nil {
		return nil, err
	}
	if d.Payload != token.Thin && d.Payload != token.Raw {
		return nil, fmt.Errorf("payload must be thin or raw")
	}
	if d.BatchSize == 0 {
		d.BatchSize = defaultBatchSize
	}
	if d.BatchSize < 1 || d.BatchSize > maxBatchSize {
		return nil, fmt.Errorf("batch_size must be 1 to %d", maxBatchSize)
	}
	if d.Tries == 0 {
		d.Tries = maxTries
	}
	if d.Tries < 1 || d.Tries > maxTries {
		return nil, fmt.Errorf("retry.tries must be 1 to %d", maxTries)
	}
	if d.AtLimit != Pause && d.AtLimit != DeadLetter {
		return nil, fmt.Errorf("retry.at_limit must be pause or dead_letter")
	}

	subs := make([]match.Subscription, len(in.Subs))
	for i, sub := range in.Subs {
		if sub.Resource == "" {
			return nil, fmt.Errorf("subs[%d] names no resource", i)
		}
		where, err := compile(sub.Where)
		if err != nil {
			return nil, fmt.Errorf("subs[%d].where: %w", i, err)
		}
		subs[i] = match.Subscription{ID: i, Resource: sub.Resource, Where: where}
	}
	d.Subscriptions = match.NewIndex(subs)

	for i, e := range in.Events {
		named, err := parseNamedEvent(e.Type, e.Resource, e.Operations, e.Old, e.New)
		if err != nil {
			return nil, fmt.Errorf("events[%d]: %w", i, err)
		}
		d.Events = append(d.Events, named)
	}
	if len(subs) == 0 && len(d.Events) == 0 {
		return nil, fmt.Errorf("destination %q lists no subs and no events", d.Name)
	}
	return d, nil
}

// checkTarget checks the name, where batches go and how they are signed.
func (d *Definition) checkTarget() error {
	if !namePattern.MatchString(d.Name) {
		return fmt.Errorf("name must be 1 to 63 letters, digits, _ or -, starting with a letter")
	}
	if d.URL == "" && d.Function == "" {
		return fmt.Errorf("destination %q has no url and no function", d.Name)
	}
	if d.URL != "" {
		u, err := url.Parse(d.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("destination %q url must be an absolute http or https URL", d.Name)
		}
	}
	if d.Function != "" && !namePattern.MatchString(d.Function) {
		return fmt.Errorf("destination %q function must be a function name", d.Name)
	}
	if _, err := newSigner(d.Secret); err != nil {
		return fmt.Errorf("destination %q secret must be whsec_ and base64", d.Name)
	}
	return nil
}

func parseNamedEvent(typ, resource string, ops []change.Operation, old, new json.RawMessage) (NamedEvent, error) {
	if !typePattern.MatchString(typ) {
		return NamedEvent{}, fmt.Errorf("type must be dotted, such as orders.paid")
	}
	if resource == "" {
		return NamedEvent{}, fmt.Errorf("names no resource")
	}
	for _, op := range ops {
		if !slices.Contains([]change.Operation{change.Insert, change.Update, change.Delete}, op) {
			return NamedEvent{}, fmt.Errorf("operations must be insert, update or delete")
		}
	}
	oldWhere, err := compile(old)
	if err != nil {
		return NamedEvent{}, fmt.Errorf("old: %w", err)
	}
	newWhere, err := compile(new)
	if err != nil {
		return NamedEvent{}, fmt.Errorf("new: %w", err)
	}
	return NamedEvent{Type: typ, Resource: resource, Operations: ops, When: match.Transition{Old: oldWhere, New: newWhere}}, nil
}

// compile reads a condition, or none when it is absent or null.
func compile(raw json.RawMessage) (*match.Condition, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	return match.Compile(raw)
}

// types names the named events a change raises, for event.Named, each type
// once however many of its rules the change meets.
func (d *Definition) types(resource string, c *change.Change) []string {
	var out []string
	for _, e := range d.Events {
		if e.Resource != resource || (len(e.Operations) > 0 && !slices.Contains(e.Operations, c.Operation)) {
			continue
		}
		if !slices.Contains(out, e.Type) && e.When.Holds(c) {
			out = append(out, e.Type)
		}
	}
	return out
}
