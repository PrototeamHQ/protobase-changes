package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	// batchContentType is the CloudEvents JSON batch format.
	batchContentType = "application/cloudevents-batch+json"
	// requestTimeout bounds one try.
	requestTimeout = 30 * time.Second
	// maxFunctionAnswer bounds the events a function returns, in bytes.
	maxFunctionAnswer = 16 << 20
)

// failure is why a try failed, in words fit to keep in the store, and how
// long the receiver asked to wait before the next.
type failure struct {
	reason     string
	retryAfter time.Duration
}

func (f *failure) Error() string { return f.reason }

// Sender sends batches to their receivers.
type Sender struct {
	Client *http.Client
	// FunctionsURL is where the app serves its API functions.
	FunctionsURL string
	Now          func() time.Time
}

// send delivers one batch: through the function first when the destination
// has one, whose answer is the events to send on, or nothing to drop the
// batch; then to the URL. A failed try returns a *failure.
func (s *Sender) send(ctx context.Context, d *Definition, id string, body []byte) error {
	if d.Function != "" {
		if s.FunctionsURL == "" {
			return &failure{reason: "the app has no functions_url to call function " + d.Function + " at"}
		}
		answer, err := s.post(ctx, d, s.FunctionsURL+"/"+url.PathEscape(d.Function), id, body)
		if err != nil {
			return err
		}
		events, err := returnedEvents(answer)
		if err != nil {
			return err
		}
		if events == nil {
			return nil
		}
		if d.URL == "" {
			return &failure{reason: "function " + d.Function + " returned events, but the destination has no url to send them to"}
		}
		body = events
	}
	_, err := s.post(ctx, d, d.URL, id, body)
	return err
}

// returnedEvents reads a function's answer: a JSON array of events to send
// on, or nil for an empty answer, null or an empty array.
func returnedEvents(answer []byte) ([]byte, error) {
	trimmed := bytes.TrimSpace(answer)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return nil, nil
	}
	var events []json.RawMessage
	if err := json.Unmarshal(trimmed, &events); err != nil {
		return nil, &failure{reason: "the function answered with something other than a JSON array of events"}
	}
	for _, e := range events {
		if !bytes.HasPrefix(bytes.TrimSpace(e), []byte("{")) {
			return nil, &failure{reason: "the function answered with an event that is not a JSON object"}
		}
	}
	if len(events) == 0 {
		return nil, nil
	}
	return trimmed, nil
}

// post sends the body, signed, and returns the answer of a 2xx response.
func (s *Sender) post(ctx context.Context, d *Definition, target, id string, body []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", batchContentType)
	if err := sign(req.Header, d.Secret, id, s.Now(), body); err != nil {
		return nil, err
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, &failure{reason: fmt.Sprintf("no answer within %s", requestTimeout)}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &failure{reason: "the request did not reach the receiver"}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, &failure{
			reason:     fmt.Sprintf("the receiver answered %d", resp.StatusCode),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), s.Now()),
		}
	}
	answer, err := io.ReadAll(io.LimitReader(resp.Body, maxFunctionAnswer+1))
	if err != nil {
		return nil, &failure{reason: "the answer broke off"}
	}
	if len(answer) > maxFunctionAnswer {
		return nil, &failure{reason: fmt.Sprintf("the answer is over %d bytes", maxFunctionAnswer)}
	}
	return answer, nil
}
