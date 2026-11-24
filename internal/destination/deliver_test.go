package destination

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestReturnedEvents(t *testing.T) {
	for _, answer := range []string{"", " null ", "[]"} {
		if events, err := returnedEvents([]byte(answer)); events != nil || err != nil {
			t.Errorf("%q: %s %v, want the batch dropped", answer, events, err)
		}
	}
	if events, err := returnedEvents([]byte(` [{"id":"a"}] `)); string(events) != `[{"id":"a"}]` || err != nil {
		t.Errorf("events %s %v", events, err)
	}
	for _, answer := range []string{`{"id":"a"}`, `[1]`, `ok`} {
		var f *failure
		if _, err := returnedEvents([]byte(answer)); !errors.As(err, &f) {
			t.Errorf("%q: %v, want a failed try", answer, err)
		}
	}
}

func TestSendFailsWhereAFunctionCannotGo(t *testing.T) {
	events := newReceiver(t, func(received) (int, string) { return http.StatusOK, `[{"id":"a"}]` })
	s := &Sender{Client: http.DefaultClient, Now: time.Now}
	d := &Definition{Name: "flagged", Function: "flag", Secret: secret}
	var f *failure
	if err := s.send(context.Background(), d, "b1", []byte(`[]`)); !errors.As(err, &f) {
		t.Fatalf("without functions_url: %v", err)
	}
	s.FunctionsURL = events.server.URL
	if err := s.send(context.Background(), d, "b1", []byte(`[]`)); !errors.As(err, &f) || f.reason != "function flag returned events, but the destination has no url to send them to" {
		t.Fatalf("events without a url: %v", err)
	}
}
