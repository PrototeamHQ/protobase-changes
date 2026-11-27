// Package serve lets consumers pull an app's changes over WebSocket,
// server-sent events or HTTP long polling, each resuming from a cursor, and
// serves the relay's connection.
package serve

import (
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/PrototeamHQ/protobase-changes/internal/destination"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/stream"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

const (
	// keepalive is how long a stream stays silent before a position event,
	// well under the 60s read timeout proxies commonly have.
	keepalive = 25 * time.Second
	// pollTimeout is how long a long poll waits for a change.
	pollTimeout = 25 * time.Second
	// batch bounds the events one long poll returns or a stream writes at once.
	batch = 500
	// writeTimeout bounds one write to a consumer that stopped reading.
	writeTimeout = 30 * time.Second
)

// Server serves the feeds of the apps it follows, by app name, and takes
// the destinations of those that keep them.
type Server struct {
	feeds map[string]*feed.Feed
	hubs  map[string]*destination.Hub
	// connections holds the open consumers of each app with a connections quota.
	connections map[string]*semaphore.Weighted
	now         func() time.Time
	// keepalive and pollTimeout are fields so tests can shorten them.
	keepalive   time.Duration
	pollTimeout time.Duration
}

// New serves the feeds, and the destinations of the apps that have a hub.
func New(feeds map[string]*feed.Feed, hubs map[string]*destination.Hub) *Server {
	connections := map[string]*semaphore.Weighted{}
	for name, f := range feeds {
		if quota := f.App.Quotas.Connections; quota != nil {
			connections[name] = semaphore.NewWeighted(int64(*quota))
		}
	}
	return &Server{
		feeds: feeds, hubs: hubs, connections: connections,
		now: time.Now, keepalive: keepalive, pollTimeout: pollTimeout,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(changesPattern, s.changes)
	mux.HandleFunc(destinationsPattern, s.putDestinations)
	mux.HandleFunc(resumePattern, s.resume)
	mux.HandleFunc(relayPattern, s.relay)
	return mux
}

// changes opens the consumer's stream, then serves it over the transport the
// request asks for.
func (s *Server) changes(w http.ResponseWriter, r *http.Request) {
	f, ok := s.feeds[r.PathValue(AppParam)]
	if !ok {
		fail(w, errUnknownApp)
		return
	}
	tok, err := s.token(r, f)
	if err != nil {
		fail(w, err)
		return
	}
	if open, ok := s.connections[r.PathValue(AppParam)]; ok {
		if !open.TryAcquire(1) {
			fail(w, errTooManyConnections)
			return
		}
		defer open.Release(1)
	}

	websocket := strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
	events := strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	sealed := r.URL.Query().Get(CursorParam)
	if last := r.Header.Get("Last-Event-ID"); last != "" && events {
		// An event source reconnects to the same URL and resends the id of the
		// last event it got, which is past the cursor it first asked for.
		sealed = last
	}
	st, err := stream.Open(f, tok, sealed, s.now)
	if err != nil {
		fail(w, err)
		return
	}

	switch {
	case websocket:
		s.websocket(w, r, st)
	case events:
		s.events(w, r, st)
	default:
		s.poll(w, r, st)
	}
}

// token opens the bearer token of the request with the app's keys.
func (s *Server) token(r *http.Request, f *feed.Feed) (token.Token, error) {
	scheme, raw, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return token.Token{}, token.ErrInvalid
	}
	return token.Open(strings.TrimSpace(raw), f.Keys, f.App.Issuer, s.now())
}
