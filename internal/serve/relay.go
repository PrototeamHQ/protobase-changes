package serve

import (
	"crypto/subtle"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/coder/websocket"
	"golang.org/x/sync/semaphore"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/keys"
	"github.com/PrototeamHQ/protobase-changes/internal/relay"
)

// relay serves the relay's connection, for the apps its key covers. A
// request whose key covers no app is refused before the handshake. The
// connection counts as one of each covered app's connections.
func (s *Server) relay(w http.ResponseWriter, r *http.Request) {
	covered := s.relayFeeds(r)
	if len(covered) == 0 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "invalid relay key", http.StatusUnauthorized)
		return
	}
	release, ok := s.acquireAll(covered)
	if !ok {
		fail(w, errTooManyConnections)
		return
	}
	defer release()
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{relay.Subprotocol}})
	if err != nil {
		// Accept answered the handshake already.
		return
	}
	defer c.CloseNow()
	relay.Serve(r.Context(), c, covered, s.now)
}

// relayFeeds returns the feeds of the apps that list the request's bearer
// key among their relay keys.
func (s *Server) relayFeeds(r *http.Request) map[string]*feed.Feed {
	scheme, raw, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return nil
	}
	key, err := keys.Parse([]byte(raw))
	if err != nil {
		return nil
	}
	covered := map[string]*feed.Feed{}
	for name, f := range s.feeds {
		for _, k := range f.RelayKeys {
			if subtle.ConstantTimeCompare(k, key) == 1 {
				covered[name] = f
			}
		}
	}
	return covered
}

// acquireAll takes a connection of each app of feeds that has a quota.
// ok is false, and nothing is taken, when one of them has none left.
func (s *Server) acquireAll(feeds map[string]*feed.Feed) (release func(), ok bool) {
	var taken []*semaphore.Weighted
	release = func() {
		for _, open := range taken {
			open.Release(1)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(feeds)) {
		open, limited := s.connections[name]
		if !limited {
			continue
		}
		if !open.TryAcquire(1) {
			release()
			return nil, false
		}
		taken = append(taken, open)
	}
	return release, true
}
