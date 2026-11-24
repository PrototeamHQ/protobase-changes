package serve

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/PrototeamHQ/protobase-changes/internal/destination"
)

// maxSetToken bounds the token that defines an app's destinations.
const maxSetToken = 1 << 20

var (
	// errNoStore means the app is followed but keeps no destinations.
	errNoStore = errors.New("the app keeps no destinations")
	// errTooLarge means a set token is over maxSetToken.
	errTooLarge = errors.New("the token is too large")
	// errBody means the request body could not be read, such as a broken chunk.
	errBody = errors.New("the body could not be read")
)

// putDestinations replaces the app's destinations with those of the token
// in the body: the app holds the definitions and puts them whenever they
// change, such as at each deploy.
func (s *Server) putDestinations(w http.ResponseWriter, r *http.Request) {
	hub, err := s.hub(r)
	if err != nil {
		fail(w, err)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSetToken+1))
	if err != nil {
		fail(w, errBody)
		return
	}
	if len(body) > maxSetToken {
		fail(w, errTooLarge)
		return
	}
	if err := hub.Put(r.Context(), strings.TrimSpace(string(body))); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// resume wakes a paused or gone destination with a bearer token that names it.
func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	hub, err := s.hub(r)
	if err != nil {
		fail(w, err)
		return
	}
	f := s.feeds[r.PathValue(AppParam)]
	scheme, raw, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		fail(w, destination.ErrInvalidToken)
		return
	}
	name, err := destination.OpenResume(strings.TrimSpace(raw), f.Keys, f.App.Issuer, s.now())
	if err != nil {
		fail(w, err)
		return
	}
	if name != r.PathValue(DestinationParam) {
		fail(w, destination.ErrInvalidToken)
		return
	}
	if err := hub.Resume(name); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) hub(r *http.Request) (*destination.Hub, error) {
	app := r.PathValue(AppParam)
	if _, ok := s.feeds[app]; !ok {
		return nil, errUnknownApp
	}
	hub, ok := s.hubs[app]
	if !ok {
		return nil, errNoStore
	}
	return hub, nil
}
