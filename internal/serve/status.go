package serve

import (
	"errors"
	"net/http"

	"github.com/PrototeamHQ/protobase-changes/internal/cursor"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/stream"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

// errUnknownApp means the path names no app this service follows.
var errUnknownApp = errors.New("unknown app")

// fail answers a request that cannot stream with the status its error
// stands for and a fixed message, never the error's own text.
func fail(w http.ResponseWriter, err error) {
	code, message := status(err)
	if code == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	http.Error(w, message, code)
}

func status(err error) (int, string) {
	switch {
	case errors.Is(err, errUnknownApp):
		return http.StatusNotFound, "unknown app"
	case errors.Is(err, token.ErrInvalid):
		return http.StatusUnauthorized, "invalid token"
	case errors.Is(err, stream.ErrExpired):
		return http.StatusUnauthorized, "the token expired"
	case errors.Is(err, stream.ErrRevoked):
		return http.StatusUnauthorized, "the session was revoked"
	case errors.Is(err, token.ErrConditions):
		return http.StatusBadRequest, "conditions are not supported yet"
	case errors.Is(err, cursor.ErrInvalid):
		return http.StatusBadRequest, "invalid cursor"
	case errors.Is(err, stream.ErrGone):
		return http.StatusGone, "the changes after this cursor are no longer held; resync from a snapshot"
	case errors.Is(err, feed.ErrNotReady):
		return http.StatusServiceUnavailable, "the change stream has not caught up"
	}
	return http.StatusInternalServerError, "internal error"
}
