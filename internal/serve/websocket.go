package serve

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/coder/websocket"

	"github.com/PrototeamHQ/protobase-changes/internal/stream"
)

// Subprotocol is the WebSocket subprotocol of one structured CloudEvent in
// JSON per text message.
const Subprotocol = "cloudevents.json"

// websocket streams one CloudEvent per text message. It reads nothing from
// the consumer but control frames, and closes with a code that says why the
// stream ended.
func (s *Server) websocket(w http.ResponseWriter, r *http.Request, st *stream.Stream) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{Subprotocol}})
	if err != nil {
		// Accept answered the handshake already.
		return
	}
	defer c.CloseNow()
	ctx := c.CloseRead(r.Context())

	for {
		events, err := st.Next(ctx, s.keepalive, batch)
		if err != nil {
			code, reason := closeStatus(err)
			c.Close(code, reason)
			return
		}
		for _, e := range events {
			data, err := json.Marshal(e)
			if err != nil {
				c.Close(websocket.StatusInternalError, "internal error")
				return
			}
			writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
			err = c.Write(writeCtx, websocket.MessageText, data)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func closeStatus(err error) (websocket.StatusCode, string) {
	switch {
	case errors.Is(err, stream.ErrGone):
		return websocket.StatusTryAgainLater, "fell behind the changes held"
	case errors.Is(err, stream.ErrExpired):
		return websocket.StatusPolicyViolation, "the token expired"
	case errors.Is(err, stream.ErrRevoked):
		return websocket.StatusPolicyViolation, "the session was revoked"
	case errors.Is(err, context.Canceled):
		return websocket.StatusGoingAway, "closing"
	}
	return websocket.StatusInternalError, "internal error"
}
