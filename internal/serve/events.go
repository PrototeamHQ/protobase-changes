package serve

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/PrototeamHQ/protobase-changes/internal/stream"
)

// events streams server-sent events, one CloudEvent each, with the event's
// cursor as its id so an event source resumes by itself. The stream ends
// when the consumer falls behind, its token expires or its session is
// revoked; the reconnect then gets the status that says which.
func (s *Server) events(w http.ResponseWriter, r *http.Request, st *stream.Stream) {
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	// Proxies that buffer responses would hold the events back.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if rc.Flush() != nil {
		return
	}

	for {
		events, err := st.Next(r.Context(), s.keepalive, batch)
		if err != nil {
			return
		}
		var buf bytes.Buffer
		for _, e := range events {
			data, err := json.Marshal(e)
			if err != nil {
				return
			}
			buf.WriteString("id: " + e.Cursor + "\ndata: ")
			buf.Write(data)
			buf.WriteString("\n\n")
		}
		if rc.SetWriteDeadline(time.Now().Add(writeTimeout)) != nil {
			return
		}
		if _, err := w.Write(buf.Bytes()); err != nil {
			return
		}
		if rc.Flush() != nil {
			return
		}
	}
}
