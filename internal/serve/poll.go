package serve

import (
	"encoding/json"
	"net/http"

	"github.com/PrototeamHQ/protobase-changes/internal/stream"
)

// batchContentType is the CloudEvents JSON batch format.
const batchContentType = "application/cloudevents-batch+json"

// poll answers with the events after the consumer's cursor as soon as there
// is one, or with a position event when none comes within the poll timeout.
func (s *Server) poll(w http.ResponseWriter, r *http.Request, st *stream.Stream) {
	events, err := st.Next(r.Context(), s.pollTimeout, batch)
	if r.Context().Err() != nil {
		// The consumer left; nobody reads an answer.
		return
	}
	if err != nil {
		fail(w, err)
		return
	}
	body, err := json.Marshal(events)
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", batchContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Write(body)
}
