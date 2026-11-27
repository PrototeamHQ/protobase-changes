package relay

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/PrototeamHQ/protobase-changes/internal/feed"
)

// Subprotocol is the WebSocket subprotocol of the relay's connection: one
// JSON message per text message.
const Subprotocol = "protobase-relay.json"

const (
	// readLimit bounds one message from the relay, a register with a long
	// list of values in its conditions included.
	readLimit = 1 << 20
	// writeTimeout bounds one write to a relay that stopped reading.
	writeTimeout = 30 * time.Second
)

// Serve reads the relay's messages on c until it closes or ctx ends, and
// serves the registrations of the apps of feeds, those the connection's key
// covers. A register for another app is refused. A message that is not
// one the relay sends closes the connection.
func Serve(ctx context.Context, c *websocket.Conn, feeds map[string]*feed.Feed, now func() time.Time) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()
	c.SetReadLimit(readLimit)
	send := func(ctx context.Context, msg any) error {
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, writeTimeout)
		defer cancel()
		return c.Write(ctx, websocket.MessageText, data)
	}

	workers := map[string]*worker{}
	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			// The relay closed, a worker's write failed or the service stops.
			c.Close(websocket.StatusGoingAway, "closing")
			return
		}
		m, err := parse(data)
		if typ != websocket.MessageText || err != nil {
			c.Close(websocket.StatusPolicyViolation, "invalid message")
			return
		}
		f, ok := feeds[m.App]
		if !ok {
			if m.Type == RegisterType {
				if send(ctx, Notice{Type: RefusedType, App: m.App, ID: m.ID, Reason: "the key does not cover the app"}) != nil {
					return
				}
			}
			continue
		}
		w := workers[m.App]
		if w == nil {
			w = newWorker(m.App, f, send, now)
			workers[m.App] = w
			// run ends only when ctx ends or a write fails: either way the
			// connection ends, and its reader then returns.
			wg.Go(func() {
				w.run(ctx)
				cancel()
			})
		}
		w.post(m)
	}
}
