package server

import (
	"net/http"
	"time"
)

// Handshake carries the request metadata captured when the underlying
// WebSocket connection was first established (mirrors the `handshake`
// object attached to every ServerSocket in bit-socket-node).
type Handshake struct {
	Headers http.Header
	URL     string
	Time    time.Time
}

// AckFunc, when non-nil, sends an acknowledgement frame back to the client
// that emitted the event being handled.
type AckFunc func(payload interface{})

// EventHandler handles one incoming application event. ack is nil unless
// the client requested an acknowledgement.
type EventHandler func(payload interface{}, ack AckFunc)

// ConnectionHandler handles a newly connected (and middleware-approved)
// socket on a namespace.
type ConnectionHandler func(socket *Socket)

// ConnMiddleware gates namespace connection/transition. Call next(nil) to
// allow the connection, or next(err) to reject it (which also closes the
// underlying WebSocket, matching bit-socket-node's behavior).
type ConnMiddleware func(socket *Socket, next func(error))

// EventPacket is the mutable [event, payload] pair passed through
// socket-level event middleware. Middleware may mutate Payload in place
// (when it's a map[string]interface{} or []interface{}) to enrich/transform
// the event before it reaches the registered handler.
type EventPacket struct {
	Event   string
	Payload interface{}
}

// SocketMiddleware gates a single incoming event for one socket. Call
// next(nil) to continue the chain, or next(err) to block the event (and, if
// the client requested an ack, return an "error" ack frame).
type SocketMiddleware func(packet *EventPacket, next func(error))

// Emitter is a small fluent helper returned by To()/Broadcast() so calls can
// be chained like `socket.To("room").Emit("event", payload)`.
type Emitter struct {
	emit func(event string, payload interface{})
}

// Emit sends event/payload through this emitter's target (a room, a
// broadcast scope, etc).
func (e *Emitter) Emit(event string, payload interface{}) {
	if e == nil || e.emit == nil {
		return
	}
	e.emit(event, payload)
}
