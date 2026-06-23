// Package client implements the BitSocket client: connects to a
// bit-socket server, multiplexes namespaces over one WebSocket, auto-syncs
// server-declared Schemas, and handles heartbeat + exponential-backoff
// reconnection. It is a Go port of bit-socket-node's src/client.
package client

// EventHandler handles one incoming event (including the synthetic
// "connect", "disconnect", and "reconnecting" lifecycle events, matching
// bit-socket-node where these are just regular entries in the same
// listeners map).
type EventHandler func(payload interface{})

// AckCallback receives the payload of an acknowledgement frame sent in
// response to an Emit call.
type AckCallback func(payload interface{})

// EventPacket is the mutable [event, payload] pair passed through
// namespace-level event middleware before it reaches a registered handler.
type EventPacket struct {
	Event   string
	Payload interface{}
}

// Middleware gates one incoming event for one namespace. Call next(nil) to
// continue the chain (eventually reaching the registered handler), or
// next(err) to halt it (matching bit-socket-node, where a middleware error
// silently drops the event rather than invoking the handler).
type Middleware func(packet *EventPacket, next func(error))

func schemaKey(nsp string) string {
	if nsp == "/" {
		return "root"
	}
	if len(nsp) > 0 && nsp[0] == '/' {
		return nsp[1:]
	}
	return nsp
}
