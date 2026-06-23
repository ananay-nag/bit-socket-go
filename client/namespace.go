package client

import (
	"sync"

	"github.com/ananay-nag/bit-socket-go/protocol"
)

// Namespace is one multiplexed channel of a Client, bound to a single
// server-side namespace path (mirrors ClientNamespace in
// bit-socket-node's src/client/client.js).
type Namespace struct {
	client *Client
	nsp    string

	listenersMu sync.RWMutex
	listeners   map[string]EventHandler

	middlewaresMu sync.RWMutex
	middlewares   []Middleware
}

func newNamespace(c *Client, nsp string) *Namespace {
	c.ensureSchemaBucket(schemaKey(nsp))
	return &Namespace{
		client:    c,
		nsp:       nsp,
		listeners: map[string]EventHandler{},
	}
}

// Schema registers one or more schemas for this namespace locally (useful
// if you want to encode against a known schema before the server's
// auto-sync CONNECT frame has arrived, or when not using auto-sync at all).
func (n *Namespace) Schema(schemas ...*protocol.Schema) *Namespace {
	n.client.registerSchemas(n.nsp, schemas...)
	return n
}

// On registers a handler for an event on this namespace. event may be a
// custom application event name, or one of the lifecycle events "connect",
// "disconnect", "reconnecting".
func (n *Namespace) On(event string, handler EventHandler) *Namespace {
	n.listenersMu.Lock()
	n.listeners[event] = handler
	n.listenersMu.Unlock()
	return n
}

// OnSchema is a convenience wrapper for On(schema.Name, handler).
func (n *Namespace) OnSchema(schema *protocol.Schema, handler EventHandler) *Namespace {
	return n.On(schema.Name, handler)
}

// Emit sends event/payload to the server on this namespace, fire-and-forget.
func (n *Namespace) Emit(event string, payload interface{}) {
	n.client.doEmit(event, payload, nil, n.nsp)
}

// EmitAck sends event/payload and invokes cb with the server's
// acknowledgement payload once it arrives.
func (n *Namespace) EmitAck(event string, payload interface{}, cb AckCallback) {
	n.client.doEmit(event, payload, cb, n.nsp)
}

// Join asks the server to add this connection to room on this namespace.
func (n *Namespace) Join(room string) {
	n.client.doJoin(room, n.nsp)
}

// Leave asks the server to remove this connection from room on this
// namespace.
func (n *Namespace) Leave(room string) {
	n.client.doLeave(room, n.nsp)
}

// Use registers event middleware for this namespace.
func (n *Namespace) Use(mw Middleware) *Namespace {
	n.middlewaresMu.Lock()
	n.middlewares = append(n.middlewares, mw)
	n.middlewaresMu.Unlock()
	return n
}

// Close clears this namespace's listeners. Closing the root ("/") namespace
// also closes the underlying client connection entirely.
func (n *Namespace) Close() {
	n.listenersMu.Lock()
	n.listeners = map[string]EventHandler{}
	n.listenersMu.Unlock()
	if n.nsp == "/" {
		n.client.Close()
	}
}

func (n *Namespace) handler(event string) (EventHandler, bool) {
	n.listenersMu.RLock()
	defer n.listenersMu.RUnlock()
	h, ok := n.listeners[event]
	return h, ok
}

// triggerEvent runs this namespace's middleware chain (if any) then invokes
// the registered handler for event, mirroring ClientNamespace._triggerEvent.
func (n *Namespace) triggerEvent(event string, data interface{}) {
	n.middlewaresMu.RLock()
	mws := append([]Middleware(nil), n.middlewares...)
	n.middlewaresMu.RUnlock()

	if len(mws) == 0 {
		if h, ok := n.handler(event); ok {
			h(data)
		}
		return
	}

	packet := &EventPacket{Event: event, Payload: data}
	var run func(idx int)
	run = func(idx int) {
		if idx >= len(mws) {
			if h, ok := n.handler(packet.Event); ok {
				h(packet.Payload)
			}
			return
		}
		mws[idx](packet, func(err error) {
			if err != nil {
				return // halt on error, matching the JS implementation
			}
			run(idx + 1)
		})
	}
	run(0)
}
