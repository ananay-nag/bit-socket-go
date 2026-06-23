package server

import (
	"sync"

	"github.com/ananay-nag/bit-socket-go/protocol"
)

// Namespace groups sockets, middleware, schemas, and rooms under a single
// path (mirrors src/server/namespace.js). The root namespace is always "/".
type Namespace struct {
	Name   string
	server *Server

	mu      sync.RWMutex
	sockets map[*Socket]struct{}

	middlewares  []ConnMiddleware
	connHandlers []ConnectionHandler

	schemasMu sync.RWMutex
	schemas   map[string]*protocol.Schema
}

func newNamespace(name string, srv *Server) *Namespace {
	return &Namespace{
		Name:    name,
		server:  srv,
		sockets: make(map[*Socket]struct{}),
		schemas: make(map[string]*protocol.Schema),
	}
}

// Schema registers one or more schemas on this namespace, keyed by their
// schema name. Registering a schema for an event lets both EncodePayload and
// the client's auto-sync feature use the compact positional binary layout
// instead of the generic msgpack+deflate envelope.
func (ns *Namespace) Schema(schemas ...*protocol.Schema) *Namespace {
	ns.schemasMu.Lock()
	defer ns.schemasMu.Unlock()
	for _, s := range schemas {
		ns.schemas[s.Name] = s
	}
	return ns
}

func (ns *Namespace) schemaFor(event string) (*protocol.Schema, bool) {
	ns.schemasMu.RLock()
	defer ns.schemasMu.RUnlock()
	s, ok := ns.schemas[event]
	return s, ok
}

// exportSchemas returns this namespace's schema definitions in a form
// suitable for embedding in a CONNECT frame payload (order-preserving via
// protocol.WireSchemaDef under the hood, even though the static type here
// is a plain Go map - see protocol.WireSchemaDef for why that's safe).
func (ns *Namespace) exportSchemas() map[string]interface{} {
	ns.schemasMu.RLock()
	defer ns.schemasMu.RUnlock()
	out := make(map[string]interface{}, len(ns.schemas))
	for event, schema := range ns.schemas {
		out[event] = protocol.WireSchemaDef(schema.Definition)
	}
	return out
}

// Use registers connection middleware for this namespace.
func (ns *Namespace) Use(mw ConnMiddleware) *Namespace {
	ns.mu.Lock()
	ns.middlewares = append(ns.middlewares, mw)
	ns.mu.Unlock()
	return ns
}

// OnConnection registers a handler invoked for every socket that
// successfully joins this namespace (after all connection middleware
// passes).
func (ns *Namespace) OnConnection(h ConnectionHandler) *Namespace {
	ns.mu.Lock()
	ns.connHandlers = append(ns.connHandlers, h)
	ns.mu.Unlock()
	return ns
}

func (ns *Namespace) fireConnection(s *Socket) {
	ns.mu.RLock()
	handlers := append([]ConnectionHandler(nil), ns.connHandlers...)
	ns.mu.RUnlock()
	for _, h := range handlers {
		h(s)
	}
}

func (ns *Namespace) addSocket(s *Socket) {
	ns.mu.Lock()
	ns.sockets[s] = struct{}{}
	ns.mu.Unlock()
}

func (ns *Namespace) removeSocket(s *Socket) {
	ns.mu.Lock()
	delete(ns.sockets, s)
	ns.mu.Unlock()
}

func (ns *Namespace) serializersFor(event string) *protocol.Serializers {
	if schema, ok := ns.schemaFor(event); ok {
		return &protocol.Serializers{EncodePayload: schema.EncodePayload}
	}
	return ns.server.serializers
}

// Emit broadcasts event/payload to every socket connected to this
// namespace.
func (ns *Namespace) Emit(event string, payload interface{}) {
	ns.emitExcluding(event, payload, nil)
}

func (ns *Namespace) emitExcluding(event string, payload interface{}, exclude *Socket) {
	buf, err := protocol.EncodeFrame(protocol.FrameOptions{
		Type: protocol.FrameEvent, Nsp: ns.Name, Event: event, Payload: payload,
	}, ns.serializersFor(event))
	if err != nil {
		return
	}
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	for s := range ns.sockets {
		if s == exclude {
			continue
		}
		s.send(buf)
	}
}

// To returns an Emitter scoped to the given room within this namespace.
func (ns *Namespace) To(room string) *Emitter {
	return &Emitter{emit: func(event string, payload interface{}) {
		ns.toExcluding(room, event, payload, nil)
	}}
}

func (ns *Namespace) toExcluding(room, event string, payload interface{}, exclude *Socket) {
	buf, err := protocol.EncodeFrame(protocol.FrameOptions{
		Type: protocol.FrameEvent, Nsp: ns.Name, Event: event, Payload: payload,
	}, ns.serializersFor(event))
	if err != nil {
		return
	}
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	for s := range ns.sockets {
		if s == exclude {
			continue
		}
		if s.HasRoom(room) {
			s.send(buf)
		}
	}
}

// runMiddlewares walks the namespace's connection middleware chain in
// order, then calls done(true) if every middleware passed, or done(false)
// after notifying and disconnecting the socket if one of them rejected the
// connection.
func (ns *Namespace) runMiddlewares(sock *Socket, done func(bool)) {
	ns.mu.RLock()
	mws := append([]ConnMiddleware(nil), ns.middlewares...)
	ns.mu.RUnlock()

	var run func(idx int)
	run = func(idx int) {
		if idx >= len(mws) {
			done(true)
			return
		}
		mws[idx](sock, func(err error) {
			if err != nil {
				msg := err.Error()
				buf, encErr := protocol.EncodeFrame(protocol.FrameOptions{
					Type: protocol.FrameAck, Nsp: ns.Name, Event: "error",
					Payload: map[string]interface{}{"message": msg},
				}, ns.server.serializers)
				if encErr == nil {
					sock.send(buf)
				}
				sock.closeConn()
				done(false)
				return
			}
			run(idx + 1)
		})
	}
	run(0)
}
