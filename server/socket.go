package server

import (
	"crypto/rand"
	"encoding/hex"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/ananay-nag/bit-socket-go/protocol"
)

// Socket represents one client multiplexed onto one namespace of one
// physical WebSocket connection (mirrors src/server/socket.js's
// ServerSocket). A single physical connection has one Socket per namespace
// it has joined (the root "/" namespace socket always exists for a
// connection; additional namespace sockets are created on demand via
// FRAME_CONNECT).
type Socket struct {
	ID        string
	Nsp       string
	Handshake Handshake

	server *Server
	conn   *websocket.Conn
	hub    *connHub

	writeMu sync.Mutex
	closed  bool

	roomsMu sync.RWMutex
	rooms   map[string]struct{}

	listenersMu sync.RWMutex
	listeners   map[string]EventHandler

	middlewaresMu sync.RWMutex
	middlewares   []SocketMiddleware
}

func newSocket(conn *websocket.Conn, srv *Server, nsp string, hs Handshake, hub *connHub) *Socket {
	id := generateSocketID()
	s := &Socket{
		ID:        id,
		Nsp:       nsp,
		Handshake: hs,
		server:    srv,
		conn:      conn,
		hub:       hub,
		rooms:     map[string]struct{}{id: {}}, // auto-join own id room
		listeners: map[string]EventHandler{},
	}
	return s
}

func generateSocketID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is effectively unrecoverable, but don't panic
		// a running server over it - fall back to a fixed-ish value space.
		for i := range buf {
			buf[i] = byte(i)
		}
	}
	return hex.EncodeToString(buf)
}

func (s *Socket) namespace() *Namespace {
	s.server.mu.RLock()
	defer s.server.mu.RUnlock()
	return s.server.namespaces[s.Nsp]
}

func (s *Socket) send(buf []byte) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed {
		return
	}
	_ = s.conn.WriteMessage(websocket.BinaryMessage, buf)
}

// closeConn closes the entire underlying physical WebSocket connection,
// affecting every namespace multiplexed onto it. This matches
// bit-socket-node, where a namespace middleware rejection closes the shared
// `ws` rather than just detaching one namespace.
func (s *Socket) closeConn() {
	s.writeMu.Lock()
	s.closed = true
	s.writeMu.Unlock()
	_ = s.conn.Close()
}

// Use registers event-level middleware for this socket.
func (s *Socket) Use(mw SocketMiddleware) *Socket {
	s.middlewaresMu.Lock()
	s.middlewares = append(s.middlewares, mw)
	s.middlewaresMu.Unlock()
	return s
}

// On registers a handler for an application event sent by this client.
func (s *Socket) On(event string, handler EventHandler) *Socket {
	s.listenersMu.Lock()
	s.listeners[event] = handler
	s.listenersMu.Unlock()
	return s
}

// OnSchema is a convenience wrapper for On(schema.Name, handler).
func (s *Socket) OnSchema(schema *protocol.Schema, handler EventHandler) *Socket {
	return s.On(schema.Name, handler)
}

// Emit sends event/payload to this socket only.
func (s *Socket) Emit(event string, payload interface{}) {
	ser := s.server.serializers
	if ns := s.namespace(); ns != nil {
		ser = ns.serializersFor(event)
	}
	buf, err := protocol.EncodeFrame(protocol.FrameOptions{
		Type: protocol.FrameEvent, Nsp: s.Nsp, Event: event, Payload: payload,
	}, ser)
	if err != nil {
		return
	}
	s.send(buf)
}

// EmitSchema is a convenience wrapper for Emit(schema.Name, payload).
func (s *Socket) EmitSchema(schema *protocol.Schema, payload interface{}) {
	s.Emit(schema.Name, payload)
}

// Broadcast returns an Emitter that sends to every other socket in this
// namespace (excluding this one).
func (s *Socket) Broadcast() *Emitter {
	ns := s.namespace()
	return &Emitter{emit: func(event string, payload interface{}) {
		if ns != nil {
			ns.emitExcluding(event, payload, s)
		}
	}}
}

// To returns an Emitter that sends to every other socket sharing room
// (excluding this one).
func (s *Socket) To(room string) *Emitter {
	ns := s.namespace()
	return &Emitter{emit: func(event string, payload interface{}) {
		if ns != nil {
			ns.toExcluding(room, event, payload, s)
		}
	}}
}

// Join adds this socket to room.
func (s *Socket) Join(room string) {
	s.roomsMu.Lock()
	s.rooms[room] = struct{}{}
	s.roomsMu.Unlock()
}

// Leave removes this socket from room.
func (s *Socket) Leave(room string) {
	s.roomsMu.Lock()
	delete(s.rooms, room)
	s.roomsMu.Unlock()
}

// HasRoom reports whether this socket is currently a member of room.
func (s *Socket) HasRoom(room string) bool {
	s.roomsMu.RLock()
	defer s.roomsMu.RUnlock()
	_, ok := s.rooms[room]
	return ok
}

// Rooms returns a snapshot of the rooms this socket currently belongs to.
func (s *Socket) Rooms() []string {
	s.roomsMu.RLock()
	defer s.roomsMu.RUnlock()
	out := make([]string, 0, len(s.rooms))
	for r := range s.rooms {
		out = append(out, r)
	}
	return out
}

// dispatch routes one already-decoded frame addressed to this socket's
// namespace (mirrors the switch in ServerSocket.initTransport's
// `ws.on('message', ...)` handler).
func (s *Socket) dispatch(frame *protocol.Frame) {
	if frame.Nsp != s.Nsp {
		return
	}
	switch frame.Type {
	case protocol.FrameEvent:
		s.processEvent(frame)
	case protocol.FrameJoin:
		if room, ok := roomFromPayload(frame.Payload); ok {
			s.Join(room)
		}
	case protocol.FrameLeave:
		if room, ok := roomFromPayload(frame.Payload); ok {
			s.Leave(room)
		}
	case protocol.FramePing:
		buf, err := protocol.EncodeFrame(protocol.FrameOptions{Type: protocol.FramePong, Nsp: s.Nsp}, s.server.serializers)
		if err == nil {
			s.send(buf)
		}
	}
}

func roomFromPayload(payload interface{}) (string, bool) {
	m, ok := payload.(map[string]interface{})
	if !ok {
		return "", false
	}
	room, ok := m["room"].(string)
	return room, ok
}

// processEvent runs the socket-level middleware chain, then dispatches to
// the registered handler for frame.Event (mirroring ServerSocket.processEvent).
func (s *Socket) processEvent(frame *protocol.Frame) {
	packet := &EventPacket{Event: frame.Event, Payload: frame.Payload}

	s.middlewaresMu.RLock()
	mws := append([]SocketMiddleware(nil), s.middlewares...)
	s.middlewaresMu.RUnlock()

	var run func(idx int)
	run = func(idx int) {
		if idx >= len(mws) {
			s.invokeHandler(packet, frame.AckID)
			return
		}
		mws[idx](packet, func(err error) {
			if err != nil {
				if frame.AckID > 0 {
					msg := err.Error()
					buf, encErr := protocol.EncodeFrame(protocol.FrameOptions{
						Type: protocol.FrameAck, Nsp: s.Nsp, Event: "error", AckID: frame.AckID,
						Payload: map[string]interface{}{"message": msg},
					}, s.server.serializers)
					if encErr == nil {
						s.send(buf)
					}
				}
				return
			}
			run(idx + 1)
		})
	}
	run(0)
}

func (s *Socket) invokeHandler(packet *EventPacket, ackID uint32) {
	s.listenersMu.RLock()
	handler, ok := s.listeners[packet.Event]
	s.listenersMu.RUnlock()
	if !ok {
		return
	}

	var ack AckFunc
	if ackID > 0 {
		event := packet.Event
		ack = func(resp interface{}) {
			// Note: matching bit-socket-node's ServerSocket.processEvent,
			// ACK frames are always encoded with the default codec, even if
			// the event has a registered Schema (only emit() looks up
			// per-event schemas).
			buf, err := protocol.EncodeFrame(protocol.FrameOptions{
				Type: protocol.FrameAck, Nsp: s.Nsp, Event: event, AckID: ackID, Payload: resp,
			}, s.server.serializers)
			if err == nil {
				s.send(buf)
			}
		}
	}

	handler(packet.Payload, ack)
}
