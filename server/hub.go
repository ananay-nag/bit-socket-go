package server

import (
	"sync"

	"github.com/gorilla/websocket"

	"github.com/ananay-nag/bit-socket-go/protocol"
)

// connHub owns the single physical WebSocket connection underlying
// potentially several namespace Sockets (one connection, N namespaces, like
// Socket.IO-style multiplexing). gorilla/websocket only allows one
// concurrent reader per connection, so unlike the Node implementation
// (where every namespace Socket independently attaches its own 'message'
// listener to the same ws), Go centralizes decoding here and dispatches by
// namespace.
type connHub struct {
	conn      *websocket.Conn
	server    *Server
	handshake Handshake

	mu      sync.RWMutex
	sockets map[string]*Socket // nsp -> socket
}

func newConnHub(conn *websocket.Conn, srv *Server, hs Handshake) *connHub {
	return &connHub{
		conn:      conn,
		server:    srv,
		handshake: hs,
		sockets:   map[string]*Socket{},
	}
}

func (h *connHub) send(buf []byte) {
	// Any socket on this hub shares the same underlying writeMu-guarded
	// conn; grab any existing socket to reuse its safe-send path, or write
	// directly if none exist yet (e.g. namespace-not-found error frame).
	h.mu.RLock()
	var any *Socket
	for _, s := range h.sockets {
		any = s
		break
	}
	h.mu.RUnlock()
	if any != nil {
		any.send(buf)
		return
	}
	_ = h.conn.WriteMessage(websocket.BinaryMessage, buf)
}

func (h *connHub) readLoop() {
	defer h.handleClose()
	for {
		_, data, err := h.conn.ReadMessage()
		if err != nil {
			return
		}
		h.handleMessage(data)
	}
}

func (h *connHub) handleMessage(raw []byte) {
	ser := &protocol.Serializers{
		DecodePayloadWithEvent: func(buf []byte, event, nsp string) (interface{}, error) {
			h.server.mu.RLock()
			ns, ok := h.server.namespaces[nsp]
			h.server.mu.RUnlock()
			if ok {
				if schema, hasSchema := ns.schemaFor(event); hasSchema {
					return schema.DecodePayload(buf)
				}
			}
			return h.server.serializers.DecodePayload(buf)
		},
	}

	frame, err := protocol.DecodeFrame(raw, ser)
	if err != nil {
		return
	}

	if frame.Type == protocol.FrameConnect {
		if frame.Nsp != "/" {
			h.handleNamespaceConnect(frame.Nsp)
		}
		return
	}

	h.mu.RLock()
	sock, ok := h.sockets[frame.Nsp]
	h.mu.RUnlock()
	if !ok {
		return
	}
	sock.dispatch(frame)
}

func (h *connHub) handleNamespaceConnect(nsp string) {
	h.server.mu.RLock()
	ns, ok := h.server.namespaces[nsp]
	h.server.mu.RUnlock()

	if !ok {
		buf, err := protocol.EncodeFrame(protocol.FrameOptions{
			Type: protocol.FrameAck, Nsp: nsp, Event: "error",
			Payload: map[string]interface{}{"message": "Requested namespace '" + nsp + "' does not exist on cluster."},
		}, h.server.serializers)
		if err == nil {
			h.send(buf)
		}
		return
	}

	sock := newSocket(h.conn, h.server, nsp, h.handshake, h)
	ns.runMiddlewares(sock, func(passed bool) {
		if !passed {
			return
		}
		ns.addSocket(sock)
		h.mu.Lock()
		h.sockets[nsp] = sock
		h.mu.Unlock()

		var connectPayload interface{}
		if h.server.useSchemas {
			connectPayload = ns.exportSchemas()
		}
		buf, err := protocol.EncodeFrame(protocol.FrameOptions{
			Type: protocol.FrameConnect, Nsp: nsp, Payload: connectPayload,
		}, h.server.serializers)
		if err == nil {
			sock.send(buf)
		}

		ns.fireConnection(sock)
	})
}

func (h *connHub) handleClose() {
	h.mu.Lock()
	socks := make([]*Socket, 0, len(h.sockets))
	for _, s := range h.sockets {
		socks = append(socks, s)
	}
	h.sockets = map[string]*Socket{}
	h.mu.Unlock()

	for _, s := range socks {
		h.server.mu.RLock()
		ns, ok := h.server.namespaces[s.Nsp]
		h.server.mu.RUnlock()
		if ok {
			ns.removeSocket(s)
		}
	}
}
