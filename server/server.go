// Package server implements the BitSocket server: a binary WebSocket
// protocol with namespaces, rooms, connection/event middleware, ack
// callbacks, and optional fixed-layout Schemas that auto-sync to clients.
//
// It is a Go port of bit-socket-node's src/server, preserving the wire
// format byte-for-byte (see the protocol package) and the same conceptual
// API shape, adapted to idiomatic Go (explicit method names instead of a
// generic on(event) dispatcher, ordered Schema field declarations instead
// of object literals, and goroutine-safe namespaces/rooms).
package server

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"bitsocket/protocol"
)

// Config configures a new Server.
type Config struct {
	// Port, if > 0, makes New start its own internal *http.Server listening
	// on this port, upgrading every incoming request to a BitSocket
	// connection (matching bit-socket-node, which doesn't restrict upgrades
	// to a specific URL path). If Port is 0, call Handler() and mount it
	// into your own http.ServeMux/router instead.
	Port int

	// Serializers overrides the default msgpack+deflate payload codec.
	Serializers *protocol.Serializers

	// UseSchemas controls whether CONNECT frames export each namespace's
	// registered schemas for client auto-sync. Defaults to true.
	UseSchemas *bool

	// CORSOrigins lists allowed Origin header values. Defaults to ["*"]
	// (allow all), matching bit-socket-node's default.
	CORSOrigins []string

	ReadBufferSize  int
	WriteBufferSize int
}

// Server is a running (or attachable) BitSocket server.
type Server struct {
	serializers    *protocol.Serializers
	useSchemas     bool
	allowedOrigins []string

	mu         sync.RWMutex
	namespaces map[string]*Namespace

	upgrader   websocket.Upgrader
	httpServer *http.Server
	listener   net.Listener
}

// New constructs a Server. If cfg.Port > 0, it also starts listening
// immediately in the background.
func New(cfg Config) *Server {
	s := &Server{
		serializers: protocol.DefaultSerializers(),
		useSchemas:  true,
		namespaces:  map[string]*Namespace{},
	}
	if cfg.Serializers != nil {
		s.serializers = cfg.Serializers
	}
	if cfg.UseSchemas != nil {
		s.useSchemas = *cfg.UseSchemas
	}
	origins := cfg.CORSOrigins
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	s.allowedOrigins = origins

	readBuf := cfg.ReadBufferSize
	if readBuf == 0 {
		readBuf = 4096
	}
	writeBuf := cfg.WriteBufferSize
	if writeBuf == 0 {
		writeBuf = 4096
	}
	s.upgrader = websocket.Upgrader{
		ReadBufferSize:  readBuf,
		WriteBufferSize: writeBuf,
		// Origin enforcement happens in httpHandler before Upgrade is
		// called, so the upgrader itself accepts everything it sees.
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	s.Of("/")

	if cfg.Port > 0 {
		mux := http.NewServeMux()
		mux.HandleFunc("/", s.httpHandler)
		addr := fmt.Sprintf(":%d", cfg.Port)
		s.httpServer = &http.Server{Addr: addr, Handler: mux}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Printf("[BitSocket] failed to listen on port %d: %v", cfg.Port, err)
		} else {
			s.listener = ln
			go func() {
				if serveErr := s.httpServer.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
					log.Printf("[BitSocket] server error: %v", serveErr)
				}
			}()
			log.Printf("[BitSocket Core Server Engine Initialized on Port %d]", cfg.Port)
		}
	}

	return s
}

// Handler returns an http.HandlerFunc that upgrades requests to BitSocket
// connections. Mount it into your own mux/router when not using Config.Port.
func (s *Server) Handler() http.HandlerFunc {
	return s.httpHandler
}

func (s *Server) httpHandler(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if !s.originAllowed(origin) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.handleConnection(conn, r)
}

func (s *Server) originAllowed(origin string) bool {
	for _, o := range s.allowedOrigins {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

func (s *Server) handleConnection(conn *websocket.Conn, r *http.Request) {
	hs := Handshake{Headers: r.Header.Clone(), URL: r.URL.String(), Time: time.Now()}
	hub := newConnHub(conn, s, hs)

	rootSock := newSocket(conn, s, "/", hs, hub)
	rootNs := s.Of("/")

	// Start reading immediately, mirroring bit-socket-node where the
	// message listener is attached synchronously on connection, racing with
	// async root middleware. If middleware later rejects the connection it
	// closes the conn, which simply ends the read loop.
	go hub.readLoop()

	rootNs.runMiddlewares(rootSock, func(passed bool) {
		if !passed {
			return
		}
		rootNs.addSocket(rootSock)
		hub.mu.Lock()
		hub.sockets["/"] = rootSock
		hub.mu.Unlock()

		var connectPayload interface{}
		if s.useSchemas {
			connectPayload = s.exportAllSchemas()
		}
		buf, err := protocol.EncodeFrame(protocol.FrameOptions{
			Type: protocol.FrameConnect, Nsp: "/", Payload: connectPayload,
		}, s.serializers)
		if err == nil {
			rootSock.send(buf)
		}

		rootNs.fireConnection(rootSock)
	})
}

// Of returns the namespace named name, creating it if it doesn't exist yet.
func (s *Server) Of(name string) *Namespace {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, ok := s.namespaces[name]
	if !ok {
		ns = newNamespace(name, s)
		s.namespaces[name] = ns
	}
	return ns
}

// Use registers connection middleware on the root ("/") namespace.
func (s *Server) Use(mw ConnMiddleware) *Server {
	s.Of("/").Use(mw)
	return s
}

// OnConnection registers a connection handler on the root ("/") namespace.
func (s *Server) OnConnection(h ConnectionHandler) *Server {
	s.Of("/").OnConnection(h)
	return s
}

// Emit broadcasts event/payload to every socket on the root namespace.
func (s *Server) Emit(event string, payload interface{}) {
	s.Of("/").Emit(event, payload)
}

// To returns an Emitter scoped to room on the given namespace (defaults to
// root "/" if nsp is omitted).
func (s *Server) To(room string, nsp ...string) *Emitter {
	name := "/"
	if len(nsp) > 0 && nsp[0] != "" {
		name = nsp[0]
	}
	s.mu.RLock()
	ns, ok := s.namespaces[name]
	s.mu.RUnlock()
	if !ok {
		return &Emitter{}
	}
	return ns.To(room)
}

// Schemas returns a snapshot of every namespace's registered schemas, keyed
// by "root" for "/" and by the namespace name (without leading slash)
// otherwise.
func (s *Server) Schemas() map[string]map[string]*protocol.Schema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]map[string]*protocol.Schema, len(s.namespaces))
	for nsp, ns := range s.namespaces {
		ns.schemasMu.RLock()
		cp := make(map[string]*protocol.Schema, len(ns.schemas))
		for k, v := range ns.schemas {
			cp[k] = v
		}
		ns.schemasMu.RUnlock()
		out[schemaExportKey(nsp)] = cp
	}
	return out
}

func (s *Server) exportAllSchemas() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]interface{}, len(s.namespaces))
	for nsp, ns := range s.namespaces {
		out[schemaExportKey(nsp)] = ns.exportSchemas()
	}
	return out
}

func schemaExportKey(nsp string) string {
	if nsp == "/" {
		return "root"
	}
	return strings.TrimPrefix(nsp, "/")
}

// Close shuts down the server's internal HTTP listener, if it owns one
// (i.e. it was started with Config.Port > 0). Servers attached via
// Handler() to an externally-owned http.Server are not affected; close that
// server yourself.
func (s *Server) Close() error {
	if s.httpServer != nil {
		return s.httpServer.Close()
	}
	return nil
}
