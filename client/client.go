package client

import (
	"errors"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ananay-nag/bit-socket-go/protocol"
)

// Options configures a new Client. All fields are optional; zero values
// fall back to the same defaults as bit-socket-node's client.
type Options struct {
	// Nsp is the namespace this client primarily represents (the one whose
	// "connect"/"disconnect"/"reconnecting" lifecycle events fire heartbeat
	// setup). Defaults to "/".
	Nsp string

	// Serializers overrides the default msgpack+deflate payload codec.
	Serializers *protocol.Serializers

	// UseSchemas controls whether the client reconstructs Schemas from
	// CONNECT frame payloads sent by the server. Defaults to true.
	UseSchemas *bool

	// AutoReconnect controls whether the client automatically reconnects
	// (with exponential backoff) after an unexpected disconnect. Defaults
	// to true.
	AutoReconnect *bool

	MaxAttempts int           // default 15
	BaseDelay   time.Duration // default 1s
	MaxDelay    time.Duration // default 7s

	PingInterval time.Duration // default 20s
	PongTimeout  time.Duration // default 8s

	// Headers are sent with the initial WebSocket upgrade request (e.g. for
	// auth tokens consumed by server-side connection middleware).
	Headers http.Header
}

// Client is a BitSocket client connection, multiplexing one or more
// Namespaces over a single underlying WebSocket.
type Client struct {
	url         string
	serializers *protocol.Serializers
	useSchemas  bool

	autoReconnect     bool
	reconnectAttempts int
	maxAttempts       int
	baseDelay         time.Duration
	maxDelay          time.Duration

	pingInterval time.Duration
	pongTimeout  time.Duration

	headers http.Header
	dialer  *websocket.Dialer

	ackCallbacksMu sync.Mutex
	ackCallbacks   map[uint32]AckCallback
	ackCounter     uint32

	schemasMu   sync.RWMutex
	schemas     map[string]map[string]*protocol.Schema // nspKey -> eventName -> schema
	flatSchemas map[string]*protocol.Schema            // eventName -> schema (auto-synced, flattened across namespaces)

	namespacesMu sync.RWMutex
	namespaces   map[string]*Namespace

	nsp           string
	rootNamespace *Namespace

	connMu sync.Mutex
	conn   *websocket.Conn

	heartbeatMu   sync.Mutex
	pingTicker    *time.Ticker
	pongTimer     *time.Timer
	stopHeartbeat chan struct{}

	closedByUser bool
	closeMu      sync.Mutex
}

// New creates a Client and immediately begins connecting in the background
// (mirroring bit-socket-node, where the constructor kicks off an async
// WebSocket dial and returns before the connection completes). Register
// On("connect", ...) handlers right after calling New.
func New(url string, opts Options) *Client {
	c := &Client{
		url:           url,
		serializers:   protocol.DefaultSerializers(),
		useSchemas:    true,
		autoReconnect: true,
		maxAttempts:   15,
		baseDelay:     time.Second,
		maxDelay:      7 * time.Second,
		pingInterval:  20 * time.Second,
		pongTimeout:   8 * time.Second,
		ackCallbacks:  map[uint32]AckCallback{},
		ackCounter:    1,
		schemas:       map[string]map[string]*protocol.Schema{},
		flatSchemas:   map[string]*protocol.Schema{},
		namespaces:    map[string]*Namespace{},
		dialer:        websocket.DefaultDialer,
	}

	if opts.Serializers != nil {
		c.serializers = opts.Serializers
	}
	if opts.UseSchemas != nil {
		c.useSchemas = *opts.UseSchemas
	}
	if opts.AutoReconnect != nil {
		c.autoReconnect = *opts.AutoReconnect
	}
	if opts.MaxAttempts > 0 {
		c.maxAttempts = opts.MaxAttempts
	}
	if opts.BaseDelay > 0 {
		c.baseDelay = opts.BaseDelay
	}
	if opts.MaxDelay > 0 {
		c.maxDelay = opts.MaxDelay
	}
	if opts.PingInterval > 0 {
		c.pingInterval = opts.PingInterval
	}
	if opts.PongTimeout > 0 {
		c.pongTimeout = opts.PongTimeout
	}
	c.headers = opts.Headers

	c.nsp = opts.Nsp
	if c.nsp == "" {
		c.nsp = "/"
	}
	c.rootNamespace = newNamespace(c, c.nsp)
	c.namespaces[c.nsp] = c.rootNamespace

	go c.connect()
	return c
}

func (c *Client) ensureSchemaBucket(nspKey string) {
	c.schemasMu.Lock()
	defer c.schemasMu.Unlock()
	if c.schemas[nspKey] == nil {
		c.schemas[nspKey] = map[string]*protocol.Schema{}
	}
}

func (c *Client) registerSchemas(nsp string, schemas ...*protocol.Schema) {
	nspKey := schemaKey(nsp)
	c.schemasMu.Lock()
	defer c.schemasMu.Unlock()
	if c.schemas[nspKey] == nil {
		c.schemas[nspKey] = map[string]*protocol.Schema{}
	}
	for _, s := range schemas {
		c.schemas[nspKey][s.Name] = s
	}
}

// --- root namespace convenience delegation (mirrors `this.on = this.rootNamespace.on.bind(...)` etc.) ---

// On delegates to the root namespace's On.
func (c *Client) On(event string, handler EventHandler) *Client {
	c.rootNamespace.On(event, handler)
	return c
}

// Emit delegates to the root namespace's Emit.
func (c *Client) Emit(event string, payload interface{}) {
	c.rootNamespace.Emit(event, payload)
}

// EmitAck delegates to the root namespace's EmitAck.
func (c *Client) EmitAck(event string, payload interface{}, cb AckCallback) {
	c.rootNamespace.EmitAck(event, payload, cb)
}

// Join delegates to the root namespace's Join.
func (c *Client) Join(room string) { c.rootNamespace.Join(room) }

// Leave delegates to the root namespace's Leave.
func (c *Client) Leave(room string) { c.rootNamespace.Leave(room) }

// Schema delegates to the root namespace's Schema.
func (c *Client) Schema(schemas ...*protocol.Schema) *Client {
	c.rootNamespace.Schema(schemas...)
	return c
}

// Use delegates to the root namespace's Use.
func (c *Client) Use(mw Middleware) *Client {
	c.rootNamespace.Use(mw)
	return c
}

// GetSchema returns a server-auto-synced schema by event name, flattened
// across whichever namespace it was declared on (mirrors reading
// `client.schemas.EVENT_NAME` directly in the JS implementation).
func (c *Client) GetSchema(eventName string) (*protocol.Schema, bool) {
	c.schemasMu.RLock()
	defer c.schemasMu.RUnlock()
	s, ok := c.flatSchemas[eventName]
	return s, ok
}

// SchemasFor returns a snapshot of the schemas known for nsp (both manually
// registered and server auto-synced).
func (c *Client) SchemasFor(nsp string) map[string]*protocol.Schema {
	c.schemasMu.RLock()
	defer c.schemasMu.RUnlock()
	out := map[string]*protocol.Schema{}
	for k, v := range c.schemas[schemaKey(nsp)] {
		out[k] = v
	}
	return out
}

// Of returns the Namespace for nsp, creating and (if already connected)
// joining it on demand.
func (c *Client) Of(nsp string) *Namespace {
	c.namespacesMu.Lock()
	ns, ok := c.namespaces[nsp]
	if !ok {
		ns = newNamespace(c, nsp)
		c.namespaces[nsp] = ns
	}
	c.namespacesMu.Unlock()

	if !ok && nsp != "/" {
		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()
		if conn != nil {
			buf, err := protocol.EncodeFrame(protocol.FrameOptions{Type: protocol.FrameConnect, Nsp: nsp}, c.serializers)
			if err == nil {
				c.safeSend(buf)
			}
		}
	}
	return ns
}

func (c *Client) getNamespace(nsp string) *Namespace {
	c.namespacesMu.RLock()
	defer c.namespacesMu.RUnlock()
	return c.namespaces[nsp]
}

func (c *Client) safeSend(buf []byte) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return errors.New("bitsocket client: connection is closed")
	}
	return conn.WriteMessage(websocket.BinaryMessage, buf)
}

func (c *Client) connect() {
	conn, _, err := c.dialer.Dial(c.url, c.headers)
	if err != nil {
		c.handleClose()
		return
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	c.reconnectAttempts = 0

	// Re-join every non-root namespace already in use (mirrors ws.onopen).
	c.namespacesMu.RLock()
	nsps := make([]string, 0, len(c.namespaces))
	for nsp := range c.namespaces {
		if nsp != "/" {
			nsps = append(nsps, nsp)
		}
	}
	c.namespacesMu.RUnlock()
	for _, nsp := range nsps {
		buf, encErr := protocol.EncodeFrame(protocol.FrameOptions{Type: protocol.FrameConnect, Nsp: nsp}, c.serializers)
		if encErr == nil {
			_ = c.safeSend(buf)
		}
	}

	go c.readLoop(conn)
}

func (c *Client) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		c.handleMessage(data)
	}
	c.handleClose()
}

func (c *Client) handleMessage(raw []byte) {
	hdr, payloadBytes, err := protocol.DecodeFrameHeader(raw)
	if err != nil {
		log.Printf("[BitSocket Client] Error parsing incoming transmission package frame: %v", err)
		return
	}

	targetNsp := c.getNamespace(hdr.Nsp)
	if targetNsp == nil {
		return // Drop frame if no multiplexed client namespace exists for it
	}

	switch hdr.Type {
	case protocol.FrameConnect:
		if c.useSchemas && len(payloadBytes) > 0 {
			c.absorbSchemaPayload(hdr.Nsp, payloadBytes)
		}
		targetNsp.triggerEvent("connect", nil)
		if hdr.Nsp == c.nsp {
			c.setupHeartbeat()
		}
	case protocol.FrameEvent:
		payload, decErr := c.decodeAppPayload(payloadBytes, hdr.Event, hdr.Nsp)
		if decErr != nil {
			log.Printf("[BitSocket Client] Error parsing incoming transmission package frame: %v", decErr)
			return
		}
		targetNsp.triggerEvent(hdr.Event, payload)
	case protocol.FrameAck:
		payload, decErr := c.decodeAppPayload(payloadBytes, hdr.Event, hdr.Nsp)
		if decErr != nil {
			log.Printf("[BitSocket Client] Error parsing incoming transmission package frame: %v", decErr)
			return
		}
		c.ackCallbacksMu.Lock()
		cb, ok := c.ackCallbacks[hdr.AckID]
		if ok {
			delete(c.ackCallbacks, hdr.AckID)
		}
		c.ackCallbacksMu.Unlock()
		if ok {
			cb(payload)
		}
	case protocol.FramePong:
		c.clearPongTimeout()
	}
}

func (c *Client) decodeAppPayload(payloadBytes []byte, event, nsp string) (interface{}, error) {
	if len(payloadBytes) == 0 {
		return nil, nil
	}
	c.schemasMu.RLock()
	schema, ok := c.schemas[schemaKey(nsp)][event]
	c.schemasMu.RUnlock()
	if ok {
		return schema.DecodePayload(payloadBytes)
	}
	return c.serializers.DecodePayload(payloadBytes)
}

// absorbSchemaPayload reconstructs Schemas from a CONNECT frame's raw
// payload bytes, preserving field order (see protocol.DecodeRootSchemaPayload
// / DecodeNamespaceSchemaPayload for why a generic msgpack decode wouldn't
// be safe here).
func (c *Client) absorbSchemaPayload(nsp string, payloadBytes []byte) {
	if nsp == "/" {
		allDefs, err := protocol.DecodeRootSchemaPayload(payloadBytes)
		if err != nil {
			return
		}
		c.schemasMu.Lock()
		for nspKey, defs := range allDefs {
			if c.schemas[nspKey] == nil {
				c.schemas[nspKey] = map[string]*protocol.Schema{}
			}
			for eventName, def := range defs {
				schema, schemaErr := protocol.NewSchema(eventName, def)
				if schemaErr != nil {
					continue
				}
				c.schemas[nspKey][eventName] = schema
				c.flatSchemas[eventName] = schema // flatten, mirrors `this.schemas[eventName] = schema`
			}
		}
		c.schemasMu.Unlock()
		return
	}

	nspKey := schemaKey(nsp)
	defs, err := protocol.DecodeNamespaceSchemaPayload(payloadBytes)
	if err != nil {
		return
	}
	c.schemasMu.Lock()
	if c.schemas[nspKey] == nil {
		c.schemas[nspKey] = map[string]*protocol.Schema{}
	}
	for eventName, def := range defs {
		schema, schemaErr := protocol.NewSchema(eventName, def)
		if schemaErr != nil {
			continue
		}
		c.schemas[nspKey][eventName] = schema
		c.flatSchemas[eventName] = schema
	}
	c.schemasMu.Unlock()
}

func (c *Client) setupHeartbeat() {
	c.heartbeatMu.Lock()
	defer c.heartbeatMu.Unlock()

	if c.pingTicker != nil {
		c.pingTicker.Stop()
	}
	if c.stopHeartbeat != nil {
		close(c.stopHeartbeat)
	}
	stop := make(chan struct{})
	c.stopHeartbeat = stop
	c.pingTicker = time.NewTicker(c.pingInterval)
	ticker := c.pingTicker

	go func() {
		for {
			select {
			case <-stop:
				ticker.Stop()
				return
			case <-ticker.C:
				buf, err := protocol.EncodeFrame(protocol.FrameOptions{Type: protocol.FramePing, Nsp: "/"}, c.serializers)
				if err != nil {
					continue
				}
				if sendErr := c.safeSend(buf); sendErr != nil {
					continue
				}
				c.armPongTimeout()
			}
		}
	}()
}

func (c *Client) armPongTimeout() {
	c.heartbeatMu.Lock()
	if c.pongTimer != nil {
		c.pongTimer.Stop()
	}
	c.pongTimer = time.AfterFunc(c.pongTimeout, func() {
		log.Printf("[BitSocket Client] Ping-Pong Heartbeat Timeout detected. Restructuring channel link connection...")
		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	})
	c.heartbeatMu.Unlock()
}

func (c *Client) clearPongTimeout() {
	c.heartbeatMu.Lock()
	if c.pongTimer != nil {
		c.pongTimer.Stop()
		c.pongTimer = nil
	}
	c.heartbeatMu.Unlock()
}

func (c *Client) cleanupHeartbeat() {
	c.heartbeatMu.Lock()
	if c.pingTicker != nil {
		c.pingTicker.Stop()
		c.pingTicker = nil
	}
	if c.stopHeartbeat != nil {
		close(c.stopHeartbeat)
		c.stopHeartbeat = nil
	}
	if c.pongTimer != nil {
		c.pongTimer.Stop()
		c.pongTimer = nil
	}
	c.heartbeatMu.Unlock()
}

func (c *Client) handleClose() {
	c.connMu.Lock()
	c.conn = nil
	c.connMu.Unlock()

	c.cleanupHeartbeat()

	c.namespacesMu.RLock()
	nsps := make([]*Namespace, 0, len(c.namespaces))
	for _, ns := range c.namespaces {
		nsps = append(nsps, ns)
	}
	c.namespacesMu.RUnlock()
	for _, ns := range nsps {
		ns.triggerEvent("disconnect", nil)
	}

	c.closeMu.Lock()
	userClosed := c.closedByUser
	c.closeMu.Unlock()

	if !userClosed && c.autoReconnect && c.reconnectAttempts < c.maxAttempts {
		c.executeReconnectionSchedule()
	}
}

func (c *Client) executeReconnectionSchedule() {
	c.reconnectAttempts++
	attempt := c.reconnectAttempts
	delay := math.Min(
		float64(c.baseDelay)*math.Pow(1.5, float64(attempt))+rand.Float64()*300*float64(time.Millisecond),
		float64(c.maxDelay),
	)

	time.AfterFunc(time.Duration(delay), func() {
		c.namespacesMu.RLock()
		nsps := make([]*Namespace, 0, len(c.namespaces))
		for _, ns := range c.namespaces {
			nsps = append(nsps, ns)
		}
		c.namespacesMu.RUnlock()
		for _, ns := range nsps {
			ns.triggerEvent("reconnecting", attempt)
		}
		c.connect()
	})
}

func (c *Client) doEmit(eventOrSchemaName string, payload interface{}, cb AckCallback, targetNsp string) {
	var ackID uint32
	if cb != nil {
		c.ackCallbacksMu.Lock()
		ackID = c.ackCounter
		c.ackCounter++
		c.ackCallbacks[ackID] = cb
		c.ackCallbacksMu.Unlock()
	}

	nspKey := schemaKey(targetNsp)
	ser := c.serializers
	c.schemasMu.RLock()
	schema, ok := c.schemas[nspKey][eventOrSchemaName]
	c.schemasMu.RUnlock()
	if ok {
		ser = &protocol.Serializers{EncodePayload: schema.EncodePayload}
	}

	buf, err := protocol.EncodeFrame(protocol.FrameOptions{
		Type: protocol.FrameEvent, Nsp: targetNsp, Event: eventOrSchemaName, AckID: ackID, Payload: payload,
	}, ser)
	if err != nil {
		log.Printf("[BitSocket Client] Failed to encode event %q: %v", eventOrSchemaName, err)
		return
	}

	if sendErr := c.safeSend(buf); sendErr != nil {
		log.Printf("[BitSocket Client] Failed data delivery initialization: Pipeline currently in closed state window. [Event: %s]", eventOrSchemaName)
	}
}

func (c *Client) doJoin(room, nsp string) {
	buf, err := protocol.EncodeFrame(protocol.FrameOptions{
		Type: protocol.FrameJoin, Nsp: nsp, Payload: map[string]interface{}{"room": room},
	}, c.serializers)
	if err != nil {
		return
	}
	_ = c.safeSend(buf)
}

func (c *Client) doLeave(room, nsp string) {
	buf, err := protocol.EncodeFrame(protocol.FrameOptions{
		Type: protocol.FrameLeave, Nsp: nsp, Payload: map[string]interface{}{"room": room},
	}, c.serializers)
	if err != nil {
		return
	}
	_ = c.safeSend(buf)
}

// Close stops auto-reconnection and closes the underlying WebSocket
// connection.
func (c *Client) Close() {
	c.closeMu.Lock()
	c.closedByUser = true
	c.closeMu.Unlock()

	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}
