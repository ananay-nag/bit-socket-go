<div align="center">
  <h1>⚡ BitSocket</h1>
  <p><strong>A high-performance, schema-driven, binary WebSocket framework for Go.</strong></p>
  <p>BitSocket provides the developer experience of Socket.io but with <b>Protobuf-level network compression</b>. By leveraging a strict Schema Engine, BitSocket drops JSON completely, stripping keys and formatting to deliver up to an 80% reduction in network payload size.</p>
</div>

<hr />

## 🚀 Why BitSocket?

Socket.io is built on top of Engine.io, which means it transmits stringified JSON. If you send an array of 100 user objects, the keys `"id"`, `"name"`, and `"email"` are transmitted 100 times. 

**BitSocket completely eliminates this overhead.** 
By defining Schemas on your server, BitSocket maps your Go data structures directly into strict binary formats. The keys are never transmitted over the network—only the pure, deeply compressed binary data.

### Features
- 🧬 **Schema Auto-Discovery**: Define your schemas on the server. The moment a client connects, the server pushes the schemas to the client during the handshake. Zero manual schema sharing required!
- 📦 **Extreme Binary Compression**: Drops all JSON overhead resulting in 40% to 80% smaller network payloads.
- 🔄 **Connection Multiplexing**: Share a single underlying TCP connection across multiple isolated Namespaces (e.g. `/user`, `/store`), exactly like Socket.io.
- 👥 **Room Broadcasting**: Full support for group communication (`socket.join('room')`, `io.to('room').emit(...)`).
- ♾️ **Recursive Data Types**: Native support for deeply nested objects and multi-dimensional arrays without losing compression.
- 🧩 **Dynamic MsgPack Fallbacks**: Need to send an arbitrary, unpredictable JSON dictionary? Define the field as `'object'` and BitSocket seamlessly drops down to MsgPack compression for that specific field.

---

## 🛠️ Installation

```bash
cd bit-socket-go
go build ./...
```

The module path is `github.com/ananay-nag/bit-socket-go`. Add it to your project dependencies by adding a `replace` directive in `go.mod` (e.g., `replace github.com/ananay-nag/bit-socket-go => ../bit-socket-go`).

---

## 📖 Quick Start

### 1. The Server
Define your schemas, attach them to a namespace, and start listening!

```go
package main

import (
	"errors"
	"net/http"

	"github.com/ananay-nag/bit-socket-go/protocol"
	"github.com/ananay-nag/bit-socket-go/server"
)

func main() {
	// 1. Define strict binary schemas
	syncSchema := protocol.MustNewSchema("SYNC_TEST", protocol.Object(
		protocol.F("id", protocol.Uint32),
		protocol.F("label", protocol.String),
	))

	// 2. Initialize Server
	io := server.New(server.Config{Port: 5000})

	// 3. Register Schema to namespace "/"
	secure := io.Of("/secure-gateway")
	secure.Schema(syncSchema)

	secure.Use(func(sock *server.Socket, next func(error)) {
		if sock.Handshake.Headers.Get("X-Auth-Token") == "enterprise-payload-passkey" {
			next(nil)
		} else {
			next(errors.New("ERR_AUTH_FAILURE"))
		}
	})

	// 4. Handle Connections
	secure.OnConnection(func(sock *server.Socket) {
		sock.Join("GRPC")

		sock.OnSchema(syncSchema, func(payload interface{}, ack server.AckFunc) {
			sock.To("GRPC").Emit("cluster:sync", payload)
			if ack != nil {
				ack(map[string]interface{}{"status": 200})
			}
		})
	})

	select {} // block forever
}
```

### 2. The Client
The client only needs to connect. **It automatically downloads the schemas during the connection handshake!**

```go
package main

import (
	"net/http"
	"time"

	"github.com/ananay-nag/bit-socket-go/client"
)

func main() {
	c := client.New("ws://localhost:5000", client.Options{
		Nsp:     "/secure-gateway",
		Headers: http.Header{"X-Auth-Token": {"enterprise-payload-passkey"}},
	})
	defer c.Close()

	c.On("connect", func(payload interface{}) {
		schema, _ := c.GetSchema("SYNC_TEST") // auto-synced from the server
		c.EmitAck(schema.Name, map[string]interface{}{"id": 1, "label": "node-1"}, func(resp interface{}) {
			// resp is the server's ack payload
		})
	})

	c.On("cluster:sync", func(payload interface{}) { /* ... */ })

	time.Sleep(10 * time.Second)
}
```

---

## 🧱 Supported Schema Types

BitSocket currently supports mapping your Go data into the following strict representations:

| Schema Type | Go Type | Byte Size |
|-------------|---------|-----------|
| `protocol.Uint8`   | `uint8`   | 1 byte    |
| `protocol.Boolean` | `bool`    | 1 byte    |
| `protocol.Uint16`  | `uint16`  | 2 bytes   |
| `protocol.Uint32`  | `uint32`  | 4 bytes   |
| `protocol.Int32`   | `int32`   | 4 bytes   |
| `protocol.Float64` | `float64` | 8 bytes   |
| `protocol.String`  | `string`  | 4 bytes (len) + utf8 bytes |
| `protocol.Bytes`   | `[]byte`  | 4 bytes (len) + buffer |

### Advanced Types
- **Arrays**: Wrap a type using `protocol.Array`. `protocol.Array(protocol.String)` or `protocol.Array(protocol.Array(protocol.Uint8))`.
- **Nested Objects**: Define using `protocol.Object`. `protocol.Object(protocol.F("profile", protocol.Object(protocol.F("age", protocol.Uint8))))`.
- **Dynamic Fallbacks**: Use `protocol.ObjectAny`, `protocol.ArrayAny`, or `protocol.Any` to allow arbitrary JSON data. BitSocket will compress this specific field using MsgPack while preserving keys.

---

## 🌐 Multiplexing & Rooms

BitSocket matches the elegant routing API of Socket.io:

**Namespaces (Multiplexing)**  
Keep logic separated without opening multiple TCP connections.
```go
chatNsp := root.Of("/chat")
gameNsp := root.Of("/game")
```

**Rooms**  
Create isolated communication channels within a namespace.
```go
// Server Side
socket.Join("lobby-1")
socket.Leave("lobby-1")

// Emit to everyone in the room EXCEPT the sender
socket.Broadcast().To("lobby-1").Emit("message", data)

// Emit to everyone in the room INCLUDING the sender
io.Of("/chat").To("lobby-1").Emit("message", data)
```

---

## 📈 Performance vs Socket.io
See the `NETWORK_ANALYSIS.md` document for a fully quantified byte-for-byte breakdown. In summary:
- **Single Objects**: ~40% smaller payloads.
- **Large Arrays**: ~50% to 80% smaller payloads.
- **Continuous Metrics**: ~60% smaller payloads.

Because network latency (I/O) is the slowest bottleneck in any real-time system, BitSocket provides lower end-to-end latency for high-frequency applications.

---

## 📦 Version History

For detailed features and run methods of each release, please refer to the VERSION file.

- [**v1.0.0**](./VERSION_V1_0_0.md) (2026-06-23) - Initial stable release containing core BitSocket Go server/client modules, handshake auth, multiplexed hub read loops, and concurrency-safe room mappings.
