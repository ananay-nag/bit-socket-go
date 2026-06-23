# BitSocket Go Library Version & Features

## Version Information
- **Current Version**: `1.0.0`
- **Module Path**: `github.com/ananay-nag/bit-socket-go`
- **Target Platform**: Go >= 1.22

## Key Features

1. **Statically-Typed Schema Engine**
   Adapts BitSocket's positional binary schema design to Go. Utilizes explicit `protocol.Object` and `protocol.F` field helpers to ensure deterministic binary ordering (since Go map iteration order is randomized).

2. **Handshake Authentication & Middleware**
   Provides a `ConnMiddleware` signature `func(*Socket, func(error))` to enforce authentication checks during connection upgrades.

3. **Multiplexed Read Loop & Hub Routing**
   Because `gorilla/websocket` limits concurrent reads to a single goroutine per connection, the Go library implements a centralized `connHub` reader that coordinates decoding and routes frames to multiplexed namespace sockets safely.

4. **Goroutine-Safe Namespace & Room Scoping**
   Namespace and Room maps (`Namespace.sockets`, `Socket.rooms`) are protected by reader-writer mutexes, enabling concurrency-safe reads/writes across multiple background goroutines.

5. **Flexible Server Mounts**
   Can boot its own TCP listener by specifying a `Port` configuration, or output an `http.HandlerFunc` to be mounted directly into a custom `http.ServeMux` or router.

## Example Run Methods

The library provides examples inside the `examples/` directory:

### 1. Run the Example Go Server
Boot the example Go server running on port `5000`:
```bash
cd bit-socket-go
go run examples/server/main.go
```

### 2. Run the Example Go Client
Boot the example Go client that connects to the server, completes handshake authentication, and prints schema-synced payloads:
```bash
cd bit-socket-go
go run examples/client/main.go
```

### 3. Run the Automated Tests
Run unit tests checking protocol serialization, positional schemas, and multiplexed namespace end-to-end flows:
```bash
cd bit-socket-go
go test ./...
```
