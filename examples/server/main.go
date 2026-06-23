// Command server demonstrates a BitSocket server with a secured namespace,
// connection middleware, rooms, schema-based encoding, and acks. It is a Go
// port of bit-socket-node's test/manual/app.js.
package main

import (
	"errors"
	"log"

	"bitsocket/protocol"
	"bitsocket/server"
)

func main() {
	syncSchema := protocol.MustNewSchema("SYNC_TEST", protocol.Object(
		protocol.F("id", protocol.Uint32),
		protocol.F("label", protocol.String),
	))

	io := server.New(server.Config{Port: 5000})

	secure := io.Of("/secure-gateway")
	secure.Schema(syncSchema)

	secure.Use(func(sock *server.Socket, next func(error)) {
		token := sock.Handshake.Headers.Get("X-Auth-Token")
		if token == "enterprise-payload-passkey" {
			next(nil)
			return
		}
		next(errors.New("ERR_AUTH_FAILURE"))
	})

	secure.OnConnection(func(sock *server.Socket) {
		log.Printf("[secure-gateway] socket connected: %s", sock.ID)

		sock.On("cluster:provision", func(payload interface{}, ack server.AckFunc) {
			m, _ := payload.(map[string]interface{})
			endpoints, _ := m["endpoints"].(map[string]interface{})
			createUser, _ := endpoints["createUser"].(map[string]interface{})
			serverType, _ := createUser["serverType"].(string)

			sock.Join(serverType)
			io.To("GRPC", "/secure-gateway").Emit("cluster:sync", map[string]interface{}{
				"status": "provisioned",
			})

			if ack != nil {
				ack(map[string]interface{}{"status": 200})
			}
		})

		sock.OnSchema(syncSchema, func(payload interface{}, ack server.AckFunc) {
			m, _ := payload.(map[string]interface{})
			sock.To("GRPC").Emit("cluster:sync", m)
		})
	})

	select {} // block forever; Ctrl+C to stop
}
