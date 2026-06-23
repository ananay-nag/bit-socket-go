// Command client demonstrates a BitSocket client connecting to a secured
// namespace, listening for schema-synced events, and emitting with an ack
// callback. It is a Go port of bit-socket-node's test/manual/client-app.js.
package main

import (
	"log"
	"net/http"
	"time"

	"bitsocket/client"
)

func main() {
	headers := http.Header{"X-Auth-Token": {"enterprise-payload-passkey"}}

	c := client.New("ws://localhost:5000", client.Options{
		Nsp:     "/secure-gateway",
		Headers: headers,
	})
	defer c.Close()

	c.On("connect", func(payload interface{}) {
		log.Println("[client] connected")

		c.EmitAck("cluster:provision", map[string]interface{}{
			"endpoints": map[string]interface{}{
				"createUser": map[string]interface{}{
					"serverType": "GRPC",
				},
			},
		}, func(resp interface{}) {
			log.Printf("[client] provision ack: %+v", resp)
		})
	})

	c.On("cluster:sync", func(payload interface{}) {
		log.Printf("[client] cluster:sync: %+v", payload)
	})

	c.On("disconnect", func(payload interface{}) {
		log.Println("[client] disconnected")
	})

	c.On("reconnecting", func(payload interface{}) {
		log.Printf("[client] reconnecting, attempt %v", payload)
	})

	time.Sleep(10 * time.Second)
}
