package main

import (
	"fmt"
	"net/http"

	"github.com/ananay-nag/bit-socket-go/server"
)

func main() {
	fmt.Println("[Golang Server C] Starting...")
	io := server.New(server.Config{})

	rootNsp := io.Of("/")
	rootNsp.OnConnection(func(socket *server.Socket) {
		fmt.Println("[Golang Server C] Client connected, Socket ID:", socket.ID)

		socket.On("step4", func(payload interface{}, ack server.AckFunc) {
			fmt.Println("[Golang Server C] Received step4:", payload)
			
			fmt.Println("[Golang Server C] Emitting step5 to Python Client C...")
			rootNsp.Emit("step5", map[string]interface{}{"msg": "hello from golang server C"})
			
			if ack != nil {
				ack(map[string]interface{}{"ok": true})
			}
		})
	})

	http.HandleFunc("/", io.Handler())
	fmt.Println("[Golang Server C] Listening on port 6003")
	err := http.ListenAndServe(":6003", nil)
	if err != nil {
		fmt.Println("Server error:", err)
	}
}
