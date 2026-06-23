package main

import (
	"fmt"
	"time"

	"github.com/ananay-nag/bit-socket-go/client"
)

func main() {
	fmt.Println("[Golang Client A] Starting...")
	c1 := client.New("ws://localhost:6001", client.Options{
		AutoReconnect: func() *bool { b := false; return &b }(),
	})

	c1.On("step1", func(payload interface{}) {
		fmt.Println("[Golang Client A] Received step1, payload:", payload)
		
		fmt.Println("[Golang Client A] Connecting to Python Server B (ws://localhost:6002)...")
		c2 := client.New("ws://localhost:6002", client.Options{
			AutoReconnect: func() *bool { b := false; return &b }(),
		})

		c2.On("connect", func(data interface{}) {
			fmt.Println("[Golang Client A] Connected to Python Server B, emitting step2...")
			c2.Emit("step2", map[string]interface{}{"msg": "hello from golang client A"})
			
			go func() {
				time.Sleep(1 * time.Second)
				c2.Close()
				c1.Close()
				fmt.Println("[Golang Client A] Disconnected and exiting.")
			}()
		})
	})

	// Block main
	select {}
}
