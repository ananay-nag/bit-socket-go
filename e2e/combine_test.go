package e2e

import (
	"net/http"
	"strings"
	"testing"
	"time"

	bsclient "github.com/ananay-nag/bit-socket-go/client"
	bsserver "github.com/ananay-nag/bit-socket-go/server"
)

func TestCombinedInteroperabilityChain(t *testing.T) {
	// Step 1: Start Go Server A on Port 6001
	ioA := bsserver.New(bsserver.Config{})
	nspA := ioA.Of("/")
	nspA.OnConnection(func(socket *bsserver.Socket) {
		nspA.Emit("step1", map[string]interface{}{"msg": "hello from server A"})
	})
	srvA := &http.Server{Addr: ":6001", Handler: ioA.Handler()}
	go func() { _ = srvA.ListenAndServe() }()
	defer srvA.Close()

	// Step 3: Start Go Server B on Port 6002
	ioB := bsserver.New(bsserver.Config{})
	nspB := ioB.Of("/")
	nspB.OnConnection(func(socket *bsserver.Socket) {
		socket.On("step2", func(payload interface{}, ack bsserver.AckFunc) {
			nspB.Emit("step3", map[string]interface{}{"msg": "hello from server B"})
			if ack != nil {
				ack(map[string]interface{}{"ok": true})
			}
		})
	})
	srvB := &http.Server{Addr: ":6002", Handler: ioB.Handler()}
	go func() { _ = srvB.ListenAndServe() }()
	defer srvB.Close()

	// Step 5: Start Go Server C on Port 6003
	ioC := bsserver.New(bsserver.Config{})
	nspC := ioC.Of("/")
	nspC.OnConnection(func(socket *bsserver.Socket) {
		socket.On("step4", func(payload interface{}, ack bsserver.AckFunc) {
			nspC.Emit("step5", map[string]interface{}{"msg": "hello from server C"})
			if ack != nil {
				ack(map[string]interface{}{"ok": true})
			}
		})
	})
	srvC := &http.Server{Addr: ":6003", Handler: ioC.Handler()}
	go func() { _ = srvC.ListenAndServe() }()
	defer srvC.Close()

	// Wait for servers to bind
	time.Sleep(100 * time.Millisecond)

	// Step 6: Start Go Client C (subscribes to Server C, verifies step5)
	c3 := bsclient.New("ws://localhost:6003", bsclient.Options{
		AutoReconnect: func() *bool { b := false; return &b }(),
	})
	defer c3.Close()

	successChan := make(chan string, 1)
	c3.On("step5", func(payload interface{}) {
		m, _ := payload.(map[string]interface{})
		msg, _ := m["msg"].(string)
		if strings.Contains(msg, "hello from server C") {
			successChan <- "step5_success"
		}
	})

	// Step 4: Start Go Client B (subscribes to Server B, on step3 emits step4 to Server C)
	c2 := bsclient.New("ws://localhost:6002", bsclient.Options{
		AutoReconnect: func() *bool { b := false; return &b }(),
	})
	defer c2.Close()

	c2.On("step3", func(payload interface{}) {
		c2_to_c := bsclient.New("ws://localhost:6003", bsclient.Options{
			AutoReconnect: func() *bool { b := false; return &b }(),
		})
		c2_to_c.On("connect", func(data interface{}) {
			c2_to_c.Emit("step4", map[string]interface{}{"msg": "hello from client B"})
			go func() {
				time.Sleep(200 * time.Millisecond)
				c2_to_c.Close()
			}()
		})
	})

	// Step 2: Start Go Client A (subscribes to Server A, on step1 emits step2 to Server B)
	c1 := bsclient.New("ws://localhost:6001", bsclient.Options{
		AutoReconnect: func() *bool { b := false; return &b }(),
	})
	defer c1.Close()

	c1.On("step1", func(payload interface{}) {
		c1_to_b := bsclient.New("ws://localhost:6002", bsclient.Options{
			AutoReconnect: func() *bool { b := false; return &b }(),
		})
		c1_to_b.On("connect", func(data interface{}) {
			c1_to_b.Emit("step2", map[string]interface{}{"msg": "hello from client A"})
			go func() {
				time.Sleep(200 * time.Millisecond)
				c1_to_b.Close()
			}()
		})
	})

	// Wait for success output
	select {
	case result := <-successChan:
		if result != "step5_success" {
			t.Fatalf("Expected step5_success, got %v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for combined integration chain success")
	}
}
