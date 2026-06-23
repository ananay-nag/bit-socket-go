package e2e

import (
	"bytes"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	bsclient "bitsocket/client"
	bsserver "bitsocket/server"
)

func TestCombinedInteroperabilityChain(t *testing.T) {
	// 1. Start Golang Server C on port 6003
	io := bsserver.New(bsserver.Config{})
	rootNsp := io.Of("/")
	rootNsp.OnConnection(func(socket *bsserver.Socket) {
		socket.On("step4", func(payload interface{}, ack bsserver.AckFunc) {
			// Emit step5 to Python Client C
			rootNsp.Emit("step5", map[string]interface{}{"msg": "hello from golang server C"})
			if ack != nil {
				ack(map[string]interface{}{"ok": true})
			}
		})
	})

	srv := &http.Server{
		Addr:    ":6003",
		Handler: io.Handler(),
	}
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Close()

	// Helper to spawn processes and ensure they get killed on test completion
	var cmds []*exec.Cmd
	defer func() {
		for _, cmd := range cmds {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		}
	}()

	startProcess := func(name string, args ...string) *exec.Cmd {
		cmd := exec.Command(name, args...)
		cmd.Dir = "/home/ananay/REDHEART_PERSONAL/PROJECTS/bit-socket/bit-socket-lang/combine"
		return cmd
	}

	// 2. Start Node Server A & Python Server B
	nodeServerCmd := startProcess("node", "node_server_a.js")
	if err := nodeServerCmd.Start(); err != nil {
		t.Fatalf("Failed to start Node Server A: %v", err)
	}

	pythonServerCmd := startProcess("python3", "python_server_b.py")
	if err := pythonServerCmd.Start(); err != nil {
		t.Fatalf("Failed to start Python Server B: %v", err)
	}

	// Wait for servers to spin up
	time.Sleep(2500 * time.Millisecond)

	// 3. Start Python Client C (we will capture its stdout to verify "step5_success")
	pythonClientCmd := startProcess("python3", "python_client_c.py")
	var pyStdout bytes.Buffer
	pythonClientCmd.Stdout = &pyStdout
	if err := pythonClientCmd.Start(); err != nil {
		t.Fatalf("Failed to start Python Client C: %v", err)
	}

	// 4. Start Node Client B
	nodeClientCmd := startProcess("node", "node_client_b.js")
	if err := nodeClientCmd.Start(); err != nil {
		t.Fatalf("Failed to start Node Client B: %v", err)
	}

	// Wait a moment for clients to connect
	time.Sleep(1000 * time.Millisecond)

	// 5. Connect Golang Client A (ws://localhost:6001)
	c1 := bsclient.New("ws://localhost:6001", bsclient.Options{
		AutoReconnect: func() *bool { b := false; return &b }(),
	})

	c1.On("step1", func(payload interface{}) {
		// Connect to Python Server B and emit step2
		c2 := bsclient.New("ws://localhost:6002", bsclient.Options{
			AutoReconnect: func() *bool { b := false; return &b }(),
		})

		c2.On("connect", func(data interface{}) {
			c2.Emit("step2", map[string]interface{}{"msg": "hello from golang client A"})
			go func() {
				time.Sleep(1 * time.Second)
				c2.Close()
				c1.Close()
			}()
		})
	})
	defer c1.Close()

	// Wait for the python client to finish and print "step5_success"
	success := false
	timeout := time.After(12 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for combined integration test success")
		case <-ticker.C:
			if strings.Contains(pyStdout.String(), "step5_success") {
				success = true
				break
			}
		}
		if success {
			break
		}
	}
}
