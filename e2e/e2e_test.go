package e2e

import (
	"sync"
	"testing"
	"time"

	bsclient "github.com/ananay-nag/bit-socket-go/client"
	"github.com/ananay-nag/bit-socket-go/protocol"
	bsserver "github.com/ananay-nag/bit-socket-go/server"
)

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for condition")
}

func TestE2E_ConnectEmitAckRoomsSchemaSync(t *testing.T) {
	syncSchema := protocol.MustNewSchema("SYNC_TEST", protocol.Object(
		protocol.F("id", protocol.Uint32),
		protocol.F("label", protocol.String),
	))

	io := bsserver.New(bsserver.Config{Port: 18181})
	defer io.Close()

	secure := io.Of("/secure-gateway")
	secure.Schema(syncSchema)

	secure.Use(func(sock *bsserver.Socket, next func(error)) {
		token := sock.Handshake.Headers.Get("X-Auth-Token")
		if token == "enterprise-payload-passkey" {
			next(nil)
		} else {
			next(errAuth{})
		}
	})

	var serverGotJoin sync.WaitGroup
	serverGotJoin.Add(1)

	secure.OnConnection(func(sock *bsserver.Socket) {
		sock.On("cluster:provision", func(payload interface{}, ack bsserver.AckFunc) {
			m := payload.(map[string]interface{})
			room, _ := m["room"].(string)
			sock.Join(room)
			serverGotJoin.Done()
			if ack != nil {
				ack(map[string]interface{}{"status": 200})
			}
		})

		sock.OnSchema(syncSchema, func(payload interface{}, ack bsserver.AckFunc) {
			m := payload.(map[string]interface{})
			sock.To("GRPC").Emit("cluster:sync", map[string]interface{}{
				"id":    m["id"],
				"label": m["label"],
			})
		})
	})

	time.Sleep(100 * time.Millisecond) // let listener bind

	headers := map[string][]string{"X-Auth-Token": {"enterprise-payload-passkey"}}

	c1 := bsclient.New("ws://127.0.0.1:18181", bsclient.Options{Nsp: "/secure-gateway", Headers: headers})
	defer c1.Close()
	c2 := bsclient.New("ws://127.0.0.1:18181", bsclient.Options{Nsp: "/secure-gateway", Headers: headers})
	defer c2.Close()

	var c1Connected, c2Connected bool
	c1.On("connect", func(payload interface{}) { c1Connected = true })
	c2.On("connect", func(payload interface{}) { c2Connected = true })

	waitFor(t, 2*time.Second, func() bool { return c1Connected && c2Connected })

	// schema auto-sync: client should now know SYNC_TEST without manual registration
	waitFor(t, 2*time.Second, func() bool {
		_, ok := c1.GetSchema("SYNC_TEST")
		return ok
	})

	// c2 joins GRPC room via plain event + ack
	ackCh := make(chan interface{}, 1)
	c2.EmitAck("cluster:provision", map[string]interface{}{"room": "GRPC"}, func(payload interface{}) {
		ackCh <- payload
	})

	select {
	case resp := <-ackCh:
		m := resp.(map[string]interface{})
		if int(toI(m["status"])) != 200 {
			t.Fatalf("unexpected ack payload: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ack")
	}
	serverGotJoin.Wait()

	// c2 listens for the broadcast triggered by c1's schema-encoded emit
	syncCh := make(chan interface{}, 1)
	c2.On("cluster:sync", func(payload interface{}) { syncCh <- payload })

	schema, ok := c1.GetSchema("SYNC_TEST")
	if !ok {
		t.Fatalf("c1 missing auto-synced schema")
	}
	c1.EmitAck(schema.Name, map[string]interface{}{"id": 77, "label": "node-7"}, nil)

	select {
	case payload := <-syncCh:
		m := payload.(map[string]interface{})
		if toI(m["id"]) != 77 || m["label"] != "node-7" {
			t.Fatalf("unexpected sync payload: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cluster:sync broadcast")
	}
}

func toI(v interface{}) int64 {
	switch t := v.(type) {
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case int:
		return int64(t)
	case int64:
		return t
	case uint64:
		return int64(t)
	case float32:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return -1
	}
}

type errAuth struct{}

func (errAuth) Error() string { return "ERR_AUTH_FAILURE" }
