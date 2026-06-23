package protocol

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeFrame_NoPayload(t *testing.T) {
	opts := FrameOptions{Type: FrameEvent, Nsp: "/test", Event: "hello", AckID: 0, Payload: nil}
	encoded, err := EncodeFrame(opts, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeFrame(encoded, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Type != opts.Type || decoded.Nsp != opts.Nsp || decoded.Event != opts.Event || decoded.AckID != opts.AckID {
		t.Fatalf("mismatch: %+v", decoded)
	}
	if decoded.Payload != nil {
		t.Fatalf("expected nil payload, got %v", decoded.Payload)
	}
}

func TestEncodeDecodeFrame_MapPayload(t *testing.T) {
	payload := map[string]interface{}{"user": "test", "id": int8(123)}
	opts := FrameOptions{Type: FrameEvent, Nsp: "/", Event: "data", AckID: 42, Payload: payload}

	encoded, err := EncodeFrame(opts, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeFrame(encoded, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Type != opts.Type || decoded.Nsp != opts.Nsp || decoded.Event != opts.Event || decoded.AckID != opts.AckID {
		t.Fatalf("mismatch: %+v", decoded)
	}
	dm, ok := decoded.Payload.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", decoded.Payload)
	}
	if dm["user"] != "test" {
		t.Fatalf("user mismatch: %v", dm["user"])
	}
	if toInt64(dm["id"]) != 123 {
		t.Fatalf("id mismatch: %v", dm["id"])
	}
}

func TestDecodeFrameHeader_TooShort(t *testing.T) {
	if _, _, err := DecodeFrameHeader([]byte{}); err == nil {
		t.Fatalf("expected error for empty frame")
	}
}

func TestFrameDefaultNamespace(t *testing.T) {
	buf, err := EncodeFrame(FrameOptions{Type: FramePing}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	f, err := DecodeFrame(buf, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if f.Nsp != "/" {
		t.Fatalf("expected default nsp '/', got %q", f.Nsp)
	}
}

func TestArrayAndNestedRoundtrip(t *testing.T) {
	payload := map[string]interface{}{
		"list": []interface{}{1, "two", map[string]interface{}{"three": 3}},
	}
	buf, err := EncodeFrame(FrameOptions{Type: FrameEvent, Event: "dyn", Payload: payload}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	f, err := DecodeFrame(buf, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(f.Event, "dyn") {
		t.Fatalf("event mismatch")
	}
}
