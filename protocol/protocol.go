package protocol

import "github.com/vmihailenco/msgpack/v5"

// Serializers controls how a frame's payload bytes are produced/consumed.
// EncodePayload/DecodePayload are the generic (non-schema) codec. If
// DecodePayloadWithEvent is set, it takes priority over DecodePayload and
// receives the event name + namespace so a caller can look up a
// per-event Schema (this is how schema-aware decoding is plugged in by the
// server/client packages without this package needing to know about
// namespaces or schemas).
type Serializers struct {
	EncodePayload          func(payload interface{}) ([]byte, error)
	DecodePayload          func(buf []byte) (interface{}, error)
	DecodePayloadWithEvent func(buf []byte, event string, nsp string) (interface{}, error)
}

// DefaultEncodePayload msgpack-encodes then raw-deflates payload, matching
// bit-socket-node's defaultEncodePayload.
func DefaultEncodePayload(payload interface{}) ([]byte, error) {
	packed, err := msgpack.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return deflateRaw(packed)
}

// DefaultDecodePayload raw-inflates then msgpack-decodes buf, matching
// bit-socket-node's defaultDecodePayload.
func DefaultDecodePayload(buf []byte) (interface{}, error) {
	raw, err := inflateRaw(buf)
	if err != nil {
		return nil, err
	}
	var out interface{}
	if err := msgpack.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// DefaultSerializers returns a fresh Serializers using the default
// msgpack+deflate payload codec.
func DefaultSerializers() *Serializers {
	return &Serializers{
		EncodePayload: DefaultEncodePayload,
		DecodePayload: DefaultDecodePayload,
	}
}
