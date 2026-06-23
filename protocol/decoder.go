package protocol

import (
	"encoding/binary"
	"fmt"
)

// Frame is a fully-decoded BitSocket wire frame.
type Frame struct {
	Type    byte
	Nsp     string
	Event   string
	AckID   uint32
	Payload interface{}
}

// FrameHeader is the fixed/variable-length part of a frame excluding the
// payload, returned by DecodeFrameHeader for callers that need to inspect
// type/nsp/event before deciding how to interpret the payload bytes (e.g.
// the client special-cases FrameConnect payloads, which carry schema
// definitions rather than a normal msgpack-encoded value).
type FrameHeader struct {
	Type  byte
	Nsp   string
	Event string
	AckID uint32
}

// DecodeFrameHeader parses everything up to (but not including) the payload
// and returns the remaining raw payload bytes unparsed.
func DecodeFrameHeader(raw []byte) (*FrameHeader, []byte, error) {
	off := 0
	if len(raw) < off+1 {
		return nil, nil, fmt.Errorf("bitsocket: frame too short (type)")
	}
	typ := raw[off]
	off++

	if len(raw) < off+1 {
		return nil, nil, fmt.Errorf("bitsocket: frame too short (nsp length)")
	}
	nspLen := int(raw[off])
	off++
	if len(raw) < off+nspLen {
		return nil, nil, fmt.Errorf("bitsocket: frame too short (nsp)")
	}
	nsp := string(raw[off : off+nspLen])
	off += nspLen

	if len(raw) < off+1 {
		return nil, nil, fmt.Errorf("bitsocket: frame too short (event length)")
	}
	eventLen := int(raw[off])
	off++
	if len(raw) < off+eventLen {
		return nil, nil, fmt.Errorf("bitsocket: frame too short (event)")
	}
	event := string(raw[off : off+eventLen])
	off += eventLen

	if len(raw) < off+4 {
		return nil, nil, fmt.Errorf("bitsocket: frame too short (ackId)")
	}
	ackID := binary.BigEndian.Uint32(raw[off : off+4])
	off += 4

	return &FrameHeader{Type: typ, Nsp: nsp, Event: event, AckID: ackID}, raw[off:], nil
}

// DecodeFrame parses raw into a fully-decoded Frame, using ser to interpret
// the payload bytes. If ser is nil, DefaultSerializers() is used.
func DecodeFrame(raw []byte, ser *Serializers) (*Frame, error) {
	if ser == nil {
		ser = DefaultSerializers()
	}
	hdr, payloadBytes, err := DecodeFrameHeader(raw)
	if err != nil {
		return nil, err
	}

	var payload interface{}
	if len(payloadBytes) > 0 {
		if ser.DecodePayloadWithEvent != nil {
			payload, err = ser.DecodePayloadWithEvent(payloadBytes, hdr.Event, hdr.Nsp)
		} else {
			payload, err = ser.DecodePayload(payloadBytes)
		}
		if err != nil {
			return nil, err
		}
	}

	return &Frame{Type: hdr.Type, Nsp: hdr.Nsp, Event: hdr.Event, AckID: hdr.AckID, Payload: payload}, nil
}
