package protocol

import "encoding/binary"

// FrameOptions are the inputs used to build one wire frame.
type FrameOptions struct {
	Type    byte
	Nsp     string // defaults to "/" if empty
	Event   string
	AckID   uint32
	Payload interface{} // nil means "no payload bytes at all"
}

// EncodeFrame serializes opts into the BitSocket binary frame layout:
//
//	[type:1][nspLen:1][nsp][eventLen:1][event][ackId:4][payload...]
//
// If ser is nil, DefaultSerializers() is used.
func EncodeFrame(opts FrameOptions, ser *Serializers) ([]byte, error) {
	if ser == nil {
		ser = DefaultSerializers()
	}
	nsp := opts.Nsp
	if nsp == "" {
		nsp = "/"
	}
	nspBytes := []byte(nsp)
	eventBytes := []byte(opts.Event)

	var payloadBytes []byte
	if opts.Payload != nil {
		var err error
		payloadBytes, err = ser.EncodePayload(opts.Payload)
		if err != nil {
			return nil, err
		}
	}

	frameSize := 1 + 1 + len(nspBytes) + 1 + len(eventBytes) + 4 + len(payloadBytes)
	buf := make([]byte, frameSize)
	off := 0

	buf[off] = opts.Type
	off++

	buf[off] = byte(len(nspBytes))
	off++
	copy(buf[off:], nspBytes)
	off += len(nspBytes)

	buf[off] = byte(len(eventBytes))
	off++
	copy(buf[off:], eventBytes)
	off += len(eventBytes)

	binary.BigEndian.PutUint32(buf[off:], opts.AckID)
	off += 4

	if len(payloadBytes) > 0 {
		copy(buf[off:], payloadBytes)
	}

	return buf, nil
}
