package protocol

// Frame types - wire-compatible with bit-socket-node's FRAME_* constants.
const (
	FrameConnect byte = 0x01
	FrameEvent   byte = 0x02
	FrameAck     byte = 0x03
	FramePing    byte = 0x04
	FramePong    byte = 0x05
	FrameJoin    byte = 0x06
	FrameLeave   byte = 0x07
)
