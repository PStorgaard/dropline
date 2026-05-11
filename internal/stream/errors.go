package stream

import "errors"

// Sentinel errors returned by package stream. Callers can match with errors.Is.
var (
	ErrShortPacket    = errors.New("stream: packet shorter than header")
	ErrBadMagic       = errors.New("stream: bad magic")
	ErrPacketTooLarge = errors.New("stream: packet exceeds MaxPacketSize")
	ErrPacketTooSmall = errors.New("stream: packet smaller than HeaderSize")
)
