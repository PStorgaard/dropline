package stream

import (
	"encoding/binary"
)

// Header is the 32-byte wire header prepended to every loss-test UDP packet.
// Layout (big-endian):
//
//	bytes  0..3   magic       u32  constant 0x4452504C ("DRPL")
//	bytes  4..7   flow_id     u32  randomized per session
//	bytes  8..15  seq         u64  monotonic, starts at 0
//	bytes 16..23  tx_unix_ns  i64  sender wall clock at send time
//	bytes 24..31  reserved    u64  zero in v1
type Header struct {
	Magic    uint32
	FlowID   uint32
	Seq      uint64
	TxUnixNS int64
	Reserved uint64
}

// Magic is the constant first u32 of every loss-test packet ("DRPL").
const Magic uint32 = 0x4452504C

// HeaderSize is the on-wire size of Header in bytes.
const HeaderSize = 32

// MaxPacketSize is the largest UDP payload accepted (RFC 768 maximum).
const MaxPacketSize = 65507

// MinPacketSize is the smallest valid loss-test packet — header only, no body.
const MinPacketSize = HeaderSize

// EncodeHeader writes h into the first HeaderSize bytes of buf. The buffer
// must be at least HeaderSize long.
func EncodeHeader(buf []byte, h Header) {
	_ = buf[HeaderSize-1] // bounds check hint
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint32(buf[4:8], h.FlowID)
	binary.BigEndian.PutUint64(buf[8:16], h.Seq)
	binary.BigEndian.PutUint64(buf[16:24], uint64(h.TxUnixNS)) // #nosec G115 -- two's-complement reinterpret is intentional
	binary.BigEndian.PutUint64(buf[24:32], h.Reserved)
}

// DecodeHeader parses the first HeaderSize bytes of buf into a Header.
// Returns ErrShortPacket if buf is too short and ErrBadMagic if the magic
// constant does not match. Both errors are returned as the sentinels
// directly (no fmt.Errorf wrap) so the drainer's hot path discarding
// errors on stray UDP traffic doesn't allocate per bad packet.
func DecodeHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, ErrShortPacket
	}
	h := Header{
		Magic:    binary.BigEndian.Uint32(buf[0:4]),
		FlowID:   binary.BigEndian.Uint32(buf[4:8]),
		Seq:      binary.BigEndian.Uint64(buf[8:16]),
		TxUnixNS: int64(binary.BigEndian.Uint64(buf[16:24])), // #nosec G115 -- two's-complement reinterpret is intentional
		Reserved: binary.BigEndian.Uint64(buf[24:32]),
	}
	if h.Magic != Magic {
		return Header{}, ErrBadMagic
	}
	return h, nil
}
