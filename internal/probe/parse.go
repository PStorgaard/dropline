package probe

import (
	"encoding/binary"
	"net"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// protocolICMP is the IANA protocol number for ICMPv4. Passed to
// icmp.ParseMessage; v1 is IPv4-only per spec § Scope.
const protocolICMP = 1

type replyKind int

const (
	replyIgnore replyKind = iota
	replyTimeExceeded
	replyEchoReply
)

// reply is the parser's classification of one inbound ICMP message.
// seq is the *original* echo sequence number (extracted from the
// embedded original packet for TimeExceeded, taken directly for
// EchoReply). src is the IP that emitted the reply (the hop, or the
// destination for an EchoReply).
type reply struct {
	kind replyKind
	seq  uint16
	src  net.IP
}

// parseReply classifies an inbound ICMPv4 message. expectedID is our
// echo identifier (low 16 bits of pid). Replies that do not carry our
// id in the embedded echo header are ignored — this filters out other
// processes' ping traffic on the same host.
func parseReply(buf []byte, src net.IP, expectedID uint16) reply {
	msg, err := icmp.ParseMessage(protocolICMP, buf)
	if err != nil {
		return reply{kind: replyIgnore}
	}
	switch body := msg.Body.(type) {
	case *icmp.Echo:
		if msg.Type != ipv4.ICMPTypeEchoReply {
			return reply{kind: replyIgnore}
		}
		// #nosec G115 -- ID/Seq fit in uint16 per ICMP wire format.
		if uint16(body.ID) != expectedID {
			return reply{kind: replyIgnore}
		}
		// #nosec G115
		return reply{kind: replyEchoReply, seq: uint16(body.Seq), src: src}
	case *icmp.TimeExceeded:
		id, seq, ok := embeddedEchoIDSeq(body.Data)
		if !ok || id != expectedID {
			return reply{kind: replyIgnore}
		}
		return reply{kind: replyTimeExceeded, seq: seq, src: src}
	}
	return reply{kind: replyIgnore}
}

// embeddedEchoIDSeq parses the data field of a TimeExceeded reply,
// which carries the original IPv4 header followed by the first 8 bytes
// of the original ICMP packet (RFC 792). Returns the embedded echo's
// identifier and sequence number, or ok=false on a malformed embed.
func embeddedEchoIDSeq(data []byte) (id, seq uint16, ok bool) {
	if len(data) < 1 {
		return 0, 0, false
	}
	// IHL is the low 4 bits of the first byte, in 32-bit words.
	ihl := int(data[0]&0x0f) * 4
	if ihl < 20 || len(data) < ihl+8 {
		return 0, 0, false
	}
	icmpHdr := data[ihl : ihl+8]
	// ICMP echo header: type(1) code(1) checksum(2) id(2 BE) seq(2 BE)
	id = binary.BigEndian.Uint16(icmpHdr[4:6])
	seq = binary.BigEndian.Uint16(icmpHdr[6:8])
	return id, seq, true
}
