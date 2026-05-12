//go:build !windows

package probe

import (
	"encoding/binary"
	"net"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// tcpReplyKind is the TCP prober's view of an inbound ICMP message.
// The wire-level kinds are the same as parse.go's replyKind, but the
// demux key is different: TCP probes are matched by the embedded TCP
// source port (which the TCPProber chose per probe and bound on the
// dial socket), not by an ICMP id+seq pair. A separate type avoids
// retrofitting the ICMP Prober's parse code to carry a "what kind of
// inner packet" tag.
type tcpReplyKind int

const (
	tcpReplyIgnore tcpReplyKind = iota
	tcpReplyTimeExceeded
)

// tcpReply is the TCP prober's classification of one inbound ICMP
// message. srcPort is the *original* TCP source port the prober chose
// when dialing — extracted from the embedded original IPv4+TCP header
// the router copies into TimeExceeded per RFC 792. The prober looks up
// inflight by srcPort to map back to the originating TTL.
type tcpReply struct {
	kind    tcpReplyKind
	srcPort uint16
	src     net.IP
}

// parseTCPReply classifies an inbound ICMPv4 message for the TCP-mode
// hop prober. Only TimeExceeded carrying an embedded TCP header is
// relevant; everything else (EchoReply, destination unreachable, our
// own ICMP-prober traffic that the kernel also delivered to this raw
// socket) is ignored. TCP terminus detection happens on the dial side
// (successful connect or ECONNREFUSED), not via ICMP, so there is no
// EchoReply analogue here.
func parseTCPReply(buf []byte, src net.IP) tcpReply {
	msg, err := icmp.ParseMessage(protocolICMP, buf)
	if err != nil {
		return tcpReply{kind: tcpReplyIgnore}
	}
	if msg.Type != ipv4.ICMPTypeTimeExceeded {
		return tcpReply{kind: tcpReplyIgnore}
	}
	te, ok := msg.Body.(*icmp.TimeExceeded)
	if !ok {
		return tcpReply{kind: tcpReplyIgnore}
	}
	port, innerProto, ok := embeddedTCPSrcPort(te.Data)
	if !ok || innerProto != protocolTCP {
		return tcpReply{kind: tcpReplyIgnore}
	}
	return tcpReply{kind: tcpReplyTimeExceeded, srcPort: port, src: src}
}

// protocolTCP is the IANA protocol number for TCP. Matched against the
// embedded original IP header's Protocol field so we don't latch onto a
// TimeExceeded whose inner packet was an ICMP echo from the ICMP prober
// sharing this process (both probers read the same raw socket — see
// CLAUDE.md note on per-prober raw ICMP sockets).
const protocolTCP = 6

// embeddedTCPSrcPort parses the data field of a TimeExceeded reply
// whose inner packet is TCP. Mirrors embeddedEchoIDSeq's IHL math from
// parse.go: the inner IP header's IHL gives the offset of the inner
// transport header, the first two bytes of which are the TCP source
// port (the port we bound on dial; the demux key). innerProto is the
// inner IP header's Protocol field, returned so the caller can filter
// out non-TCP TimeExceeded (e.g. the ICMP prober's echoes that this
// process also receives copies of). RFC 792 guarantees the original IP
// header plus at least 8 bytes of the original transport header are
// preserved, which is exactly the 4 bytes (src port + dst port) we
// need.
func embeddedTCPSrcPort(data []byte) (srcPort uint16, innerProto uint8, ok bool) {
	if len(data) < 1 {
		return 0, 0, false
	}
	ihl := int(data[0]&0x0f) * 4
	if ihl < 20 || len(data) < ihl+4 {
		return 0, 0, false
	}
	// IPv4 header byte 9 is the Protocol field.
	innerProto = data[9]
	tcpHdr := data[ihl : ihl+4]
	srcPort = binary.BigEndian.Uint16(tcpHdr[0:2])
	return srcPort, innerProto, true
}
