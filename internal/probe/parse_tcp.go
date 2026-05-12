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
// hop prober. Only TimeExceeded (code 0) carrying an embedded TCP
// header whose inner destination IP and port match this prober's
// (target, port) is relevant; everything else (EchoReply, destination
// unreachable, the sibling ICMP prober's traffic that the kernel also
// delivered to this raw socket, stray TCP flows that happen to alias a
// source port in our range, fragment-reassembly time-exceeded) is
// ignored. TCP terminus detection happens on the dial side (successful
// connect or ECONNREFUSED), not via ICMP, so there is no EchoReply
// analogue here.
func parseTCPReply(buf []byte, src net.IP, target net.IP, port uint16) tcpReply {
	msg, err := icmp.ParseMessage(protocolICMP, buf)
	if err != nil {
		return tcpReply{kind: tcpReplyIgnore}
	}
	if msg.Type != ipv4.ICMPTypeTimeExceeded {
		return tcpReply{kind: tcpReplyIgnore}
	}
	// ICMP code 0 is "TTL exceeded in transit" (the only code we care
	// about). Code 1 is "fragment reassembly time exceeded" — possible
	// in lossy networks but uninterpretable as a hop result.
	if msg.Code != 0 {
		return tcpReply{kind: tcpReplyIgnore}
	}
	te, ok := msg.Body.(*icmp.TimeExceeded)
	if !ok {
		return tcpReply{kind: tcpReplyIgnore}
	}
	h, innerProto, ok := embeddedTCPHeader(te.Data)
	if !ok || innerProto != protocolTCP {
		return tcpReply{kind: tcpReplyIgnore}
	}
	if !h.dstIP.Equal(target) || h.dstPort != port {
		return tcpReply{kind: tcpReplyIgnore}
	}
	return tcpReply{kind: tcpReplyTimeExceeded, srcPort: h.srcPort, src: src}
}

// protocolTCP is the IANA protocol number for TCP. Matched against the
// embedded original IP header's Protocol field so we don't latch onto a
// TimeExceeded whose inner packet was an ICMP echo from the ICMP prober
// sharing this process (both probers read the same raw socket — see
// CLAUDE.md note on per-prober raw ICMP sockets).
const protocolTCP = 6

// embeddedTCP is the subset of the embedded original IP+TCP header that
// the TCP prober uses for demux and validation. srcPort is the demux
// key into p.inflight; dstIP and dstPort are validated against the
// prober's configured target so stray TCP flows that happen to alias a
// source port in our range can't mis-attribute hops.
type embeddedTCP struct {
	srcPort, dstPort uint16
	dstIP            net.IP
}

// embeddedTCPHeader parses the data field of a TimeExceeded reply whose
// inner packet is TCP. Mirrors embeddedEchoIDSeq's IHL math from
// parse.go. RFC 792 guarantees the original IP header plus at least 8
// bytes of the original transport header are preserved; we only need
// the first 4 bytes of the TCP header (src port + dst port). innerProto
// is the inner IP header's Protocol field, returned so the caller can
// filter out non-TCP TimeExceeded.
func embeddedTCPHeader(data []byte) (h embeddedTCP, innerProto uint8, ok bool) {
	if len(data) < 20 {
		return embeddedTCP{}, 0, false
	}
	ihl := int(data[0]&0x0f) * 4
	if ihl < 20 || len(data) < ihl+4 {
		return embeddedTCP{}, 0, false
	}
	// IPv4 header byte 9 is the Protocol field; bytes 16..19 are the
	// destination address (the prober's target).
	innerProto = data[9]
	h.dstIP = net.IP(append(make([]byte, 0, 4), data[16:20]...))
	tcpHdr := data[ihl : ihl+4]
	h.srcPort = binary.BigEndian.Uint16(tcpHdr[0:2])
	h.dstPort = binary.BigEndian.Uint16(tcpHdr[2:4])
	return h, innerProto, true
}
