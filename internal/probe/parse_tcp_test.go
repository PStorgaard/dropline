//go:build !windows

package probe

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// makeOriginalTCP builds the IPv4 header + first 8 bytes of the TCP
// header that gets echoed back inside a TimeExceeded reply's data
// field. IHL=5 (20-byte v4 header), protocol=6 (TCP), inner dst IP set
// from dstIP (so parseTCPReply's target match can be exercised), then
// src port + dst port + 4 bytes of sequence number — the RFC 792
// minimum.
func makeOriginalTCP(srcPort, dstPort uint16, dstIP net.IP) []byte {
	ipHdr := make([]byte, 20)
	ipHdr[0] = 0x45 // version=4 IHL=5
	ipHdr[9] = 6    // protocol=TCP
	if v4 := dstIP.To4(); v4 != nil {
		copy(ipHdr[16:20], v4)
	}
	tcpHdr := make([]byte, 8)
	binary.BigEndian.PutUint16(tcpHdr[0:2], srcPort)
	binary.BigEndian.PutUint16(tcpHdr[2:4], dstPort)
	// remaining 4 bytes (sequence number) left zero
	return append(ipHdr, tcpHdr...)
}

func mustMarshalTCPTimeExceeded(t *testing.T, original []byte) []byte {
	t.Helper()
	return mustMarshalTCPTimeExceededCode(t, original, 0)
}

func mustMarshalTCPTimeExceededCode(t *testing.T, original []byte, code int) []byte {
	t.Helper()
	msg := icmp.Message{
		Type: ipv4.ICMPTypeTimeExceeded,
		Code: code,
		Body: &icmp.TimeExceeded{Data: original},
	}
	b, err := msg.Marshal(nil)
	if err != nil {
		t.Fatalf("marshal time exceeded: %v", err)
	}
	return b
}

func TestEmbeddedTCPHeader_Matching(t *testing.T) {
	target := net.ParseIP("203.0.113.7")
	original := makeOriginalTCP(0xc0de, 443, target)
	h, proto, ok := embeddedTCPHeader(original)
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if h.srcPort != 0xc0de {
		t.Fatalf("srcPort: got %d want %d", h.srcPort, 0xc0de)
	}
	if h.dstPort != 443 {
		t.Fatalf("dstPort: got %d want 443", h.dstPort)
	}
	if !h.dstIP.Equal(target) {
		t.Fatalf("dstIP: got %v want %v", h.dstIP, target)
	}
	if proto != protocolTCP {
		t.Fatalf("proto: got %d want %d", proto, protocolTCP)
	}
}

func TestEmbeddedTCPHeader_InnerNotTCP(t *testing.T) {
	// IPv4 header marking protocol=1 (ICMP) with a TCP-shaped payload —
	// embeddedTCPHeader should still parse the fields but the protocol
	// field signals "not TCP", so the caller filters this out.
	ipHdr := make([]byte, 20)
	ipHdr[0] = 0x45
	ipHdr[9] = 1 // ICMP
	inner := make([]byte, 8)
	binary.BigEndian.PutUint16(inner[0:2], 1234)
	original := append(ipHdr, inner...)
	_, proto, ok := embeddedTCPHeader(original)
	if !ok {
		t.Fatalf("expected ok=true (parses regardless of proto)")
	}
	if proto != 1 {
		t.Fatalf("proto: got %d want 1", proto)
	}
}

func TestEmbeddedTCPHeader_TooShort(t *testing.T) {
	if _, _, ok := embeddedTCPHeader(nil); ok {
		t.Fatalf("expected ok=false on nil")
	}
	if _, _, ok := embeddedTCPHeader([]byte{0x45}); ok {
		t.Fatalf("expected ok=false on 1 byte")
	}
	// IHL=5 (20-byte header) but only 22 bytes total — TCP src port
	// would need at least bytes 20..22, but dst port (22..24) is needed
	// for the 4-byte read; embeddedTCPHeader requires ihl+4 bytes total.
	short := make([]byte, 22)
	short[0] = 0x45
	short[9] = 6
	if _, _, ok := embeddedTCPHeader(short); ok {
		t.Fatalf("expected ok=false on truncated transport header")
	}
}

func TestParseTCPReply_Matching(t *testing.T) {
	const srcPort uint16 = 33500
	target := net.ParseIP("203.0.113.7")
	original := makeOriginalTCP(srcPort, 443, target)
	buf := mustMarshalTCPTimeExceeded(t, original)
	src := net.ParseIP("10.0.0.42")
	r := parseTCPReply(buf, src, target, 443)
	if r.kind != tcpReplyTimeExceeded {
		t.Fatalf("kind: got %v want tcpReplyTimeExceeded", r.kind)
	}
	if r.srcPort != srcPort {
		t.Fatalf("srcPort: got %d want %d", r.srcPort, srcPort)
	}
	if !r.src.Equal(src) {
		t.Fatalf("src: got %v want %v", r.src, src)
	}
}

func TestParseTCPReply_InnerICMPIgnored(t *testing.T) {
	// A TimeExceeded whose inner packet is ICMP (i.e., a leak from the
	// sibling ICMP prober on this process) must be ignored.
	original := makeOriginalEcho(0xbeef, 7) // protocol left zero — but we need protocol=1
	original[9] = 1                         // mark inner as ICMP
	buf := mustMarshalTCPTimeExceeded(t, original)
	r := parseTCPReply(buf, net.ParseIP("10.0.0.1"), net.ParseIP("203.0.113.7"), 443)
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore for inner-ICMP, got %v", r.kind)
	}
}

func TestParseTCPReply_EchoReplyIgnored(t *testing.T) {
	// EchoReply (not TimeExceeded) — TCP prober has no use for it.
	buf := mustMarshalEchoReply(t, 0xbeef, 1)
	r := parseTCPReply(buf, net.ParseIP("8.8.8.8"), net.ParseIP("203.0.113.7"), 443)
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore for echo reply, got %v", r.kind)
	}
}

func TestParseTCPReply_Garbage(t *testing.T) {
	r := parseTCPReply([]byte{0xff, 0xff}, net.ParseIP("10.0.0.1"), net.ParseIP("203.0.113.7"), 443)
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore on garbage, got %v", r.kind)
	}
}

// TestParseTCPReply_WrongDstIP guards finding-3: a TimeExceeded whose
// embedded inner-IP destination doesn't match the prober's target must
// be ignored, even when the inner-TCP src port happens to land in our
// range. This is the realistic failure mode under allocator overlap or
// stray TCP flows.
func TestParseTCPReply_WrongDstIP(t *testing.T) {
	target := net.ParseIP("203.0.113.7")
	other := net.ParseIP("198.51.100.9")
	original := makeOriginalTCP(33500, 443, other)
	buf := mustMarshalTCPTimeExceeded(t, original)
	r := parseTCPReply(buf, net.ParseIP("10.0.0.1"), target, 443)
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore for wrong inner dst IP, got %v", r.kind)
	}
}

// TestParseTCPReply_WrongDstPort is the dst-port analogue of the above.
func TestParseTCPReply_WrongDstPort(t *testing.T) {
	target := net.ParseIP("203.0.113.7")
	original := makeOriginalTCP(33500, 80, target) // wrong dst port
	buf := mustMarshalTCPTimeExceeded(t, original)
	r := parseTCPReply(buf, net.ParseIP("10.0.0.1"), target, 443)
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore for wrong inner dst port, got %v", r.kind)
	}
}

// TestParseTCPReply_ICMPCode1Ignored — code 1 is "fragment reassembly
// time exceeded", not "TTL exceeded in transit". Must not be folded
// into hop stats.
func TestParseTCPReply_ICMPCode1Ignored(t *testing.T) {
	target := net.ParseIP("203.0.113.7")
	original := makeOriginalTCP(33500, 443, target)
	buf := mustMarshalTCPTimeExceededCode(t, original, 1)
	r := parseTCPReply(buf, net.ParseIP("10.0.0.1"), target, 443)
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore for ICMP code 1, got %v", r.kind)
	}
}

// TestNextTCPSrcPortBase_DisjointAcrossMaxHops is the regression test
// for finding 2: two probers with different MaxHops constructed
// concurrently must own non-overlapping port ranges. Before the fix,
// the cursor counted probers and was multiplied by the caller's local
// rangeSize, so a small-MaxHops prober could sit inside a large
// prober's range.
func TestNextTCPSrcPortBase_DisjointAcrossMaxHops(t *testing.T) {
	tcpPortCursor.Store(0)
	rsLarge := computeTCPRangeSize(time.Second, 255)
	rsSmall := computeTCPRangeSize(time.Second, 30)
	a := uint32(nextTCPSrcPortBase(rsLarge))
	b := uint32(nextTCPSrcPortBase(rsSmall))
	rangeOverlaps := func(aStart, aEnd, bStart, bEnd uint32) bool {
		return aStart < bEnd && bStart < aEnd
	}
	if rangeOverlaps(a, a+rsLarge, b, b+rsSmall) {
		t.Fatalf("ranges overlap: large=[%d,%d) small=[%d,%d)",
			a, a+rsLarge, b, b+rsSmall)
	}
}

// TestNextTCPSrcPortBase_DisjointRanges keeps the original
// same-MaxHops regression but rewritten against the new slot-counting
// cursor.
func TestNextTCPSrcPortBase_DisjointRanges(t *testing.T) {
	tcpPortCursor.Store(0)
	const maxHops = 30
	rs := computeTCPRangeSize(time.Second, maxHops)
	a := nextTCPSrcPortBase(rs)
	b := nextTCPSrcPortBase(rs)
	if b == a {
		t.Fatalf("consecutive base ports collided: a=%d b=%d", a, b)
	}
	if uint32(b-a) != rs {
		t.Fatalf("expected gap %d between ranges, got %d (a=%d b=%d)",
			rs, uint32(b-a), a, b)
	}
}
