//go:build !windows

package probe

import (
	"encoding/binary"
	"net"
	"testing"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// makeOriginalTCP builds the IPv4 header + first 8 bytes of the TCP
// header that gets echoed back inside a TimeExceeded reply's data
// field. IHL=5 (20-byte v4 header), protocol=6 (TCP), then src port +
// dst port + 4 bytes of sequence number — the RFC 792 minimum.
func makeOriginalTCP(srcPort, dstPort uint16) []byte {
	ipHdr := make([]byte, 20)
	ipHdr[0] = 0x45 // version=4 IHL=5
	ipHdr[9] = 6    // protocol=TCP
	tcpHdr := make([]byte, 8)
	binary.BigEndian.PutUint16(tcpHdr[0:2], srcPort)
	binary.BigEndian.PutUint16(tcpHdr[2:4], dstPort)
	// remaining 4 bytes (sequence number) left zero
	return append(ipHdr, tcpHdr...)
}

func mustMarshalTCPTimeExceeded(t *testing.T, original []byte) []byte {
	t.Helper()
	msg := icmp.Message{
		Type: ipv4.ICMPTypeTimeExceeded,
		Code: 0,
		Body: &icmp.TimeExceeded{Data: original},
	}
	b, err := msg.Marshal(nil)
	if err != nil {
		t.Fatalf("marshal time exceeded: %v", err)
	}
	return b
}

func TestEmbeddedTCPSrcPort_Matching(t *testing.T) {
	original := makeOriginalTCP(0xc0de, 443)
	port, proto, ok := embeddedTCPSrcPort(original)
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if port != 0xc0de {
		t.Fatalf("port: got %d want %d", port, 0xc0de)
	}
	if proto != protocolTCP {
		t.Fatalf("proto: got %d want %d", proto, protocolTCP)
	}
}

func TestEmbeddedTCPSrcPort_InnerNotTCP(t *testing.T) {
	// IPv4 header marking protocol=1 (ICMP) with a TCP-shaped payload —
	// embeddedTCPSrcPort should still parse the port but the protocol
	// field signals "not TCP", so the caller filters this out.
	ipHdr := make([]byte, 20)
	ipHdr[0] = 0x45
	ipHdr[9] = 1 // ICMP
	inner := make([]byte, 8)
	binary.BigEndian.PutUint16(inner[0:2], 1234)
	original := append(ipHdr, inner...)
	_, proto, ok := embeddedTCPSrcPort(original)
	if !ok {
		t.Fatalf("expected ok=true (parses regardless of proto)")
	}
	if proto != 1 {
		t.Fatalf("proto: got %d want 1", proto)
	}
}

func TestEmbeddedTCPSrcPort_TooShort(t *testing.T) {
	if _, _, ok := embeddedTCPSrcPort(nil); ok {
		t.Fatalf("expected ok=false on nil")
	}
	if _, _, ok := embeddedTCPSrcPort([]byte{0x45}); ok {
		t.Fatalf("expected ok=false on 1 byte")
	}
	// IHL=5 (20-byte header) but only 22 bytes total — TCP src port
	// would need at least bytes 20..22, but dst port (22..24) is needed
	// for the 4-byte read; current implementation requires ihl+4 bytes
	// total.
	short := make([]byte, 22)
	short[0] = 0x45
	short[9] = 6
	if _, _, ok := embeddedTCPSrcPort(short); ok {
		t.Fatalf("expected ok=false on truncated transport header")
	}
}

func TestParseTCPReply_Matching(t *testing.T) {
	const srcPort uint16 = 33500
	original := makeOriginalTCP(srcPort, 443)
	buf := mustMarshalTCPTimeExceeded(t, original)
	src := net.ParseIP("10.0.0.42")
	r := parseTCPReply(buf, src)
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
	original[9] = 1                          // mark inner as ICMP
	buf := mustMarshalTCPTimeExceeded(t, original)
	r := parseTCPReply(buf, net.ParseIP("10.0.0.1"))
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore for inner-ICMP, got %v", r.kind)
	}
}

func TestParseTCPReply_EchoReplyIgnored(t *testing.T) {
	// EchoReply (not TimeExceeded) — TCP prober has no use for it.
	buf := mustMarshalEchoReply(t, 0xbeef, 1)
	r := parseTCPReply(buf, net.ParseIP("8.8.8.8"))
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore for echo reply, got %v", r.kind)
	}
}

func TestParseTCPReply_Garbage(t *testing.T) {
	r := parseTCPReply([]byte{0xff, 0xff}, net.ParseIP("10.0.0.1"))
	if r.kind != tcpReplyIgnore {
		t.Fatalf("expected tcpReplyIgnore on garbage, got %v", r.kind)
	}
}

func TestNextTCPSrcPortBase_DisjointRanges(t *testing.T) {
	// Each call should advance by maxHops * tcpProbeRangeFactor, so two
	// consecutive ranges don't overlap.
	const maxHops = 30
	a := nextTCPSrcPortBase(maxHops)
	b := nextTCPSrcPortBase(maxHops)
	if b == a {
		t.Fatalf("consecutive base ports collided: a=%d b=%d", a, b)
	}
	rangeSize := uint16(maxHops) * uint16(tcpProbeRangeFactor) // #nosec G115
	// b - a should equal rangeSize (modulo wrap, which is fine for small N).
	if b-a != rangeSize {
		t.Fatalf("expected gap %d between ranges, got %d (a=%d b=%d)",
			rangeSize, b-a, a, b)
	}
}
