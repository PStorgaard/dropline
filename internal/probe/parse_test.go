package probe

import (
	"encoding/binary"
	"net"
	"testing"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// makeOriginalEcho builds the IPv4 header + ICMP echo header that gets
// echoed back inside a TimeExceeded reply's data field. Just enough
// bytes for our parser — IHL=5 (20-byte v4 header) + 8-byte ICMP echo
// header (type=8 code=0 checksum=0 id seq).
func makeOriginalEcho(id, seq uint16) []byte {
	ipHdr := make([]byte, 20)
	ipHdr[0] = 0x45 // version=4 IHL=5
	icmpHdr := make([]byte, 8)
	icmpHdr[0] = 8 // type=Echo
	icmpHdr[1] = 0
	binary.BigEndian.PutUint16(icmpHdr[2:4], 0) // checksum (don't care)
	binary.BigEndian.PutUint16(icmpHdr[4:6], id)
	binary.BigEndian.PutUint16(icmpHdr[6:8], seq)
	return append(ipHdr, icmpHdr...)
}

func mustMarshalEchoReply(t *testing.T, id, seq int) []byte {
	t.Helper()
	msg := icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: &icmp.Echo{ID: id, Seq: seq},
	}
	b, err := msg.Marshal(nil)
	if err != nil {
		t.Fatalf("marshal echo reply: %v", err)
	}
	return b
}

func mustMarshalTimeExceeded(t *testing.T, original []byte) []byte {
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

func TestParseReply_EchoReplyMatching(t *testing.T) {
	const id = uint16(0xbeef)
	buf := mustMarshalEchoReply(t, int(id), 42)
	src := net.ParseIP("8.8.8.8")
	r := parseReply(buf, src, id)
	if r.kind != replyEchoReply {
		t.Fatalf("kind: got %v want replyEchoReply", r.kind)
	}
	if r.seq != 42 {
		t.Fatalf("seq: got %d want 42", r.seq)
	}
	if !r.src.Equal(src) {
		t.Fatalf("src: got %v want %v", r.src, src)
	}
}

func TestParseReply_EchoReplyMismatchedID(t *testing.T) {
	buf := mustMarshalEchoReply(t, 0xaaaa, 1)
	r := parseReply(buf, net.ParseIP("8.8.8.8"), 0xbbbb)
	if r.kind != replyIgnore {
		t.Fatalf("expected replyIgnore on id mismatch, got %v", r.kind)
	}
}

func TestParseReply_TimeExceededMatching(t *testing.T) {
	const id = uint16(0x1234)
	original := makeOriginalEcho(id, 7)
	buf := mustMarshalTimeExceeded(t, original)
	src := net.ParseIP("10.0.0.1")
	r := parseReply(buf, src, id)
	if r.kind != replyTimeExceeded {
		t.Fatalf("kind: got %v want replyTimeExceeded", r.kind)
	}
	if r.seq != 7 {
		t.Fatalf("seq: got %d want 7", r.seq)
	}
}

func TestParseReply_TimeExceededMismatchedID(t *testing.T) {
	original := makeOriginalEcho(0x1111, 5)
	buf := mustMarshalTimeExceeded(t, original)
	r := parseReply(buf, net.ParseIP("10.0.0.1"), 0x2222)
	if r.kind != replyIgnore {
		t.Fatalf("expected replyIgnore on id mismatch, got %v", r.kind)
	}
}

func TestParseReply_TruncatedEmbed(t *testing.T) {
	// IHL=5 means 20-byte header; supply only 24 bytes total — embedded
	// echo header is incomplete.
	short := make([]byte, 24)
	short[0] = 0x45
	buf := mustMarshalTimeExceeded(t, short)
	r := parseReply(buf, net.ParseIP("10.0.0.1"), 0x1111)
	if r.kind != replyIgnore {
		t.Fatalf("expected replyIgnore on truncated embed, got %v", r.kind)
	}
}

func TestParseReply_Garbage(t *testing.T) {
	r := parseReply([]byte{0xff, 0xff}, net.ParseIP("10.0.0.1"), 1)
	if r.kind != replyIgnore {
		t.Fatalf("expected replyIgnore on garbage, got %v", r.kind)
	}
}

func TestEmbeddedEchoIDSeq_HeaderTooShort(t *testing.T) {
	if _, _, ok := embeddedEchoIDSeq(nil); ok {
		t.Fatalf("expected ok=false on nil")
	}
	if _, _, ok := embeddedEchoIDSeq([]byte{0x45}); ok {
		t.Fatalf("expected ok=false on 1 byte")
	}
}
