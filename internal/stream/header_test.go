package stream

import (
	"errors"
	"reflect"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{
		Magic:    Magic,
		FlowID:   0xDEADBEEF,
		Seq:      0x1122334455667788,
		TxUnixNS: 1_715_000_000_000_000_000,
		Reserved: 0,
	}
	buf := make([]byte, HeaderSize)
	EncodeHeader(buf, h)
	got, err := DecodeHeader(buf)
	if err != nil {
		t.Fatalf("DecodeHeader: %v", err)
	}
	if !reflect.DeepEqual(h, got) {
		t.Fatalf("round-trip mismatch:\n want %#v\n  got %#v", h, got)
	}
}

func TestDecodeHeaderShort(t *testing.T) {
	_, err := DecodeHeader(make([]byte, HeaderSize-1))
	if !errors.Is(err, ErrShortPacket) {
		t.Fatalf("want ErrShortPacket, got %v", err)
	}
}

func TestDecodeHeaderBadMagic(t *testing.T) {
	buf := make([]byte, HeaderSize)
	EncodeHeader(buf, Header{Magic: 0xCAFEBABE})
	_, err := DecodeHeader(buf)
	if !errors.Is(err, ErrBadMagic) {
		t.Fatalf("want ErrBadMagic, got %v", err)
	}
}

// Sanity-check the on-wire layout: magic is the first 4 bytes big-endian.
func TestHeaderWireLayout(t *testing.T) {
	buf := make([]byte, HeaderSize)
	EncodeHeader(buf, Header{Magic: Magic, FlowID: 1})
	want := []byte{0x44, 0x52, 0x50, 0x4C} // "DRPL"
	if !reflect.DeepEqual(buf[0:4], want) {
		t.Fatalf("magic bytes: want %x, got %x", want, buf[0:4])
	}
}
