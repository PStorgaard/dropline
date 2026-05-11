package control

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte(`{"type":"hello","version":1}`)
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %q, want %q", got, payload)
	}
}

func TestFramePartialRead(t *testing.T) {
	var buf bytes.Buffer
	payload := bytes.Repeat([]byte("x"), 257)
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	// OneByteReader forces ReadFrame to reassemble across many Read calls.
	got, err := ReadFrame(iotest.OneByteReader(&buf))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch after partial reads")
	}
}

func TestFrameZeroLength(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, nil); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected zero-length payload, got %d bytes", len(got))
	}
}

func TestFrameOversizeRead(t *testing.T) {
	var buf bytes.Buffer
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], MaxFrameSize+1)
	buf.Write(hdr[:])
	body := []byte("body-bytes-not-drained")
	buf.Write(body)
	_, err := ReadFrame(&buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
	// The body must remain readable — ReadFrame must not drain on rejection.
	rest, _ := io.ReadAll(&buf)
	if !bytes.Equal(rest, body) {
		t.Fatalf("body was drained: got %q, want %q", rest, body)
	}
}

func TestFrameOversizeWrite(t *testing.T) {
	var buf bytes.Buffer
	payload := bytes.Repeat([]byte("x"), MaxFrameSize+1)
	err := WriteFrame(&buf, payload)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("WriteFrame must not write on oversize, got %d bytes", buf.Len())
	}
}
