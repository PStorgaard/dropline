package control

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxFrameSize bounds the length-prefix value accepted on the wire. Frames
// larger than this are rejected before any body bytes are written or read.
// The largest legitimate v1 message (Final with full burst histogram) is
// under 1 KB; 64 KiB leaves several orders of magnitude of headroom while
// capping the allocation a malicious peer can request from a 4-byte header.
const MaxFrameSize = 64 << 10

// WriteFrame writes a 4-byte big-endian length followed by payload.
// Returns ErrFrameTooLarge without writing any bytes if the payload exceeds
// MaxFrameSize.
func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload))) // #nosec G115 -- bounded by MaxFrameSize check above
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("control: write header: %w", err)
	}
	if len(payload) == 0 {
		return nil
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("control: write payload: %w", err)
	}
	return nil
}

// ReadFrame reads a length-prefixed frame. Returns ErrFrameTooLarge without
// draining the body if the announced length exceeds MaxFrameSize — the caller
// is expected to close the connection.
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("control: read header: %w", err)
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if n == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("control: read payload: %w", err)
	}
	return buf, nil
}

// WriteMessage marshals m as JSON and writes it as a single frame.
func WriteMessage(w io.Writer, m Message) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("control: marshal: %w", err)
	}
	return WriteFrame(w, payload)
}

// ReadMessage reads a single frame and decodes it into the concrete Message
// type indicated by its "type" discriminator. Unknown type values return
// ErrUnknownType wrapping the raw type string.
func ReadMessage(r io.Reader) (Message, error) {
	payload, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	var env struct {
		Type Type `json:"type"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("control: decode envelope: %w", err)
	}
	var msg Message
	switch env.Type {
	case TypeHello:
		msg = &Hello{}
	case TypeReady:
		msg = &Ready{}
	case TypeError:
		msg = &Error{}
	case TypeStats:
		msg = &Stats{}
	case TypeFinal:
		msg = &Final{}
	case TypeReverseHopUpdate:
		msg = &ReverseHopUpdate{}
	case TypePause:
		msg = &Pause{}
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownType, string(env.Type))
	}
	if err := json.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("control: decode %s: %w", env.Type, err)
	}
	return msg, nil
}
