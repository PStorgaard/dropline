//go:build windows

package tcpinfo

import (
	"net"
	"testing"
	"unsafe"
)

// TestTCPInfoV0SizeIs88 guards the struct layout against drift. Any
// future field reorder or padding change must keep the byte length
// equal to the Microsoft TCP_INFO_v0 size (88 bytes) — otherwise the
// WSAIoctl call writes past the buffer end or returns truncated data.
func TestTCPInfoV0SizeIs88(t *testing.T) {
	if got := unsafe.Sizeof(tcpInfoV0{}); got != 88 {
		t.Errorf("sizeof tcpInfoV0 = %d, want 88", got)
	}
}

// TestSampleOnLoopback opens a TCP connection to a loopback listener,
// pushes a small payload, and verifies Sample() returns no error and a
// non-zero BytesOut. On hosts older than Win10 1703 (where
// SIO_TCP_INFO isn't implemented) the sampler downgrades to a nop and
// returns zeros — assertions are tolerant of that case.
func TestSampleOnLoopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 4096)
		for {
			if _, err := c.Read(buf); err != nil {
				return
			}
		}
	}()

	dial, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = dial.Close() }()
	tcpConn := dial.(*net.TCPConn)

	payload := make([]byte, 64*1024)
	if _, err := tcpConn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	sampler, err := New(tcpConn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = sampler.Close() }()

	stats, err := sampler.Sample()
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	// On a host with SIO_TCP_INFO support: BytesOut > 0 after a 64KB
	// write. On a host without: the downgrade path returned zeros and
	// nil error — accept that as "not supported on this host".
	if stats == (Stats{}) {
		t.Skip("Sample returned zero Stats; SIO_TCP_INFO likely unsupported on this host")
	}
	if stats.BytesOut == 0 {
		t.Errorf("BytesOut = 0; expected > 0 after a 64KB write")
	}
	if stats.BytesRetrans != 0 {
		t.Logf("BytesRetrans = %d on loopback (informational)", stats.BytesRetrans)
	}
}
