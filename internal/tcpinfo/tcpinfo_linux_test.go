//go:build linux

package tcpinfo

import (
	"net"
	"testing"
)

// TestSampleOnLoopback opens a TCP connection to a loopback listener,
// pushes a small payload, and verifies Sample() returns no error and a
// non-zero BytesOut. Retransmits are expected to be zero on loopback.
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
		// Drain everything the client sends so the kernel keeps
		// transmitting.
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
	if stats.BytesOut == 0 {
		t.Errorf("BytesOut = 0; expected > 0 after a 64KB write")
	}
	// Loopback should not retransmit. A non-zero value would still be
	// "legal" output but indicates a misalignment / wrong field.
	if stats.BytesRetrans != 0 {
		t.Logf("BytesRetrans = %d on loopback (informational)", stats.BytesRetrans)
	}
}
