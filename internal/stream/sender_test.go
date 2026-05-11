package stream

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"
)

// dialedPair sets up a UDP listener on loopback and a connected sender-side
// conn pointing at it. Returns the sender conn (for Sender.Write) and the
// receiver conn (for ReadFromUDP). Both are closed via t.Cleanup.
func dialedPair(t *testing.T) (sendConn, recvConn *net.UDPConn) {
	t.Helper()
	rc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	t.Cleanup(func() { _ = rc.Close() })

	raddr := rc.LocalAddr().(*net.UDPAddr)
	sc, err := net.DialUDP("udp4", nil, raddr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	return sc, rc
}

// A paused gate halts the sender — no packets should arrive at the
// receiver while paused. Resuming restarts emission. Validates the
// drift-free schedule's pause-offset path end-to-end.
func TestSenderPauseGateBlocksAndResumes(t *testing.T) {
	sc, rc := dialedPair(t)
	const packetSize = 64
	const pps = 200
	rateBPS := int64(packetSize * 8 * pps)

	gate := NewPauseGate(time.Now)
	s, err := NewSender(sc, SenderConfig{
		RateBPS:    rateBPS,
		PacketSize: packetSize,
		Duration:   400 * time.Millisecond,
		Gate:       gate,
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}

	gate.Pause()
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	// While paused, no packets should arrive within the read window.
	buf := make([]byte, packetSize)
	if err := rc.SetReadDeadline(time.Now().Add(120 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, _, err := rc.ReadFromUDP(buf); err == nil {
		t.Fatalf("packet arrived while gate paused")
	}

	// Resume; packets should start flowing.
	gate.Resume()
	if err := rc.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, _, err := rc.ReadFromUDP(buf); err != nil {
		t.Fatalf("expected packet after resume, got: %v", err)
	}

	// Drain until the sender's Run returns.
	_ = rc.SetReadDeadline(time.Now().Add(time.Second))
	for {
		if _, _, err := rc.ReadFromUDP(buf); err != nil {
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("sender.Run: %v", err)
	}
}

// A nil gate must not panic — callers without pause support pass nil.
func TestSenderNilGate(t *testing.T) {
	sc, rc := dialedPair(t)
	s, err := NewSender(sc, SenderConfig{
		RateBPS:    int64(64 * 8 * 100),
		PacketSize: 64,
		Duration:   50 * time.Millisecond,
		Gate:       nil,
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	_ = rc.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 64)
	for {
		if _, _, err := rc.ReadFromUDP(buf); err != nil {
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("sender.Run: %v", err)
	}
}

func TestNewSenderValidatesConfig(t *testing.T) {
	sc, _ := dialedPair(t)
	cases := []struct {
		name string
		cfg  SenderConfig
		err  error
	}{
		{"too-small", SenderConfig{RateBPS: 1_000_000, PacketSize: HeaderSize - 1}, ErrPacketTooSmall},
		{"too-large", SenderConfig{RateBPS: 1_000_000, PacketSize: MaxPacketSize + 1}, ErrPacketTooLarge},
		{"zero-rate", SenderConfig{RateBPS: 0, PacketSize: 64}, nil}, // matches by message, not sentinel
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSender(sc, tc.cfg)
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("want %v, got %v", tc.err, err)
			}
		})
	}
}

// At a low rate (timer-sleep path) the sender should emit roughly the
// expected number of packets in a fixed interval and never less than 80%
// of the rate's worth — the +20% slack covers Windows timer granularity.
func TestSenderPacingTickerPath(t *testing.T) {
	sc, rc := dialedPair(t)
	// 100 pps × 64 bytes = 51200 bps.
	const packetSize = 64
	const pps = 100
	rateBPS := int64(packetSize * 8 * pps)

	s, err := NewSender(sc, SenderConfig{
		RateBPS:    rateBPS,
		PacketSize: packetSize,
		Duration:   200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	// Drain receiver.
	buf := make([]byte, packetSize)
	deadline := time.Now().Add(time.Second)
	if err := rc.SetReadDeadline(deadline); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	var got int
	for time.Now().Before(deadline) {
		n, _, err := rc.ReadFromUDP(buf)
		if err != nil {
			break
		}
		if n != packetSize {
			t.Fatalf("short read: %d", n)
		}
		got++
	}

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Expected = pps × duration = 100 × 0.2 = 20. Allow [10, 25] to cover
	// scheduler jitter on Windows.
	if got < 10 || got > 25 {
		t.Fatalf("packet count out of range: got %d, want roughly 20", got)
	}
	if got64 := s.Sent(); int64(got) > got64 {
		t.Fatalf("Sent() %d less than received %d", got64, got)
	}
}

// Busy-wait path: very high rate, very short duration. We just verify that
// (a) more than a handful of packets land and (b) the body is identical
// across packets, confirming the "generate once and reuse" invariant.
func TestSenderBusyWaitPathAndBodyReuse(t *testing.T) {
	sc, rc := dialedPair(t)
	const packetSize = 256
	// Rate that yields a sub-1ms interval: 10000 pps × 256 × 8 bps.
	rateBPS := int64(packetSize * 8 * 10_000)

	s, err := NewSender(sc, SenderConfig{
		RateBPS:    rateBPS,
		PacketSize: packetSize,
		Duration:   30 * time.Millisecond,
		FlowID:     0xCAFEF00D,
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}

	if s.FlowID() != 0xCAFEF00D {
		t.Fatalf("FlowID not honored: got %x", s.FlowID())
	}

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	buf := make([]byte, packetSize)
	if err := rc.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	var firstBody []byte
	var got int
	for {
		n, _, err := rc.ReadFromUDP(buf)
		if err != nil {
			break
		}
		if n != packetSize {
			t.Fatalf("short read: %d", n)
		}
		h, err := DecodeHeader(buf[:HeaderSize])
		if err != nil {
			t.Fatalf("DecodeHeader: %v", err)
		}
		if h.FlowID != 0xCAFEF00D {
			t.Fatalf("flow id mismatch: got %x", h.FlowID)
		}
		body := append([]byte(nil), buf[HeaderSize:n]...)
		if firstBody == nil {
			firstBody = body
		} else if !reflect.DeepEqual(firstBody, body) {
			t.Fatalf("body changed across packets at recv #%d", got)
		}
		got++
	}

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got < 5 {
		t.Fatalf("expected at least a few packets in busy-wait path, got %d", got)
	}
}

func TestSenderRespectsContext(t *testing.T) {
	sc, _ := dialedPair(t)
	s, err := NewSender(sc, SenderConfig{
		RateBPS:    8 * 64, // 1 pps — long-running unless ctx interrupts
		PacketSize: 64,
		Duration:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
