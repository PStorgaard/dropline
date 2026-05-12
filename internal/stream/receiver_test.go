package stream

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestNewReceiverValidatesConfig(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := NewReceiver(nil, ReceiverConfig{}); err == nil {
		t.Fatal("nil conn: want error, got nil")
	}
	if _, err := NewReceiver(conn, ReceiverConfig{RingSize: -1}); err == nil {
		t.Fatal("negative ring: want error, got nil")
	}
	if _, err := NewReceiver(conn, ReceiverConfig{}); err != nil {
		t.Fatalf("default config: unexpected error %v", err)
	}
}

// Receiver returns a final Snapshot after ctx cancel and exits cleanly,
// counting all packets that arrived before cancel.
func TestReceiverRunReturnsAfterCancel(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = conn.Close() }()

	r, err := NewReceiver(conn, ReceiverConfig{Tick: -1}) // no periodic emit
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}

	// Send 5 packets to the listener.
	addr := conn.LocalAddr().(*net.UDPAddr)
	sender, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sender.Close() }()
	buf := make([]byte, 64)
	for i := uint64(0); i < 5; i++ {
		EncodeHeader(buf, Header{Magic: Magic, FlowID: 1, Seq: i, TxUnixNS: time.Now().UnixNano()})
		if _, err := sender.Write(buf); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		s   Snapshot
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := r.Run(ctx)
		done <- result{s, err}
	}()

	// Give the drainer time to ingest. 250 ms covers two shutdown-poll cycles.
	time.Sleep(250 * time.Millisecond)
	cancel()

	select {
	case res := <-done:
		if res.err != nil && !errors.Is(res.err, net.ErrClosed) && !isTimeout(res.err) {
			t.Fatalf("Run returned unexpected error: %v", res.err)
		}
		if res.s.Recv != 5 {
			t.Fatalf("recv: want 5, got %d", res.s.Recv)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestReceiverFiltersByFlowID(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = conn.Close() }()

	r, err := NewReceiver(conn, ReceiverConfig{FlowID: 0xABCDEF01, Tick: -1})
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}

	addr := conn.LocalAddr().(*net.UDPAddr)
	sender, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sender.Close() }()
	buf := make([]byte, 64)

	// 3 matching, 2 non-matching.
	for i, fid := range []uint32{0xABCDEF01, 0xABCDEF01, 0xDEADBEEF, 0xABCDEF01, 0xDEADBEEF} {
		EncodeHeader(buf, Header{Magic: Magic, FlowID: fid, Seq: uint64(i), TxUnixNS: time.Now().UnixNano()})
		if _, err := sender.Write(buf); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		s   Snapshot
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := r.Run(ctx)
		done <- result{s, err}
	}()
	time.Sleep(250 * time.Millisecond)
	cancel()

	res := <-done
	if res.s.Recv != 3 {
		t.Fatalf("recv (matching flow only): want 3, got %d", res.s.Recv)
	}
}

// The final Snapshot returned by Run must report kernel_drops as the
// test-window delta (i.e. via the baseline-wrapped sampler), not the
// raw host-wide counter. Regression for a bug where the final read
// bypassed the wrapper and overwrote the accumulator with the absolute
// host UDP-error count since boot.
func TestReceiverFinalKernelDropsUsesBaseline(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// First sample latches baseline = 1000; every subsequent sample
	// returns 1050. Padded so the script outlasts any tick count.
	sampler := &scriptedSampler{
		values: []int64{1000, 1050, 1050, 1050, 1050, 1050, 1050, 1050, 1050, 1050, 1050, 1050},
	}
	r, err := NewReceiver(conn, ReceiverConfig{
		Tick:        30 * time.Millisecond,
		Snapshots:   make(chan Snapshot, 16),
		KernelDrops: sampler,
	})
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		s   Snapshot
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := r.Run(ctx)
		done <- result{s, err}
	}()

	// At least one tick to latch the baseline before cancel.
	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case res := <-done:
		if res.s.KernelDrops != 50 {
			t.Fatalf("final kernel_drops: want 50 (1050 - baseline 1000), got %d", res.s.KernelDrops)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestReceiverEmitsPeriodicSnapshots(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = conn.Close() }()

	snaps := make(chan Snapshot, 10)
	r, err := NewReceiver(conn, ReceiverConfig{
		Tick:      40 * time.Millisecond,
		Snapshots: snaps,
	})
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() { _, _ = r.Run(ctx) }()

	got := 0
	deadline := time.After(500 * time.Millisecond)
loop:
	for {
		select {
		case <-snaps:
			got++
			if got >= 2 {
				break loop
			}
		case <-deadline:
			break loop
		}
	}
	if got < 2 {
		t.Fatalf("expected ≥2 snapshots in 200ms, got %d", got)
	}
}

// TestReceiverFiltersByToken pins direct-conn receiver behavior: with
// ReceiverConfig.Token set, packets stamped with a different Token are
// silently dropped (no recv, no local-drop accounting).
func TestReceiverFiltersByToken(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = conn.Close() }()

	const expected uint64 = 0xCAFEBEEFDEADBEEF
	r, err := NewReceiver(conn, ReceiverConfig{Tick: -1, Token: expected})
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}

	addr := conn.LocalAddr().(*net.UDPAddr)
	sender, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sender.Close() }()

	buf := make([]byte, 64)
	// 3 mismatched (wrong token) + 2 matched. Only the 2 matched count.
	for i := uint64(0); i < 3; i++ {
		EncodeHeader(buf, Header{Magic: Magic, FlowID: 1, Seq: i, TxUnixNS: time.Now().UnixNano(), Token: 0x1})
		if _, err := sender.Write(buf); err != nil {
			t.Fatalf("Write mismatched: %v", err)
		}
	}
	for i := uint64(0); i < 2; i++ {
		EncodeHeader(buf, Header{Magic: Magic, FlowID: 1, Seq: i + 3, TxUnixNS: time.Now().UnixNano(), Token: expected})
		if _, err := sender.Write(buf); err != nil {
			t.Fatalf("Write matched: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		s   Snapshot
		err error
	}
	done := make(chan result, 1)
	go func() { s, err := r.Run(ctx); done <- result{s, err} }()

	time.Sleep(250 * time.Millisecond)
	cancel()

	res := <-done
	if res.err != nil && !errors.Is(res.err, net.ErrClosed) && !isTimeout(res.err) {
		t.Fatalf("Run: %v", res.err)
	}
	if res.s.Recv != 2 {
		t.Fatalf("recv: want 2 (only matched-token packets), got %d", res.s.Recv)
	}
}
