package stream

import (
	"context"
	"net"
	"testing"
	"time"
)

// End-to-end: a real Sender feeds a real Receiver across loopback. The
// loopback path is lossless, so recv must equal sent and there must be
// zero out-of-order, duplicate, lost, or local-drop counts.
func TestLoopbackSenderReceiverNoLoss(t *testing.T) {
	rconn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = rconn.Close() }()

	addr := rconn.LocalAddr().(*net.UDPAddr)
	sconn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sconn.Close() }()

	const flowID uint32 = 0x5A5A5A5A
	const packetSize = 256
	const pps = 200
	rateBPS := int64(packetSize * 8 * pps)

	sender, err := NewSender(sconn, SenderConfig{
		RateBPS:    rateBPS,
		PacketSize: packetSize,
		Duration:   150 * time.Millisecond,
		FlowID:     flowID,
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}

	receiver, err := NewReceiver(rconn, ReceiverConfig{
		FlowID: flowID,
		Tick:   -1, // no periodic emit; we want only the final
	})
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}

	rctx, rcancel := context.WithCancel(context.Background())
	defer rcancel()

	type final struct {
		s   Snapshot
		err error
	}
	done := make(chan final, 1)
	go func() {
		s, err := receiver.Run(rctx)
		done <- final{s, err}
	}()

	// Run sender to completion.
	if err := sender.Run(context.Background()); err != nil {
		t.Fatalf("sender.Run: %v", err)
	}
	// Allow drainer + stats to absorb in-flight packets.
	time.Sleep(150 * time.Millisecond)
	rcancel()

	select {
	case res := <-done:
		sent := sender.Sent()
		if res.s.Recv != sent {
			t.Errorf("recv != sent: recv=%d sent=%d", res.s.Recv, sent)
		}
		if res.s.Lost != 0 {
			t.Errorf("lost: want 0, got %d", res.s.Lost)
		}
		if res.s.OutOfOrder != 0 {
			t.Errorf("out_of_order: want 0, got %d", res.s.OutOfOrder)
		}
		if res.s.Duplicates != 0 {
			t.Errorf("duplicates: want 0, got %d", res.s.Duplicates)
		}
		if res.s.LocalDrops != 0 {
			t.Errorf("local_drops: want 0, got %d", res.s.LocalDrops)
		}
		if res.s.Bursts.Max != 0 {
			t.Errorf("max_burst: want 0, got %d", res.s.Bursts.Max)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receiver.Run did not return")
	}
}
