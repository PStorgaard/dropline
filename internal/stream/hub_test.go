package stream

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// TestHubRoutesByFlowID stands up a single UDP socket, registers two
// flow_ids, runs a sender for each, and confirms each HubFlow only sees
// its own flow's observations.
func TestHubRoutesByFlowID(t *testing.T) {
	rconn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = rconn.Close() }()
	addr := rconn.LocalAddr().(*net.UDPAddr)

	hub := NewHub(rconn)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	hubDone := make(chan error, 1)
	go func() { hubDone <- hub.Run(hubCtx) }()

	const (
		flowA      uint32 = 0xAAAAAAAA
		flowB      uint32 = 0xBBBBBBBB
		packetSize        = 128
		pps               = 200
		duration          = 150 * time.Millisecond
	)
	rateBPS := int64(packetSize * 8 * pps)

	flowAHandle := hub.Register(flowA, DefaultRingSize)
	defer flowAHandle.Release()
	flowBHandle := hub.Register(flowB, DefaultRingSize)
	defer flowBHandle.Release()

	dial := func() *net.UDPConn {
		sconn, err := net.DialUDP("udp4", nil, addr)
		if err != nil {
			t.Fatalf("DialUDP: %v", err)
		}
		return sconn
	}
	sconnA := dial()
	defer func() { _ = sconnA.Close() }()
	sconnB := dial()
	defer func() { _ = sconnB.Close() }()

	mkSender := func(conn *net.UDPConn, flow uint32) *Sender {
		s, err := NewSender(conn, SenderConfig{
			RateBPS:    rateBPS,
			PacketSize: packetSize,
			Duration:   duration,
			FlowID:     flow,
		})
		if err != nil {
			t.Fatalf("NewSender: %v", err)
		}
		return s
	}
	senderA := mkSender(sconnA, flowA)
	senderB := mkSender(sconnB, flowB)

	// Start consumers per flow before senders so we don't lose early packets.
	var countA, countB int
	var consumeWG sync.WaitGroup
	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	defer consumeCancel()
	consumeWG.Add(2)
	go func() {
		defer consumeWG.Done()
		for {
			select {
			case obs, ok := <-flowAHandle.Observations():
				if !ok {
					return
				}
				if obs.h.FlowID != flowA {
					t.Errorf("flowA channel got cross-flow observation: %x", obs.h.FlowID)
				}
				countA++
			case <-consumeCtx.Done():
				return
			}
		}
	}()
	go func() {
		defer consumeWG.Done()
		for {
			select {
			case obs, ok := <-flowBHandle.Observations():
				if !ok {
					return
				}
				if obs.h.FlowID != flowB {
					t.Errorf("flowB channel got cross-flow observation: %x", obs.h.FlowID)
				}
				countB++
			case <-consumeCtx.Done():
				return
			}
		}
	}()

	sendCtx, sendCancel := context.WithTimeout(context.Background(), duration+100*time.Millisecond)
	defer sendCancel()
	var senderWG sync.WaitGroup
	senderWG.Add(2)
	go func() { defer senderWG.Done(); _ = senderA.Run(sendCtx) }()
	go func() { defer senderWG.Done(); _ = senderB.Run(sendCtx) }()
	senderWG.Wait()

	// Brief grace period for in-flight packets to drain through the hub.
	time.Sleep(100 * time.Millisecond)
	consumeCancel()
	consumeWG.Wait()

	// Each flow should have received the bulk of its packets. Loopback is
	// lossless, but we leave a generous margin for the brief timing slop
	// at the test's tail.
	expected := int(int64(duration) * int64(pps) / int64(time.Second))
	if countA < expected/2 {
		t.Errorf("flowA observations: want >= %d, got %d", expected/2, countA)
	}
	if countB < expected/2 {
		t.Errorf("flowB observations: want >= %d, got %d", expected/2, countB)
	}

	hubCancel()
	select {
	case <-hubDone:
	case <-time.After(time.Second):
		t.Fatal("hub.Run did not return within 1s of cancel")
	}
}

// TestHubRingOverflowAccountsLocalDrops fills a flow's ring without
// draining and checks PullLocalDrops returns the count of dropped
// observations.
func TestHubRingOverflowAccountsLocalDrops(t *testing.T) {
	rconn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = rconn.Close() }()
	addr := rconn.LocalAddr().(*net.UDPAddr)

	hub := NewHub(rconn)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go func() { _ = hub.Run(hubCtx) }()

	const flow uint32 = 0xDEADBEEF
	const ringSize = 1
	flowHandle := hub.Register(flow, ringSize)
	defer flowHandle.Release()

	// Send 5 packets without draining the channel — the first will park in
	// the ring, the remaining four should be counted as local drops.
	sconn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sconn.Close() }()
	const packetSize = 128
	const sendCount = 5
	sender, err := NewSender(sconn, SenderConfig{
		RateBPS:    int64(packetSize * 8 * 1000), // 1ms-per-packet pacing
		PacketSize: packetSize,
		Duration:   time.Duration(sendCount) * time.Millisecond,
		FlowID:     flow,
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer sendCancel()
	_ = sender.Run(sendCtx)

	// Give the hub time to receive everything that's coming.
	time.Sleep(100 * time.Millisecond)

	drops := flowHandle.PullLocalDrops()
	if drops < 1 {
		t.Errorf("expected at least one local drop with ringSize=1, got %d", drops)
	}
	// Second pull should be 0 — Pull resets the counter.
	if next := flowHandle.PullLocalDrops(); next != 0 {
		t.Errorf("PullLocalDrops should reset to 0; second pull returned %d", next)
	}
}

// TestHubReleaseStopsDelivery asserts that after Release the per-flow
// channel is closed and no further observations arrive.
func TestHubReleaseStopsDelivery(t *testing.T) {
	rconn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = rconn.Close() }()

	hub := NewHub(rconn)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go func() { _ = hub.Run(hubCtx) }()

	const flow uint32 = 0xFEEDFACE
	flowHandle := hub.Register(flow, DefaultRingSize)
	flowHandle.Release()

	// Channel must already be closed.
	select {
	case _, ok := <-flowHandle.Observations():
		if ok {
			t.Error("expected closed channel after Release, got an observation")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("channel not closed within 100ms of Release")
	}

	// Idempotency: a second Release should not panic.
	flowHandle.Release()
}

// TestHubTryRegisterCollision asserts the non-panicking sibling of
// Register returns (nil, false) when flow_id is already in use, and
// that the first handle stays functional.
func TestHubTryRegisterCollision(t *testing.T) {
	rconn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = rconn.Close() }()

	hub := NewHub(rconn)
	const flow uint32 = 0x12345678
	first, ok := hub.TryRegister(flow, DefaultRingSize)
	if !ok || first == nil {
		t.Fatalf("first TryRegister: got ok=%v handle=%v, want ok=true non-nil", ok, first)
	}
	defer first.Release()

	dup, ok := hub.TryRegister(flow, DefaultRingSize)
	if ok || dup != nil {
		t.Fatalf("dup TryRegister: got ok=%v handle=%v, want ok=false nil", ok, dup)
	}

	// Original handle still works: its channel is open and Release is a no-op.
	select {
	case _, open := <-first.Observations():
		if !open {
			t.Errorf("first handle's channel closed by the failed second TryRegister")
		}
	default:
		// expected: nothing pending, channel still open
	}
}

// TestHubDoubleRegisterPanics protects against the session-id collision
// case the hub explicitly refuses to handle.
func TestHubDoubleRegisterPanics(t *testing.T) {
	rconn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = rconn.Close() }()

	hub := NewHub(rconn)
	const flow uint32 = 0xABCD1234
	first := hub.Register(flow, DefaultRingSize)
	defer first.Release()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate Register, got nil")
		}
	}()
	_ = hub.Register(flow, DefaultRingSize)
}

// TestHubReleaseDuringActiveSend exercises the lookup→send window in
// Hub.Run against a concurrent Release. Pre-fix the hub would close the
// channel between RUnlock and the channel send, panicking "send on
// closed channel"; post-fix the RLock is held through the send and
// Release serializes naturally with sends.
//
// Run with -race for cross-coverage of the synchronization fix.
func TestHubReleaseDuringActiveSend(t *testing.T) {
	rconn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer func() { _ = rconn.Close() }()
	addr := rconn.LocalAddr().(*net.UDPAddr)

	hub := NewHub(rconn)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	hubDone := make(chan error, 1)
	go func() { hubDone <- hub.Run(hubCtx) }()
	// Pre-fix the hub goroutine would panic ("send on closed channel")
	// inside its own goroutine, crashing the test binary. Post-fix the
	// RLock-through-send serializes with Release and the hub keeps
	// running. Under -race the data race on fq.ch surfaces directly.
	defer func() {
		hubCancel()
		select {
		case err := <-hubDone:
			if err != nil {
				t.Errorf("hub.Run returned error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("hub.Run did not return within 1s of cancel")
		}
	}()

	const (
		flow       uint32 = 0xC0FFEE01
		packetSize        = 128
		pps               = 5_000 // high pps to maximize race likelihood
		sendFor           = 100 * time.Millisecond
	)
	rateBPS := int64(packetSize * 8 * pps)

	// Repeat the register/release cycle several times under load; each
	// cycle is a fresh chance for the race to fire if the fix regresses.
	for i := 0; i < 5; i++ {
		flowHandle := hub.Register(flow, DefaultRingSize)

		sconn, err := net.DialUDP("udp4", nil, addr)
		if err != nil {
			t.Fatalf("DialUDP: %v", err)
		}
		sender, err := NewSender(sconn, SenderConfig{
			RateBPS:    rateBPS,
			PacketSize: packetSize,
			Duration:   sendFor,
			FlowID:     flow,
		})
		if err != nil {
			_ = sconn.Close()
			t.Fatalf("NewSender: %v", err)
		}
		sendDone := make(chan struct{})
		go func() {
			defer close(sendDone)
			_ = sender.Run(context.Background())
		}()

		// Let traffic build up, then Release mid-stream. The hub
		// goroutine is actively pushing onto fq.ch when this fires.
		time.Sleep(15 * time.Millisecond)
		flowHandle.Release()

		// Drain the rest of the sender; we don't care about counts.
		<-sendDone
		_ = sconn.Close()
	}
}
