//go:build !windows

package probe

import (
	"net"
	"testing"
	"time"
)

// TestComputeTCPRangeSize_ScalesWithLossWindow guards finding 1: at the
// canonical 1s interval the size is the legacy floor of 4*MaxHops, but
// at the aggressive 100ms interval (where lossTimeout floors at 2s) the
// range must be wide enough to cover the full loss window plus
// headroom, so port reuse cannot overwrite a still-inflight entry.
func TestComputeTCPRangeSize_ScalesWithLossWindow(t *testing.T) {
	cases := []struct {
		name        string
		interval    time.Duration
		maxHops     int
		wantAtLeast uint32
	}{
		{"1s_30hops_floor", time.Second, 30, 4 * 30},
		{"100ms_30hops_covers_2s_window", 100 * time.Millisecond, 30, 20 * 30},
		{"100ms_255hops_covers_2s_window", 100 * time.Millisecond, 255, 20 * 255},
		{"5s_30hops_proportional", 5 * time.Second, 30, 4 * 30}, // floor still applies (lt/interval = 2)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeTCPRangeSize(tc.interval, tc.maxHops)
			if got < tc.wantAtLeast {
				t.Errorf("rangeSize: got %d, want >= %d (interval=%v, maxHops=%d)",
					got, tc.wantAtLeast, tc.interval, tc.maxHops)
			}
		})
	}
}

// TestNextSrcPort_SkipsInflight exercises the inflight-aware allocator:
// a slot whose port is currently in p.inflight must be skipped so the
// old entry is preserved (rather than silently overwritten, which would
// leak the old hop's inflight counter — finding 1).
func TestNextSrcPort_SkipsInflight(t *testing.T) {
	p := &TCPProber{
		srcPortBase:  33434,
		srcPortRange: 10,
		portCursor:   0,
		inflight: map[uint16]tcpInflight{
			33434: {ttl: 1, sentAt: time.Now()}, // slot 0 busy
			33435: {ttl: 2, sentAt: time.Now()}, // slot 1 busy
		},
	}
	port, ok := p.nextSrcPort()
	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if port != 33436 {
		t.Fatalf("port: got %d, want 33436 (first free slot)", port)
	}
}

// TestNextSrcPort_ExhaustionReturnsFalse: if every slot in the range is
// inflight, the allocator must report exhaustion rather than overwrite.
func TestNextSrcPort_ExhaustionReturnsFalse(t *testing.T) {
	inflight := map[uint16]tcpInflight{}
	for i := uint16(0); i < 5; i++ {
		inflight[33434+i] = tcpInflight{ttl: 1, sentAt: time.Now()}
	}
	p := &TCPProber{
		srcPortBase:  33434,
		srcPortRange: 5,
		inflight:     inflight,
	}
	if _, ok := p.nextSrcPort(); ok {
		t.Fatalf("expected ok=false when every slot is inflight")
	}
}

// TestNextSrcPort_AdvancesAndWraps confirms the cursor walks
// monotonically over the range and wraps. (Sanity check on the modulo
// arithmetic after the type widening to uint32.)
func TestNextSrcPort_AdvancesAndWraps(t *testing.T) {
	p := &TCPProber{
		srcPortBase:  33434,
		srcPortRange: 3,
		inflight:     map[uint16]tcpInflight{},
	}
	seen := []uint16{}
	for i := 0; i < 6; i++ {
		port, ok := p.nextSrcPort()
		if !ok {
			t.Fatalf("call %d: ok=false", i)
		}
		seen = append(seen, port)
	}
	want := []uint16{33434, 33435, 33436, 33434, 33435, 33436}
	for i, w := range want {
		if seen[i] != w {
			t.Errorf("call %d: got %d, want %d", i, seen[i], w)
		}
	}
}

// TestPruneTCPInflightLocked mirrors TestPruneInflightLocked in
// probe_test.go: entries older than the cutoff must be removed from
// p.inflight AND their per-hop inflight counters decremented.
func TestPruneTCPInflightLocked(t *testing.T) {
	now := time.Now()
	p := &TCPProber{
		hops: []*hopState{
			{inflight: 1}, // ttl 1 — stale, should drop to 0
			{inflight: 2}, // ttl 2 — half stale, should drop to 1
			{inflight: 1}, // ttl 3 — fresh, should stay 1
		},
		inflight: map[uint16]tcpInflight{
			33434: {ttl: 1, sentAt: now.Add(-5 * time.Second)},
			33435: {ttl: 2, sentAt: now.Add(-5 * time.Second)},
			33436: {ttl: 2, sentAt: now.Add(-1 * time.Second)},
			33437: {ttl: 3, sentAt: now.Add(-1 * time.Second)},
		},
	}
	p.pruneTCPInflightLocked(now.Add(-2 * time.Second))
	if _, ok := p.inflight[33434]; ok {
		t.Errorf("stale entry 33434 should have been pruned")
	}
	if _, ok := p.inflight[33435]; ok {
		t.Errorf("stale entry 33435 should have been pruned")
	}
	if _, ok := p.inflight[33436]; !ok {
		t.Errorf("fresh entry 33436 should remain")
	}
	if _, ok := p.inflight[33437]; !ok {
		t.Errorf("fresh entry 33437 should remain")
	}
	if got := p.hops[0].inflight; got != 0 {
		t.Errorf("ttl 1 inflight: got %d, want 0", got)
	}
	if got := p.hops[1].inflight; got != 1 {
		t.Errorf("ttl 2 inflight: got %d, want 1", got)
	}
	if got := p.hops[2].inflight; got != 1 {
		t.Errorf("ttl 3 inflight: got %d, want 1", got)
	}
}

// TestPruneTCPInflightLocked_OutOfRangeTTL: a malformed entry with a
// TTL outside the hops slice must still be deleted from the map; no hop
// counter is touched.
func TestPruneTCPInflightLocked_OutOfRangeTTL(t *testing.T) {
	now := time.Now()
	p := &TCPProber{
		hops: []*hopState{{inflight: 1}},
		inflight: map[uint16]tcpInflight{
			33434: {ttl: 0, sentAt: now.Add(-5 * time.Second)},  // ttlIdx = -1
			33435: {ttl: 99, sentAt: now.Add(-5 * time.Second)}, // ttlIdx >= len
			33436: {ttl: 1, sentAt: now.Add(-10 * time.Second)}, // valid
		},
	}
	p.pruneTCPInflightLocked(now.Add(-2 * time.Second))
	if len(p.inflight) != 0 {
		t.Errorf("all stale entries should be pruned, got %d remaining", len(p.inflight))
	}
	if got := p.hops[0].inflight; got != 0 {
		t.Errorf("valid ttl 1 inflight: got %d, want 0", got)
	}
}

// TestTCPProberEmit_PrunesWhenOutNil: with nil out the emit path must
// still age out inflight probes, otherwise the map grows unbounded for
// a no-consumer prober.
func TestTCPProberEmit_PrunesWhenOutNil(t *testing.T) {
	now := time.Now()
	p := &TCPProber{
		cfg: TCPConfig{
			Target:      net.ParseIP("203.0.113.7"),
			Port:        443,
			MTRInterval: time.Second,
			MaxHops:     2,
		},
		out: nil,
		hops: []*hopState{
			{sent: 1, inflight: 1},
			{sent: 1, inflight: 1},
		},
		inflight: map[uint16]tcpInflight{
			33434: {ttl: 1, sentAt: now.Add(-10 * time.Second)},
			33435: {ttl: 2, sentAt: now.Add(-10 * time.Second)},
		},
	}
	p.emit(now)
	if len(p.inflight) != 0 {
		t.Errorf("nil-out emit should still prune inflight, got %d entries remaining", len(p.inflight))
	}
	for i, h := range p.hops {
		if h.inflight != 0 {
			t.Errorf("ttl %d inflight after prune: got %d, want 0", i+1, h.inflight)
		}
	}
}
