//go:build !windows

package probe

import (
	"testing"
	"time"
)

// TestPruneInflightLocked exercises the age-out logic in isolation: it
// constructs a Prober directly (no socket open) and verifies that
// entries older than the cutoff are removed from p.inflight AND their
// per-hop inflight counters are decremented, while younger entries are
// preserved.
func TestPruneInflightLocked(t *testing.T) {
	now := time.Now()
	p := &Prober{
		hops: []*hopState{
			{inflight: 1}, // ttl 1 — stale, should drop to 0
			{inflight: 2}, // ttl 2 — half stale, should drop to 1
			{inflight: 1}, // ttl 3 — fresh, should stay 1
		},
		inflight: map[uint16]inflightProbe{
			1: {ttl: 1, sentAt: now.Add(-5 * time.Second)}, // stale
			2: {ttl: 2, sentAt: now.Add(-5 * time.Second)}, // stale
			3: {ttl: 2, sentAt: now.Add(-1 * time.Second)}, // fresh
			4: {ttl: 3, sentAt: now.Add(-1 * time.Second)}, // fresh
		},
	}

	p.pruneInflightLocked(now.Add(-2 * time.Second))

	if _, ok := p.inflight[1]; ok {
		t.Errorf("stale entry seq=1 should have been pruned")
	}
	if _, ok := p.inflight[2]; ok {
		t.Errorf("stale entry seq=2 should have been pruned")
	}
	if _, ok := p.inflight[3]; !ok {
		t.Errorf("fresh entry seq=3 should still be present")
	}
	if _, ok := p.inflight[4]; !ok {
		t.Errorf("fresh entry seq=4 should still be present")
	}
	if got := p.hops[0].inflight; got != 0 {
		t.Errorf("ttl 1 inflight: got %d, want 0", got)
	}
	if got := p.hops[1].inflight; got != 1 {
		t.Errorf("ttl 2 inflight: got %d, want 1 (one stale pruned, one fresh kept)", got)
	}
	if got := p.hops[2].inflight; got != 1 {
		t.Errorf("ttl 3 inflight: got %d, want 1 (untouched)", got)
	}
}

// TestPruneInflightLocked_OutOfRangeTTL guards against panics if a
// malformed inflight entry somehow carries a ttl outside the hops slice.
// The map entry should still be removed; no hop counter is decremented.
func TestPruneInflightLocked_OutOfRangeTTL(t *testing.T) {
	now := time.Now()
	p := &Prober{
		hops: []*hopState{{inflight: 0}},
		inflight: map[uint16]inflightProbe{
			1: {ttl: 0, sentAt: now.Add(-5 * time.Second)},   // ttlIdx = -1
			2: {ttl: 99, sentAt: now.Add(-5 * time.Second)},  // ttlIdx >= len
			3: {ttl: 1, sentAt: now.Add(-10 * time.Second)},  // valid
		},
	}
	p.hops[0].inflight = 1

	p.pruneInflightLocked(now.Add(-2 * time.Second))

	if len(p.inflight) != 0 {
		t.Errorf("all stale entries should be pruned, got %d remaining", len(p.inflight))
	}
	if got := p.hops[0].inflight; got != 0 {
		t.Errorf("valid ttl 1 inflight: got %d, want 0", got)
	}
}

// TestEmit_PrunesWhenOutNil is the regression test for the documented
// "out may be nil" contract. With nil out, emit must still age out
// inflight probes — otherwise the map grows unbounded and lossPct stays
// 0 forever.
func TestEmit_PrunesWhenOutNil(t *testing.T) {
	now := time.Now()
	p := &Prober{
		cfg: Config{MTRInterval: time.Second},
		out: nil,
		hops: []*hopState{
			{sent: 1, inflight: 1},
			{sent: 1, inflight: 1},
		},
		inflight: map[uint16]inflightProbe{
			1: {ttl: 1, sentAt: now.Add(-10 * time.Second)},
			2: {ttl: 2, sentAt: now.Add(-10 * time.Second)},
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

// TestEmit_SendsSnapshotWhenOutNonNil verifies that the channel send path
// still works after the refactor — pruning runs, the snapshot is built,
// and a non-blocking send delivers it.
func TestEmit_SendsSnapshotWhenOutNonNil(t *testing.T) {
	now := time.Now()
	ch := make(chan Snapshot, 1)
	p := &Prober{
		cfg: Config{MTRInterval: time.Second},
		out: ch,
		hops: []*hopState{
			{sent: 3, recv: 2, inflight: 0, last: 1.5, best: 1.0, worst: 2.0, mean: 1.5, ewma: 1.5, ewmaInit: true},
		},
		inflight: map[uint16]inflightProbe{},
	}

	p.emit(now)

	select {
	case snap := <-ch:
		if !snap.At.Equal(now) {
			t.Errorf("Snapshot.At: got %v, want %v (sweep-start time)", snap.At, now)
		}
		if len(snap.Hops) != 1 {
			t.Fatalf("snap.Hops: got %d, want 1", len(snap.Hops))
		}
		if snap.Hops[0].TTL != 1 || snap.Hops[0].Sent != 3 || snap.Hops[0].Recv != 2 {
			t.Errorf("hop[0]: got %+v", snap.Hops[0])
		}
	default:
		t.Fatalf("expected a Snapshot on the channel")
	}
}

// TestNextProbeID_Monotonic asserts the counter advances by exactly 1 on
// each call. Distinctness across full uint16 wrap is not asserted (modulo
// collisions are tolerated), but the increment-by-1 invariant ensures
// concurrent Probers in the same process can never collide if fewer than
// 65536 are constructed.
func TestNextProbeID_Monotonic(t *testing.T) {
	prev := nextProbeID()
	for i := 0; i < 1000; i++ {
		got := nextProbeID()
		want := uint16((uint32(prev) + 1) & 0xffff) // #nosec G115
		if got != want {
			t.Fatalf("call %d: got %d, want %d (prev=%d)", i, got, want, prev)
		}
		prev = got
	}
}

// TestNextProbeID_WrapSurvives forces the counter near a 32-bit wrap and
// drives it past zero. Asserts the mod-65536 reduction keeps producing
// distinct ids in the immediate window and that wrap-through-zero is
// non-panicking. ICMP id=0 is legal so the zero value is not special-cased.
func TestNextProbeID_WrapSurvives(t *testing.T) {
	probeIDCounter.Store(0xfffffff0)
	seen := make(map[uint16]int)
	for i := 0; i < 32; i++ {
		seen[nextProbeID()]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("id %d appeared %d times within a 32-call window", id, n)
		}
	}
}

// TestLossTimeout_Scaling is a table-driven check that the per-probe
// settle window stays at the 2s floor for short sweeps and scales as
// 2*MTRInterval for long sweeps. The 2s case sits right at the floor:
// 2*2s = 4s > 2s, so the method picks 4s; the >= check on `t > floor`
// keeps the threshold sharp.
func TestLossTimeout_Scaling(t *testing.T) {
	cases := []struct {
		interval time.Duration
		want     time.Duration
	}{
		{100 * time.Millisecond, 2 * time.Second},
		{500 * time.Millisecond, 2 * time.Second},
		{time.Second, 2 * time.Second},
		{2 * time.Second, 4 * time.Second},
		{3 * time.Second, 6 * time.Second},
		{5 * time.Second, 10 * time.Second},
	}
	for _, tc := range cases {
		if got := lossTimeout(tc.interval); got != tc.want {
			t.Errorf("MTRInterval=%v: lossTimeout=%v, want %v", tc.interval, got, tc.want)
		}
	}
}
