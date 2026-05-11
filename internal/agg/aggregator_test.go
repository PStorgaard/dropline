package agg

import (
	"testing"
	"time"

	"github.com/PStorgaard/dropline/internal/control"
	"github.com/PStorgaard/dropline/internal/probe"
	"github.com/PStorgaard/dropline/internal/stream"
)

// drain returns the most recent StateSnapshot on out, or fails if none arrived.
func drain(t *testing.T, out <-chan StateSnapshot) StateSnapshot {
	t.Helper()
	var last StateSnapshot
	got := false
	for {
		select {
		case s := <-out:
			last = s
			got = true
		case <-time.After(50 * time.Millisecond):
			if !got {
				t.Fatal("no StateSnapshot arrived")
			}
			return last
		}
	}
}

// Two snapshots ~within the same second roll into a single bucket whose
// recv/lost equal the latter snapshot's cumulative values.
func TestIngestStreamSingleBucket(t *testing.T) {
	out := make(chan StateSnapshot, 4)
	a := New(time.Unix(1_000_000_000, 0), out)

	a.IngestStream(stream.Snapshot{T: 0.3, Recv: 100, Lost: 5})
	a.IngestStream(stream.Snapshot{T: 0.9, Recv: 200, Lost: 7})

	s := drain(t, out)
	if len(s.Buckets) != 1 {
		t.Fatalf("buckets: want 1, got %d", len(s.Buckets))
	}
	b := s.Buckets[0]
	if b.T != 0 {
		t.Errorf("bucket T: want 0, got %d", b.T)
	}
	if b.StreamRecv != 200 {
		t.Errorf("bucket recv: want 200 (cumulative across same second), got %d", b.StreamRecv)
	}
	if b.StreamLost != 7 {
		t.Errorf("bucket lost: want 7, got %d", b.StreamLost)
	}
	if s.Stream.Recv != 200 || s.Stream.Lost != 7 {
		t.Errorf("stream view: want 200/7, got %d/%d", s.Stream.Recv, s.Stream.Lost)
	}
}

// Snapshots crossing second boundaries produce separate buckets keyed by
// floor(T). Each bucket holds the *delta* against the previous snapshot.
func TestIngestStreamMultipleBuckets(t *testing.T) {
	out := make(chan StateSnapshot, 8)
	a := New(time.Unix(1_000_000_000, 0), out)

	a.IngestStream(stream.Snapshot{T: 0.3, Recv: 100, Lost: 0})
	a.IngestStream(stream.Snapshot{T: 1.4, Recv: 250, Lost: 5})
	a.IngestStream(stream.Snapshot{T: 2.7, Recv: 400, Lost: 8})

	s := drain(t, out)
	if len(s.Buckets) != 3 {
		t.Fatalf("buckets: want 3, got %d", len(s.Buckets))
	}
	want := []Bucket{
		{T: 0, StreamRecv: 100, StreamLost: 0, StreamLossPct: 0},
		{T: 1, StreamRecv: 150, StreamLost: 5},
		{T: 2, StreamRecv: 150, StreamLost: 3},
	}
	for i, w := range want {
		got := s.Buckets[i]
		if got.T != w.T || got.StreamRecv != w.StreamRecv || got.StreamLost != w.StreamLost {
			t.Errorf("bucket[%d]: want %+v, got %+v", i, w, got)
		}
	}
	// Final stream view holds cumulative totals.
	if s.Stream.Recv != 400 || s.Stream.Lost != 8 {
		t.Errorf("stream view: want 400/8, got %d/%d", s.Stream.Recv, s.Stream.Lost)
	}
	wantLossPct := 8.0 / 408.0 * 100
	if abs := s.Stream.LossPct - wantLossPct; abs < -1e-6 || abs > 1e-6 {
		t.Errorf("stream loss_pct: want %f, got %f", wantLossPct, s.Stream.LossPct)
	}
}

// A slow / blocked consumer must not stall IngestStream. We drive
// thousands of ingests synchronously into a 1-cap channel and assert
// total time stays well under any plausible block.
func TestIngestStreamNonBlockingOnSlowConsumer(t *testing.T) {
	out := make(chan StateSnapshot, 1)
	a := New(time.Unix(1_000_000_000, 0), out)

	const N = 5000
	start := time.Now()
	for i := 0; i < N; i++ {
		a.IngestStream(stream.Snapshot{T: float64(i) * 0.001, Recv: int64(i)})
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("IngestStream blocked: %d ingests took %v", N, elapsed)
	}
}

// IngestStream with a nil out channel must update internal state and
// return without panic.
func TestIngestStreamNilOut(t *testing.T) {
	a := New(time.Unix(0, 0), nil)
	a.IngestStream(stream.Snapshot{T: 0.5, Recv: 10, Lost: 1})
	a.IngestStream(stream.Snapshot{T: 1.5, Recv: 20, Lost: 2})
	// No assertion: the test passes if the calls did not panic and
	// returned promptly.
}

// IngestForwardHop translates probe.HopStat → HopView verbatim and
// emits the latest hop table on every snapshot.
func TestIngestForwardHopBasic(t *testing.T) {
	out := make(chan StateSnapshot, 1)
	a := New(time.Unix(1_000_000_000, 0), out)

	a.IngestForwardHop(probe.Snapshot{
		At: time.Unix(1_000_000_000, 0).Add(2 * time.Second),
		Hops: []probe.HopStat{
			{TTL: 1, Addr: "10.0.0.1", Sent: 3, Recv: 3, LastRTTMS: 0.4, BestRTTMS: 0.3, WorstRTTMS: 0.6, AvgRTTMS: 0.4, StdDevRTTMS: 0.05, LossPct: 0},
			{TTL: 2, Addr: "10.0.0.2", Sent: 3, Recv: 2, LastRTTMS: 1.2, BestRTTMS: 1.0, WorstRTTMS: 1.5, AvgRTTMS: 1.2, StdDevRTTMS: 0.1, LossPct: 33.333333, Terminus: true},
		},
	})

	s := drain(t, out)
	if len(s.LatestForwardHops) != 2 {
		t.Fatalf("hops: want 2, got %d", len(s.LatestForwardHops))
	}
	h := s.LatestForwardHops[1]
	if h.TTL != 2 || h.IP != "10.0.0.2" {
		t.Errorf("hop[1] addr fields: %+v", h)
	}
	if h.Sent != 3 || h.Recv != 2 || !h.Terminus {
		t.Errorf("hop[1] counters: %+v", h)
	}
	if h.LossPct < 33.0 || h.LossPct > 33.5 {
		t.Errorf("hop[1] LossPct: got %v", h.LossPct)
	}
	if s.T < 1.99 || s.T > 2.01 {
		t.Errorf("snapshot T should reflect probe.At - t0 (~2s), got %v", s.T)
	}
}

// IngestForwardHop replaces the hop slice on every call rather than
// appending — the latest snapshot is the source of truth.
func TestIngestForwardHopOverwrites(t *testing.T) {
	out := make(chan StateSnapshot, 4)
	a := New(time.Unix(0, 0), out)

	a.IngestForwardHop(probe.Snapshot{Hops: []probe.HopStat{{TTL: 1, Addr: "10.0.0.1"}, {TTL: 2, Addr: "10.0.0.2"}, {TTL: 3, Addr: "10.0.0.3"}}})
	a.IngestForwardHop(probe.Snapshot{Hops: []probe.HopStat{{TTL: 1, Addr: "10.0.0.9"}}})

	s := drain(t, out)
	if len(s.LatestForwardHops) != 1 {
		t.Fatalf("hops: want 1 after overwrite, got %d", len(s.LatestForwardHops))
	}
	if s.LatestForwardHops[0].IP != "10.0.0.9" {
		t.Errorf("expected most recent address, got %q", s.LatestForwardHops[0].IP)
	}
}

// Stream snapshots emitted after a hop ingest carry the latest hop
// table — the two streams of input share one StateSnapshot shape.
func TestIngestStreamCarriesLatestHops(t *testing.T) {
	out := make(chan StateSnapshot, 4)
	a := New(time.Unix(0, 0), out)

	a.IngestForwardHop(probe.Snapshot{Hops: []probe.HopStat{{TTL: 1, Addr: "10.0.0.1", Sent: 1, Recv: 1}}})
	a.IngestStream(stream.Snapshot{T: 0.5, Recv: 100, Lost: 0})

	s := drain(t, out)
	if len(s.LatestForwardHops) != 1 || s.LatestForwardHops[0].IP != "10.0.0.1" {
		t.Fatalf("expected stream snapshot to carry latest hop, got %+v", s.LatestForwardHops)
	}
	if s.Stream.Recv != 100 {
		t.Errorf("stream recv lost: %+v", s.Stream)
	}
}

// IngestForwardHop with a nil out channel must not panic.
func TestIngestForwardHopNilOut(t *testing.T) {
	a := New(time.Unix(0, 0), nil)
	a.IngestForwardHop(probe.Snapshot{Hops: []probe.HopStat{{TTL: 1, Addr: "10.0.0.1"}}})
}

// Bursts and other passthrough fields land verbatim in StreamView.
func TestStreamViewPassthrough(t *testing.T) {
	out := make(chan StateSnapshot, 1)
	a := New(time.Unix(0, 0), out)
	in := stream.Snapshot{
		T: 0.5, Recv: 99, Lost: 1, OutOfOrder: 2, Duplicates: 3,
		LocalDrops: 4, KernelDrops: 5, JitterMS: 0.42, MaxSeq: 100,
		Bursts: stream.BurstBuckets{One: 7, Two: 1, Max: 2},
	}
	a.IngestStream(in)
	s := <-out
	if s.Stream.OutOfOrder != 2 || s.Stream.Duplicates != 3 ||
		s.Stream.LocalDrops != 4 || s.Stream.KernelDrops != 5 ||
		s.Stream.JitterMS != 0.42 || s.Stream.MaxSeq != 100 {
		t.Errorf("passthrough mismatch: %+v", s.Stream)
	}
	if s.Stream.Bursts.One != 7 || s.Stream.Bursts.Two != 1 || s.Stream.Bursts.Max != 2 {
		t.Errorf("bursts mismatch: %+v", s.Stream.Bursts)
	}
}

func TestIngestReverseHopMergesByTTL(t *testing.T) {
	out := make(chan StateSnapshot, 8)
	a := New(time.Unix(1_000_000_000, 0), out)

	// Two TTLs, then an update that replaces TTL 1.
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 2, Addr: "10.0.0.2", RTTMS: 2.0})
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 1, Addr: "10.0.0.1", RTTMS: 0.5})
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 1, Addr: "10.0.0.1", RTTMS: 0.7, LossPct: 50})

	s := drain(t, out)
	if got := len(s.LatestReverseHops); got != 2 {
		t.Fatalf("want 2 reverse hops, got %d (%+v)", got, s.LatestReverseHops)
	}
	if s.LatestReverseHops[0].TTL != 1 || s.LatestReverseHops[1].TTL != 2 {
		t.Errorf("expected TTL-sorted, got TTLs %d,%d", s.LatestReverseHops[0].TTL, s.LatestReverseHops[1].TTL)
	}
	// TTL 1 should reflect the latest update (RTT 0.7, loss 50), not the first (RTT 0.5).
	if h := s.LatestReverseHops[0]; h.RTTMS != 0.7 || h.LossPct != 50 {
		t.Errorf("TTL 1 not replaced by latest update: %+v", h)
	}
}

func TestIngestForwardHopRecordsBucketHopsByElapsed(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	out := make(chan StateSnapshot, 8)
	a := New(t0, out)

	// First, push stream stats for second 0 and 1 to seed the timeline.
	a.IngestStream(stream.Snapshot{T: 0.5, Recv: 10, Lost: 0})
	a.IngestStream(stream.Snapshot{T: 1.5, Recv: 20, Lost: 1})

	// Now hop snapshot at second 1: should attribute to the bucket at idx 1,
	// not idx 0.
	a.IngestForwardHop(probe.Snapshot{
		At: t0.Add(1500 * time.Millisecond),
		Hops: []probe.HopStat{
			{TTL: 1, Addr: "10.0.0.1", Sent: 2, Recv: 2, LastRTTMS: 1.0},
			{TTL: 2, Addr: "10.0.0.2", Sent: 2, Recv: 2, LastRTTMS: 5.0},
		},
	})
	s := drain(t, out)
	if len(s.Buckets) != 2 {
		t.Fatalf("buckets: want 2, got %d", len(s.Buckets))
	}
	if hops := s.Buckets[0].Hops; len(hops) != 0 {
		t.Errorf("bucket[0] should have no hops, got %d", len(hops))
	}
	if hops := s.Buckets[1].Hops; len(hops) != 2 {
		t.Fatalf("bucket[1] hops: want 2, got %d", len(hops))
	}
	if s.Buckets[1].Hops[0].TTL != 1 || s.Buckets[1].Hops[1].TTL != 2 {
		t.Errorf("bucket[1] hops not in TTL order: %+v", s.Buckets[1].Hops)
	}
}

func TestIngestForwardHopBeforeStreamCreatesBucket(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	out := make(chan StateSnapshot, 4)
	a := New(t0, out)

	// Hop arrives before any stream snapshot — the aggregator must still
	// attribute it to the bucket at floor(elapsed) rather than dropping it.
	a.IngestForwardHop(probe.Snapshot{
		At:   t0.Add(2 * time.Second),
		Hops: []probe.HopStat{{TTL: 1, Addr: "10.0.0.1"}},
	})
	s := drain(t, out)
	if len(s.Buckets) != 1 {
		t.Fatalf("buckets: want 1, got %d", len(s.Buckets))
	}
	if s.Buckets[0].T != 2 {
		t.Errorf("bucket T: want 2, got %d", s.Buckets[0].T)
	}
	if len(s.Buckets[0].Hops) != 1 {
		t.Errorf("bucket hops: want 1, got %d", len(s.Buckets[0].Hops))
	}
}

// A rich-shape ReverseHopUpdate's rolling stats are surfaced verbatim
// in the merged ReverseHopView. Old (slim-shape) servers leave these
// fields zero — renderers degrade naturally.
func TestIngestReverseHopRichShape(t *testing.T) {
	out := make(chan StateSnapshot, 4)
	a := New(time.Unix(0, 0), out)

	a.IngestReverseHop(control.ReverseHopUpdate{
		TTL: 3, Addr: "10.0.0.3",
		RTTMS: 5.0, LossPct: 0,
		Sent: 4, Recv: 4, BestRTTMS: 4.8, WorstRTTMS: 5.4,
		AvgRTTMS: 5.0, StdDevRTTMS: 0.2, BaselineRTTMS: 4.95,
	})
	s := drain(t, out)
	if got := len(s.LatestReverseHops); got != 1 {
		t.Fatalf("hops: want 1, got %d", got)
	}
	h := s.LatestReverseHops[0]
	if h.Sent != 4 || h.Recv != 4 {
		t.Errorf("Sent/Recv: %+v", h)
	}
	if h.BestRTTMS != 4.8 || h.WorstRTTMS != 5.4 || h.AvgRTTMS != 5.0 {
		t.Errorf("Best/Worst/Avg: %+v", h)
	}
	if h.StdDevRTTMS != 0.2 || h.BaselineRTTMS != 4.95 {
		t.Errorf("StdDev/Baseline: %+v", h)
	}
}

// When the server reports a MaxTTL on a reverse hop update, the aggregator
// drops any cached hop past that cap. Models the real flow: the first sweep
// emits every TTL up to MaxHops with sent=1 (terminus unknown), then later
// sweeps contract to terminus and the no-reply tail must disappear.
func TestIngestReverseHopPrunesPastMaxTTL(t *testing.T) {
	out := make(chan StateSnapshot, 16)
	a := New(time.Unix(1_000_000_000, 0), out)

	// First sweep: 5 hops with no MaxTTL signal (legacy server semantics —
	// keep them).
	for ttl := 1; ttl <= 5; ttl++ {
		a.IngestReverseHop(control.ReverseHopUpdate{TTL: ttl, Sent: 1, LossPct: 100})
	}
	s := drain(t, out)
	if len(s.LatestReverseHops) != 5 {
		t.Fatalf("pre-trim: want 5 hops, got %d", len(s.LatestReverseHops))
	}

	// Second sweep: terminus detected at TTL 2. Each update stamps MaxTTL=2.
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 1, Addr: "10.0.0.1", Sent: 2, MaxTTL: 2})
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 2, Addr: "10.0.0.2", Sent: 2, MaxTTL: 2, Terminus: "host"})
	s = drain(t, out)
	if got := len(s.LatestReverseHops); got != 2 {
		t.Fatalf("post-trim: want 2 hops, got %d (%+v)", got, s.LatestReverseHops)
	}
	if s.LatestReverseHops[0].TTL != 1 || s.LatestReverseHops[1].TTL != 2 {
		t.Errorf("post-trim TTLs: %+v", s.LatestReverseHops)
	}
}

// MaxTTL == 0 (old server that doesn't report the cap) must NOT prune.
// Verifies the backward-compat fallback documented in the wire schema.
func TestIngestReverseHopLegacyServerDoesNotPrune(t *testing.T) {
	out := make(chan StateSnapshot, 8)
	a := New(time.Unix(1_000_000_000, 0), out)

	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 1, Addr: "10.0.0.1"})
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 2, Addr: "10.0.0.2"})
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 3, Addr: "10.0.0.3"})
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 1, Addr: "10.0.0.1", RTTMS: 1.0})

	s := drain(t, out)
	if got := len(s.LatestReverseHops); got != 3 {
		t.Fatalf("want 3 hops (legacy merge-only), got %d", got)
	}
}

// A reverse hop tagged Terminus="nat" sticky-escalates the status from
// "ok" to "terminated_at_nat" so JSON consumers can spot truncated reverse
// paths without inspecting individual hops.
func TestIngestReverseHopNATEscalatesFromOK(t *testing.T) {
	out := make(chan StateSnapshot, 8)
	a := New(time.Unix(1_000_000_000, 0), out)

	a.SetReversePathStatus("ok")
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 1, Addr: "10.0.0.1", Terminus: ""})
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 2, Addr: "203.0.113.4", Terminus: "nat"})
	s := drain(t, out)
	if s.ReversePathStatus != "terminated_at_nat" {
		t.Errorf("status = %q, want terminated_at_nat", s.ReversePathStatus)
	}
}

// A "host" terminus does not change the status — only "nat" escalates.
func TestIngestReverseHopHostKeepsOK(t *testing.T) {
	out := make(chan StateSnapshot, 4)
	a := New(time.Unix(0, 0), out)

	a.SetReversePathStatus("ok")
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 1, Addr: "127.0.0.1", Terminus: "host"})
	s := drain(t, out)
	if s.ReversePathStatus != "ok" {
		t.Errorf("status = %q, want ok", s.ReversePathStatus)
	}
}

// Disabled states must not be overridden by a NAT terminus — that
// combination should not occur (a disabled reverse trace produces no hops),
// but defend against it so a flaky/replayed message can't corrupt status.
func TestIngestReverseHopNATDoesNotOverrideDisabled(t *testing.T) {
	out := make(chan StateSnapshot, 4)
	a := New(time.Unix(0, 0), out)

	a.SetReversePathStatus("disabled_by_client")
	a.IngestReverseHop(control.ReverseHopUpdate{TTL: 1, Terminus: "nat"})
	s := drain(t, out)
	if s.ReversePathStatus != "disabled_by_client" {
		t.Errorf("status = %q, want disabled_by_client (sticky)", s.ReversePathStatus)
	}
}

// MarkSuspects flips Suspect=true on the matching TTLs in both the
// live hop slice and any per-bucket hop snapshots, and emits a fresh
// StateSnapshot so the TUI sees the marks before the channel closes.
func TestMarkSuspectsFlipsHopAndBucketHops(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	out := make(chan StateSnapshot, 8)
	a := New(t0, out)

	// Seed two hops AND a bucket-resident hop snapshot.
	a.IngestForwardHop(probe.Snapshot{
		At: t0.Add(2 * time.Second),
		Hops: []probe.HopStat{
			{TTL: 1, Addr: "10.0.0.1", Sent: 3, Recv: 3},
			{TTL: 2, Addr: "10.0.0.2", Sent: 3, Recv: 2, LossPct: 33.3},
		},
	})

	a.MarkSuspects([]int{2})
	s := drain(t, out)

	if len(s.LatestForwardHops) != 2 {
		t.Fatalf("hops: want 2, got %d", len(s.LatestForwardHops))
	}
	if s.LatestForwardHops[0].Suspect {
		t.Errorf("TTL 1 should not be Suspect")
	}
	if !s.LatestForwardHops[1].Suspect {
		t.Errorf("TTL 2 should be Suspect")
	}
	// Per-bucket hop snapshot must mirror the flag — JSON timeline
	// consumers read from there.
	if len(s.Buckets) == 0 || len(s.Buckets[0].Hops) != 2 {
		t.Fatalf("bucket hops missing: %+v", s.Buckets)
	}
	if !s.Buckets[0].Hops[1].Suspect {
		t.Errorf("bucket hop TTL 2 should be Suspect")
	}
}

// MarkSuspects with an unknown TTL is a no-op (and doesn't panic).
func TestMarkSuspectsIgnoresUnknownTTL(t *testing.T) {
	out := make(chan StateSnapshot, 4)
	a := New(time.Unix(0, 0), out)
	a.IngestForwardHop(probe.Snapshot{Hops: []probe.HopStat{{TTL: 1, Addr: "10.0.0.1"}}})
	a.MarkSuspects([]int{99})
	s := drain(t, out)
	if s.LatestForwardHops[0].Suspect {
		t.Errorf("TTL 1 should not have been flipped by an unknown-TTL mark")
	}
}

// Reset clears bucket history and re-arms delta math so the next
// IngestStream attributes zero deltas to the post-reset bucket — the
// previously-cumulative counters are not double-counted.
func TestResetClearsBucketsAndArmsDeltas(t *testing.T) {
	out := make(chan StateSnapshot, 16)
	a := New(time.Unix(1_000_000_000, 0), out)

	a.IngestStream(stream.Snapshot{T: 0.5, Recv: 100, Lost: 0})
	a.IngestStream(stream.Snapshot{T: 1.5, Recv: 250, Lost: 5})

	// Drain so the next snapshot we assert on is the post-reset one.
	drain(t, out)

	a.Reset()

	// Post-reset, ingesting *the same cumulative counters* should yield
	// a single bucket with zero deltas. If Reset wiped `last` to nil,
	// the next ingest would re-attribute the full cumulative counters.
	a.IngestStream(stream.Snapshot{T: 2.5, Recv: 250, Lost: 5})
	s := drain(t, out)
	if len(s.Buckets) != 1 {
		t.Fatalf("buckets after reset: want 1, got %d (%+v)", len(s.Buckets), s.Buckets)
	}
	if b := s.Buckets[0]; b.StreamRecv != 0 || b.StreamLost != 0 {
		t.Fatalf("post-reset bucket should hold deltas of 0, got recv=%d lost=%d", b.StreamRecv, b.StreamLost)
	}

	// A genuine increment after reset shows up as a delta.
	a.IngestStream(stream.Snapshot{T: 3.5, Recv: 260, Lost: 7})
	s = drain(t, out)
	if last := s.Buckets[len(s.Buckets)-1]; last.StreamRecv != 10 || last.StreamLost != 2 {
		t.Errorf("delta after post-reset increment: want recv=10 lost=2, got recv=%d lost=%d", last.StreamRecv, last.StreamLost)
	}
}

// Reset must clear any Suspect flags set by a previous test's
// MarkSuspects so the TUI doesn't keep highlighting hops with no
// current correlator evidence after [r] re-arms a session. Rolling
// per-hop stats (LossPct, RTTs) are intentionally preserved by Reset.
func TestResetClearsSuspectFlags(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	out := make(chan StateSnapshot, 16)
	a := New(t0, out)

	a.IngestForwardHop(probe.Snapshot{
		At: t0.Add(2 * time.Second),
		Hops: []probe.HopStat{
			{TTL: 1, Addr: "10.0.0.1", Sent: 3, Recv: 3},
			{TTL: 2, Addr: "10.0.0.2", Sent: 3, Recv: 2, LossPct: 33.3},
		},
	})
	a.MarkSuspects([]int{2})
	pre := drain(t, out)
	if !pre.LatestForwardHops[1].Suspect {
		t.Fatalf("setup: expected TTL 2 Suspect=true, got %+v", pre.LatestForwardHops[1])
	}

	a.Reset()
	post := drain(t, out)
	for _, h := range post.LatestForwardHops {
		if h.Suspect {
			t.Errorf("hop TTL %d still Suspect after Reset", h.TTL)
		}
	}
	// Rolling stats must survive Reset — confirm one of them did.
	if got := post.LatestForwardHops[1].LossPct; got != 33.3 {
		t.Errorf("LossPct should survive Reset: got %v, want 33.3", got)
	}
}

func TestSetReversePathStatusEmitsImmediately(t *testing.T) {
	out := make(chan StateSnapshot, 4)
	a := New(time.Unix(1_000_000_000, 0), out)

	a.SetReversePathStatus("disabled_by_server")
	s := drain(t, out)
	if s.ReversePathStatus != "disabled_by_server" {
		t.Errorf("status = %q, want disabled_by_server", s.ReversePathStatus)
	}
	if len(s.LatestReverseHops) != 0 {
		t.Errorf("expected no reverse hops, got %+v", s.LatestReverseHops)
	}
}
