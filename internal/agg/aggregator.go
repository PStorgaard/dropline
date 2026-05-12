package agg

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/PStorgaard/dropline/internal/control"
	"github.com/PStorgaard/dropline/internal/probe"
	"github.com/PStorgaard/dropline/internal/stream"
)

// Aggregator consumes per-second stream snapshots, indexes them into
// per-second buckets, and pushes an immutable StateSnapshot onto the
// caller's output channel after every ingest.
//
// Sends to the output channel are non-blocking: if the consumer is slow,
// the snapshot is dropped. This matches the same rule enforced in
// stream.Receiver — business goroutines must never gate on the UI.
type Aggregator struct {
	mu      sync.Mutex
	t0      time.Time
	out     chan<- StateSnapshot
	buckets []Bucket
	// last is a copy of the most recent stream.Snapshot ingested. The
	// next ingest computes per-bucket deltas against it. nil before the
	// first IngestStream call.
	last *stream.Snapshot
	// lastView caches streamView(*last) so the non-stream ingests don't
	// recompute it on every snapshot. Mutated only when last changes.
	lastView StreamView
	// hops is the most recent probe.Snapshot's hops translated into
	// HopView; rebuilt on every IngestForwardHop. Emitted in every
	// StateSnapshot regardless of which Ingest method produced it.
	hops []HopView
	// reverseHops is the per-TTL merged view of the server's reverse trace.
	// Each ReverseHopUpdate replaces the entry at its TTL. The map is
	// flattened, sorted by TTL, and copied into every StateSnapshot.
	reverseHops map[int]ReverseHopView
	// reverseStatus is the resolved reverse-path status string, set by the
	// trace driver once Ready has been parsed.
	reverseStatus string
	// lastReverse is the most recent reverse-direction stream.Snapshot
	// ingested via IngestReverseStream. nil before the first ingest;
	// once set, the snapshotLocked builder publishes a StreamView pointer
	// into every emitted StateSnapshot.
	lastReverse *stream.Snapshot
	// lastTCPInfo is the most recent TCP_INFO sample from the
	// corroboration probe. nil before the first ingest; once set,
	// snapshotLocked publishes a TCPCorroborateView pointer in every
	// emitted StateSnapshot.
	lastTCPInfo *TCPCorroborateView
}

// New constructs an Aggregator. t0 is informational only this slice — it
// is not used for bucketing because the bucket index is taken from
// stream.Snapshot.T (which is itself measured from the receiver's t0).
// Pass the same t0 you used for the receiver so future hop ingestion
// (which will use wall-clock arrival times) can be normalized against
// it. Callers may pass a nil out channel; ingests still update internal
// state but emit nothing.
func New(t0 time.Time, out chan<- StateSnapshot) *Aggregator {
	return &Aggregator{
		t0:          t0,
		out:         out,
		reverseHops: make(map[int]ReverseHopView),
	}
}

// IngestStream records one stream.Snapshot. It updates the bucket at
// floor(s.T), recomputes the StreamView, and emits a StateSnapshot.
//
// Snapshots are assumed monotonic in T (the only producer is
// stream.Receiver, which emits on a fixed ticker). A snapshot whose T
// straddles a second boundary is fully attributed to the bucket at
// floor(s.T) rather than split — at the canonical 1-second tick this
// is exact; sub-second ticks would mildly skew bucket totals (acceptable
// for v1 and documented).
func (a *Aggregator) IngestStream(s stream.Snapshot) {
	a.mu.Lock()
	state := a.ingestStreamLocked(s)
	a.mu.Unlock()
	a.emit(state)
}

func (a *Aggregator) ingestStreamLocked(s stream.Snapshot) StateSnapshot {
	bucketIdx := int(math.Floor(s.T))
	if bucketIdx < 0 {
		bucketIdx = 0
	}

	// Compute deltas against the previous ingest. On the first ingest
	// the full counters are attributed to this bucket, which matches the
	// intuition that "everything observed so far happened in this second."
	var dRecv, dLost int64
	if a.last == nil {
		dRecv = s.Recv
		dLost = s.Lost
	} else {
		dRecv = s.Recv - a.last.Recv
		dLost = s.Lost - a.last.Lost
		if dRecv < 0 {
			// Recv is monotonic by construction in stream.Accumulator —
			// clamp defensively in case a future Reset path violates that.
			dRecv = 0
		}
	}

	cur := a.bucketAtLocked(bucketIdx)
	cur.StreamRecv += dRecv
	if dLost >= 0 {
		cur.StreamLost += dLost
	} else {
		// Lost can decrease when stream.Accumulator records an out-of-order
		// recovery (a late packet fills a previously-counted gap). Unwind
		// the correction across recent buckets so per-second StreamLost
		// stays in sync with the cumulative StreamView.Lost.
		a.unwindLossLocked(-dLost)
	}
	if total := cur.StreamRecv + cur.StreamLost; total > 0 {
		cur.StreamLossPct = float64(cur.StreamLost) / float64(total) * 100
	} else {
		cur.StreamLossPct = 0
	}

	snap := s
	a.last = &snap
	a.lastView = streamView(s)

	return a.snapshotLocked(s.T, a.lastView)
}

// IngestReverseStream records one reverse-direction stream.Snapshot.
// It updates the aggregator's reverse view (kept separate from the
// forward view used for buckets/correlation) and emits a fresh
// StateSnapshot. Reverse counters are intentionally not bucketed in
// v1 — the per-second timeline stays forward-only — so this ingest
// only refreshes the cumulative ReverseStream pointer.
//
// Sends are non-blocking — see IngestStream.
func (a *Aggregator) IngestReverseStream(s stream.Snapshot) {
	a.mu.Lock()
	snap := s
	a.lastReverse = &snap
	state := a.snapshotLocked(a.lastTLocked(), a.lastView)
	a.mu.Unlock()
	a.emit(state)
}

// IngestTCPInfo records one TCP_INFO sample from the corroboration
// probe. bytesRetrans / bytesOut are cumulative since the connection
// was opened; the aggregator publishes them as-is plus a precomputed
// retransmit fraction so renderers can branch on a single value. The
// emitted TCPCorroborateView is always marked Supported=true — callers
// that learn the local sampler can't read TCP_INFO at all should use
// MarkTCPCorroborateUnsupported instead.
//
// Sends are non-blocking — see IngestStream.
func (a *Aggregator) IngestTCPInfo(bytesRetrans, bytesOut uint64, rttUs, minRttUs uint32) {
	a.mu.Lock()
	var pct float64
	if bytesOut > 0 {
		pct = float64(bytesRetrans) / float64(bytesOut) * 100
	}
	a.lastTCPInfo = &TCPCorroborateView{
		Supported:    true,
		BytesRetrans: bytesRetrans,
		BytesOut:     bytesOut,
		RetransPct:   pct,
		RttUs:        rttUs,
		MinRttUs:     minRttUs,
	}
	state := a.snapshotLocked(a.lastTLocked(), a.lastView)
	a.mu.Unlock()
	a.emit(state)
}

// MarkTCPCorroborateUnsupported records that the local TCP_INFO
// sampler has reported it cannot read per-connection retransmit
// counters on this host (macOS/BSD fallback, pre-Win10-1703 Windows
// after WSAEINVAL on SIO_TCP_INFO). The emitted view carries zeroed
// counters with Supported=false so renderers can show "unsupported"
// distinctly from "0 retransmits observed".
//
// Sends are non-blocking — see IngestStream.
func (a *Aggregator) MarkTCPCorroborateUnsupported() {
	a.mu.Lock()
	a.lastTCPInfo = &TCPCorroborateView{Supported: false}
	state := a.snapshotLocked(a.lastTLocked(), a.lastView)
	a.mu.Unlock()
	a.emit(state)
}

// IngestForwardHop records one probe.Snapshot. It overwrites the
// aggregator's view of the forward hop table and emits a fresh
// StateSnapshot. Stream-side state is unchanged; the snapshot's
// Stream / Buckets fields reflect whatever the most recent
// IngestStream produced (zero-valued if none yet).
//
// Sends are non-blocking — see IngestStream.
func (a *Aggregator) IngestForwardHop(s probe.Snapshot) {
	a.mu.Lock()
	a.hops = make([]HopView, len(s.Hops))
	for i, h := range s.Hops {
		a.hops[i] = HopView{
			TTL:           h.TTL,
			IP:            h.Addr,
			Sent:          h.Sent,
			Recv:          h.Recv,
			LastRTTMS:     h.LastRTTMS,
			BestRTTMS:     h.BestRTTMS,
			WorstRTTMS:    h.WorstRTTMS,
			AvgRTTMS:      h.AvgRTTMS,
			StdDevRTTMS:   h.StdDevRTTMS,
			BaselineRTTMS: h.BaselineRTTMS,
			LossPct:       h.LossPct,
			Terminus:      h.Terminus,
		}
	}
	// Attribute this hop snapshot to the bucket at floor(elapsed). The
	// snapshot's per-bucket Hops slice powers the JSON timeline output and
	// the suspect-hop correlator. Last-write-wins within a single second
	// is fine at the canonical 1Hz probe cadence.
	elapsed := s.At.Sub(a.t0).Seconds()
	bucketIdx := int(math.Floor(elapsed))
	if bucketIdx < 0 {
		bucketIdx = 0
	}
	cur := a.bucketAtLocked(bucketIdx)
	cur.Hops = make([]HopView, len(a.hops))
	copy(cur.Hops, a.hops)

	state := a.snapshotLocked(elapsed, a.lastView)
	a.mu.Unlock()
	a.emit(state)
}

// IngestReverseHop records one ReverseHopUpdate and emits a fresh
// StateSnapshot. The map is upserted by TTL — each update replaces any
// previous entry for the same hop. Stream-side state is unchanged; the
// snapshot's Stream / Buckets fields reflect the most recent
// IngestStream call (zero-valued if none yet).
//
// Sends are non-blocking — see IngestStream.
func (a *Aggregator) IngestReverseHop(u control.ReverseHopUpdate) {
	a.mu.Lock()
	a.reverseHops[u.TTL] = ReverseHopView{
		TTL:           u.TTL,
		Addr:          u.Addr,
		RTTMS:         u.RTTMS,
		LossPct:       u.LossPct,
		Sent:          u.Sent,
		Recv:          u.Recv,
		BestRTTMS:     u.BestRTTMS,
		WorstRTTMS:    u.WorstRTTMS,
		AvgRTTMS:      u.AvgRTTMS,
		StdDevRTTMS:   u.StdDevRTTMS,
		BaselineRTTMS: u.BaselineRTTMS,
		Terminus:      u.Terminus,
	}
	// Prune any cached hop past the server-reported cap. The first sweep
	// emits every TTL up to MaxHops (terminus unknown yet, no replies);
	// later sweeps contract once the prober detects terminus. Without
	// this trim those early no-reply TTLs would persist in the table.
	// MaxTTL == 0 means an old server that doesn't report the cap —
	// fall back to the legacy merge-only behavior.
	if u.MaxTTL > 0 {
		for ttl := range a.reverseHops {
			if ttl > u.MaxTTL {
				delete(a.reverseHops, ttl)
			}
		}
	}
	// Sticky escalation: a NAT terminus overrides "ok" so the JSON's
	// reverse_path_status surfaces the truth that the path didn't reach
	// the client host. Disabled states are not overridden — a NAT can't
	// be observed if reverse trace was off.
	if u.Terminus == "nat" && a.reverseStatus == control.ReverseStatusOK {
		a.reverseStatus = control.ReverseStatusTerminatedAtNAT
	}
	state := a.snapshotLocked(a.lastTLocked(), a.lastView)
	a.mu.Unlock()
	a.emit(state)
}

// MarkSuspects flips HopView.Suspect=true on the matching TTLs and emits
// a fresh StateSnapshot. Used by the trace driver at end-of-test, after
// agg.Correlate, so the TUI's test-complete view can highlight suspect
// hops without re-running the correlator. Mutates both the live hop
// slice and any per-bucket hop snapshots so JSON timeline consumers also
// see the flag. TTLs not in a.hops are silently ignored. Empty ttls is
// a no-op — no scan, no snapshot emit.
func (a *Aggregator) MarkSuspects(ttls []int) {
	if len(ttls) == 0 {
		return
	}
	a.mu.Lock()
	want := make(map[int]bool, len(ttls))
	for _, ttl := range ttls {
		want[ttl] = true
	}
	for i := range a.hops {
		if want[a.hops[i].TTL] {
			a.hops[i].Suspect = true
		}
	}
	for bi := range a.buckets {
		for i := range a.buckets[bi].Hops {
			if want[a.buckets[bi].Hops[i].TTL] {
				a.buckets[bi].Hops[i].Suspect = true
			}
		}
	}
	state := a.snapshotLocked(a.lastTLocked(), a.lastView)
	a.mu.Unlock()
	a.emit(state)
}

// Reset clears the per-second history and re-arms delta math so the next
// IngestStream call attributes zero deltas to a fresh bucket. Hops and
// reverse hops are intentionally preserved — they are test-long rolling
// stats, not per-window history. Emits a StateSnapshot so the TUI sees
// the cleared timeline immediately.
func (a *Aggregator) Reset() {
	a.mu.Lock()
	a.buckets = nil
	if a.last != nil {
		// Snapshot the current cumulative counters so the next ingest
		// computes delta = 0 against this baseline rather than re-
		// attributing the full counter as a "first ingest".
		snap := *a.last
		a.last = &snap
	}
	// Suspect is a per-window correlator verdict, not a rolling stat.
	// Clear it so a re-armed session doesn't keep highlighting hops
	// flagged by the previous test's MarkSuspects.
	for i := range a.hops {
		a.hops[i].Suspect = false
	}
	state := a.snapshotLocked(a.lastTLocked(), a.lastView)
	a.mu.Unlock()
	a.emit(state)
}

// SetReversePathStatus updates the reverse-path status string and emits a
// StateSnapshot so the TUI sees the banner update immediately, even before
// the first ReverseHopUpdate arrives. Called once by the trace driver
// after Ready has been parsed. Sends are non-blocking.
func (a *Aggregator) SetReversePathStatus(s string) {
	a.mu.Lock()
	a.reverseStatus = s
	state := a.snapshotLocked(a.lastTLocked(), a.lastView)
	a.mu.Unlock()
	a.emit(state)
}

// Snapshot returns an immutable view of the aggregator's current
// state. T and Stream reflect the most recent IngestStream call (zero
// when none has happened); Buckets and LatestForwardHops are copies
// safe to retain. Used by the trace driver at end-of-test to populate
// the report regardless of whether the chan-based path was drained.
func (a *Aggregator) Snapshot() StateSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	var t float64
	if a.last != nil {
		t = a.last.T
	}
	return a.snapshotLocked(t, a.lastView)
}

// emit publishes state on the out channel without blocking. A nil out
// channel or a slow consumer both result in the snapshot being dropped.
func (a *Aggregator) emit(state StateSnapshot) {
	if a.out == nil {
		return
	}
	select {
	case a.out <- state:
	default:
	}
}

// snapshotLocked builds a StateSnapshot from current aggregator state.
// Caller must hold a.mu. t and stream are the snapshot's Stream-side
// view; the hop slice is taken from a.hops.
func (a *Aggregator) snapshotLocked(t float64, stream StreamView) StateSnapshot {
	bucketsCopy := make([]Bucket, len(a.buckets))
	for i, b := range a.buckets {
		bc := b
		if len(b.Hops) > 0 {
			bc.Hops = make([]HopView, len(b.Hops))
			copy(bc.Hops, b.Hops)
		}
		bucketsCopy[i] = bc
	}
	hopsCopy := make([]HopView, len(a.hops))
	copy(hopsCopy, a.hops)
	reverseCopy := make([]ReverseHopView, 0, len(a.reverseHops))
	for _, v := range a.reverseHops {
		reverseCopy = append(reverseCopy, v)
	}
	sort.Slice(reverseCopy, func(i, j int) bool {
		return reverseCopy[i].TTL < reverseCopy[j].TTL
	})
	var reverseStream *StreamView
	if a.lastReverse != nil {
		v := streamView(*a.lastReverse)
		reverseStream = &v
	}
	var tcpInfo *TCPCorroborateView
	if a.lastTCPInfo != nil {
		v := *a.lastTCPInfo
		tcpInfo = &v
	}
	return StateSnapshot{
		T:                 t,
		Stream:            stream,
		Buckets:           bucketsCopy,
		LatestForwardHops: hopsCopy,
		LatestReverseHops: reverseCopy,
		ReversePathStatus: a.reverseStatus,
		ReverseStream:     reverseStream,
		TCPCorroborate:    tcpInfo,
	}
}

// bucketAtLocked finds or inserts the Bucket whose T == idx, keeping
// a.buckets sorted by T ascending. The hot path is a tail-append: stream
// snapshots arrive in monotonic T order. An idx older than the head can
// arise when IngestForwardHop (probe wallclock) races ahead of
// IngestStream (receiver t0) at a second boundary — in that case we
// insert at the correct position rather than collapsing into the head,
// so later stream stats for the older second don't bleed into the
// newer bucket. Caller must hold a.mu. Returned pointer is valid until
// the next bucket-mutation call.
func (a *Aggregator) bucketAtLocked(idx int) *Bucket {
	n := len(a.buckets)
	if n == 0 || a.buckets[n-1].T < idx {
		a.buckets = append(a.buckets, Bucket{T: idx})
		return &a.buckets[len(a.buckets)-1]
	}
	if a.buckets[n-1].T == idx {
		return &a.buckets[n-1]
	}
	i := sort.Search(n, func(j int) bool { return a.buckets[j].T >= idx })
	if i < n && a.buckets[i].T == idx {
		return &a.buckets[i]
	}
	a.buckets = append(a.buckets, Bucket{})
	copy(a.buckets[i+1:], a.buckets[i:])
	a.buckets[i] = Bucket{T: idx}
	return &a.buckets[i]
}

// unwindLossLocked subtracts n from per-bucket StreamLost, walking
// buckets from highest-T (slice tail) to lowest. Each touched bucket is
// floored at zero and its StreamLossPct is recomputed. A no-op when n
// is 0 or a.buckets is empty (which happens if the rollback straddles a
// Reset). Caller must hold a.mu.
//
// Out-of-order recovery in stream.Accumulator decrements `lost` at the
// arrival time of the late packet, not at the bucket where the original
// `lost++` happened. At the canonical 1Hz aggregation cadence the
// receiver's OOO window is small, so newest-first unwind almost always
// targets the head bucket; the cumulative StreamView.Lost stays exact
// regardless of which bucket(s) absorb the correction.
func (a *Aggregator) unwindLossLocked(n int64) {
	if n <= 0 {
		return
	}
	remaining := n
	for i := len(a.buckets) - 1; i >= 0 && remaining > 0; i-- {
		b := &a.buckets[i]
		if b.StreamLost <= 0 {
			continue
		}
		if b.StreamLost >= remaining {
			b.StreamLost -= remaining
			remaining = 0
		} else {
			remaining -= b.StreamLost
			b.StreamLost = 0
		}
		if total := b.StreamRecv + b.StreamLost; total > 0 {
			b.StreamLossPct = float64(b.StreamLost) / float64(total) * 100
		} else {
			b.StreamLossPct = 0
		}
	}
}

// lastTLocked returns the elapsed time of the most recent stream snapshot,
// or 0 if none has been ingested. Used by IngestReverseHop /
// SetReversePathStatus when they need to build a StateSnapshot whose Stream
// side hasn't changed. Caller must hold a.mu.
func (a *Aggregator) lastTLocked() float64 {
	if a.last == nil {
		return 0
	}
	return a.last.T
}

func streamView(s stream.Snapshot) StreamView {
	v := StreamView{
		Recv:        s.Recv,
		Lost:        s.Lost,
		OutOfOrder:  s.OutOfOrder,
		Duplicates:  s.Duplicates,
		LocalDrops:  s.LocalDrops,
		KernelDrops: s.KernelDrops,
		JitterMS:    s.JitterMS,
		MaxSeq:      s.MaxSeq,
		Bursts:      s.Bursts,
	}
	if total := s.Recv + s.Lost; total > 0 {
		v.LossPct = float64(s.Lost) / float64(total) * 100
	}
	return v
}
