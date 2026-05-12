package agg

import "github.com/PStorgaard/dropline/internal/stream"

// StateSnapshot is the immutable view pushed to the TUI on every ingest.
// All slice fields point to copies; consumers may treat the value as
// safe to read across goroutines without further locking.
type StateSnapshot struct {
	// T is elapsed seconds since the aggregator's t0.
	T float64
	// Stream is the current view of cumulative stream counters plus
	// derived per-test loss percentage.
	Stream StreamView
	// Buckets is per-second history, oldest first. Each bucket holds the
	// stream stats *for that second only* (deltas, not cumulative). Length
	// grows by one for each new second observed in IngestStream.
	Buckets []Bucket
	// LatestForwardHops is the most recent forward-trace hop table. Empty
	// until IngestForwardHop has been called at least once. Hops are sorted
	// by TTL ascending and are pruned at the first responding terminus.
	LatestForwardHops []HopView
	// LatestReverseHops is the merged reverse-trace hop table — the latest
	// ReverseHopUpdate for each TTL the server has streamed, sorted by TTL.
	// Empty until IngestReverseHop has been called at least once, or when
	// reverse trace is disabled for the session.
	LatestReverseHops []ReverseHopView
	// ReversePathStatus is one of "ok", "disabled_by_server",
	// "disabled_by_client", or "" before the client has resolved it (which
	// happens immediately after Ready arrives). The terminated_at_nat
	// state is reserved for a follow-up slice.
	ReversePathStatus string
	// ReverseStream is the cumulative view of reverse-direction UDP
	// stream counters (server-to-client) when reverse-stream is enabled
	// for this session. Nil otherwise. Renderers branch on nil to decide
	// whether to render the reverse-stream KPI / section.
	ReverseStream *StreamView
	// TCPCorroborate is the most recent TCP retransmit / RTT sample
	// from the dedicated corroboration probe socket. Nil until the
	// first sample arrives (or for the entire test when the probe is
	// disabled). Renderers branch on nil to decide whether to render
	// the section.
	TCPCorroborate *TCPCorroborateView
}

// TCPCorroborateView mirrors tcpinfo.Stats in the renderer's type
// space so internal/tui and internal/report don't import internal/tcpinfo.
// RetransPct is precomputed for the renderer: BytesRetrans / BytesOut
// * 100, or 0 when BytesOut is zero.
type TCPCorroborateView struct {
	BytesRetrans uint64
	BytesOut     uint64
	RetransPct   float64
	RttUs        uint32
	MinRttUs     uint32
}

// ReverseHopView is the immutable per-hop view of the server's reverse
// trace. Mirrors the forward HopView's rolling-stat shape modulo two
// intentional differences: Addr in place of IP, no Suspect flag
// (correlator runs against forward-path stream loss only). Rolling
// fields are zero for old (slim-shape) servers — renderers degrade to a
// slim view in that case.
type ReverseHopView struct {
	TTL  int
	Addr string
	// RTTMS is the latest sample (== LastRTTMS in forward HopView); kept
	// for backward compat with the slim wire shape.
	RTTMS         float64
	LossPct       float64
	Sent          int64
	Recv          int64
	BestRTTMS     float64
	WorstRTTMS    float64
	AvgRTTMS      float64
	StdDevRTTMS   float64
	BaselineRTTMS float64
	// Terminus is the wire's terminus tag: "host" if the final hop was the
	// client's own IP (echo-reply), "nat" if it stopped at a NAT
	// translator, or empty if the server hasn't classified yet.
	Terminus string
}

// HopView is the immutable per-hop view emitted in StateSnapshot.
// Mirrors the spec § Probe layer rolling stats. RTT fields are
// milliseconds; LossPct is in [0,100].
type HopView struct {
	TTL         int
	IP          string
	Sent        int64
	Recv        int64
	LastRTTMS   float64
	BestRTTMS   float64
	WorstRTTMS  float64
	AvgRTTMS    float64
	StdDevRTTMS float64
	// BaselineRTTMS is the per-hop EWMA-smoothed RTT, mirrored from
	// probe.HopStat.BaselineRTTMS. Renderers use the gap between
	// LastRTTMS and BaselineRTTMS (scaled by StdDevRTTMS) to flag
	// rising-RTT hops live, without waiting for the correlator.
	BaselineRTTMS float64
	LossPct       float64
	Terminus      bool
	// Suspect is set by Aggregator.MarkSuspects at end-of-test once
	// agg.Correlate has run. Live rendering keeps it false; the TUI
	// uses it only on the test-complete view.
	Suspect bool
}

// StreamView is the cumulative view of stream counters at the time the
// snapshot was built, plus the derived overall loss percentage. Mirrors
// stream.Snapshot but with LossPct precomputed for the renderer.
type StreamView struct {
	Recv        int64
	Lost        int64
	OutOfOrder  int64
	Duplicates  int64
	LocalDrops  int64
	KernelDrops int64
	JitterMS    float64
	MaxSeq      uint64
	// Bursts is reused from internal/stream — shape and JSON tags are
	// identical to control.BurstBuckets. Importing keeps a single source
	// of truth on the value side; the wire side already mirrors.
	Bursts stream.BurstBuckets
	// LossPct = Lost / (Recv + Lost) * 100, or 0 when no traffic.
	LossPct float64
}

// Bucket is one second of stream history. Stream-side fields aggregate
// deltas; Hops is a snapshot of the forward hop table at the most
// recent IngestForwardHop call attributed to this second (last-write-wins
// at the canonical 1Hz probe cadence).
type Bucket struct {
	// T is the exact integer second index. Buckets in StateSnapshot.Buckets
	// are sorted by T ascending and each T value is unique.
	T int
	// StreamRecv is the count of packets received during this second.
	StreamRecv int64
	// StreamLost is the count of packets lost during this second.
	StreamLost int64
	// StreamLossPct = StreamLost / (StreamRecv + StreamLost) * 100,
	// or 0 when no traffic was observed in this second.
	StreamLossPct float64
	// Hops is the forward-hop table snapshot for this second. Empty when
	// no IngestForwardHop has fired in this bucket. Sorted by TTL
	// ascending; same shape as StateSnapshot.LatestForwardHops. Used by
	// the JSON timeline output and the suspect-hop correlator.
	Hops []HopView
}
