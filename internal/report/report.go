// Package report renders the final test result in spec-defined formats:
// a plain-text summary (--report) and a single JSON document (--json).
//
// The JSON schema is authoritative and tracked in spec § Output formats.
// Hop, correlation, and per-second hop arrays are emitted as empty slots
// until internal/probe lands so consumers can rely on a stable shape.
package report

import (
	"time"

	"github.com/PStorgaard/dropline/internal/agg"
	"github.com/PStorgaard/dropline/internal/control"
)

// Data is the input to both RenderText and RenderJSON. It collects every
// field the spec's JSON output references plus the few extras the text
// summary uses (Target, StartedAt). Fields are populated by the trace
// driver after a session has produced a control.Final.
type Data struct {
	Version   int
	Target    string
	StartedAt time.Time
	DurationS float64
	Config    Config
	Stream    control.FinalStats
	Timeline  []agg.Bucket
	// Hops is the forward-trace hop table at end of test. Empty when
	// the probe didn't run or returned no answers.
	Hops []HopReport
	// ReversePath is the merged reverse-trace hop table the server
	// streamed. Empty when reverse trace was disabled or no updates
	// arrived.
	ReversePath []ReverseHopReport
	// ReversePathStatus is one of "ok", "disabled_by_server",
	// "disabled_by_client". Empty before the trace driver has resolved
	// it from Ready; the renderer treats empty as "disabled_by_server".
	ReversePathStatus string
	// Correlation is the ranked suspect-hop output from agg.Correlate,
	// computed by the trace driver after end-of-test. Empty when no hop
	// crossed the >50% co-occurrence bar.
	Correlation []SuspectHopReport
	// ReverseStream summarizes server→client UDP loss for the reverse-
	// direction sub-test. nil when reverse stream was disabled for this
	// session; renderers drop the section in that case. The "Sent" count
	// is the server's exact counter carried in Final.ReverseSent, while
	// the receive/loss/jitter counters are the client's local view.
	ReverseStream *ReverseStreamReport
	// TCPCorroborate summarizes TCP retransmit activity on the dedicated
	// corroboration probe socket. nil when tcp-corroborate was disabled
	// or when the local platform's TCP_INFO sampler is a nop. Used to
	// answer "is the observed UDP loss path-wide, or UDP-specific?".
	TCPCorroborate *TCPCorroborateReport
}

// TCPCorroborateReport mirrors agg.TCPCorroborateView in the renderer's
// type space. All four byte/microsecond counters are kernel-cumulative
// on the probe socket; RetransPct is precomputed for the renderer.
type TCPCorroborateReport struct {
	BytesRetrans uint64  `json:"bytes_retrans"`
	BytesOut     uint64  `json:"bytes_out"`
	RetransPct   float64 `json:"retrans_pct"`
	RttUs        uint32  `json:"rtt_us"`
	MinRttUs     uint32  `json:"min_rtt_us"`
}

// ReverseStreamReport is the report-layer view of the reverse-direction
// UDP sub-test. The fields mirror the relevant subset of
// control.FinalStats / agg.StreamView so renderers don't import either
// package's wire surface.
type ReverseStreamReport struct {
	// Sent is the server-side exact count of UDP packets transmitted
	// back to the client (Final.ReverseSent on the wire).
	Sent int64 `json:"sent"`
	// Recv, Lost, OutOfOrder, Duplicates are the client's local
	// observations on the reverse stream.
	Recv       int64 `json:"recv"`
	Lost       int64 `json:"loss"`
	OutOfOrder int64 `json:"out_of_order"`
	Duplicates int64 `json:"duplicates"`
	// LossPct = Lost / (Recv + Lost) * 100, or 0 when no traffic. Renderers
	// fall back to (Sent - Recv) / Sent when Recv + Lost is zero (the
	// receiver may not have logged Lost yet for very short tests).
	LossPct float64 `json:"loss_pct"`
	// JitterMS is the RFC 3550 EWMA jitter the client observed.
	JitterMS float64 `json:"jitter_ms"`
	// DurationS is the wall-clock duration the server-side sender
	// actually ran for.
	DurationS float64 `json:"duration_s"`
}

// SuspectHopReport mirrors agg.SuspectHop in the renderer's type space so
// the report layer never imports internal/agg directly. The trace driver
// is the adapter between them.
type SuspectHopReport struct {
	TTL        int
	Confidence float64
	Evidence   string
}

// HopReport is the per-hop view passed to the renderers. Mirrors the
// spec § Output formats hop shape (ttl, ip, sent, recv, loss_pct,
// rtt_ms{...}, suspect). Suspect is always false until the correlator
// lands.
type HopReport struct {
	TTL         int
	IP          string
	Sent        int64
	Recv        int64
	LossPct     float64
	LastRTTMS   float64
	BestRTTMS   float64
	WorstRTTMS  float64
	AvgRTTMS    float64
	StdDevRTTMS float64
	Suspect     bool
}

// ReverseHopReport is the per-hop view of the server's reverse trace.
// Mirrors HopReport's rolling-stat shape modulo two intentional
// differences: Addr in place of IP, no Suspect flag. RTTMS is the
// latest sample (semantically the same as LastRTTMS); kept for backward
// compat with the slim wire shape.
type ReverseHopReport struct {
	TTL  int
	Addr string
	// RTTMS == LastRTTMS; both are written from
	// control.ReverseHopUpdate.RTTMS.
	RTTMS         float64
	LossPct       float64
	Sent          int64
	Recv          int64
	BestRTTMS     float64
	WorstRTTMS    float64
	AvgRTTMS      float64
	StdDevRTTMS   float64
	BaselineRTTMS float64
	// Terminus is "host", "nat", or empty.
	Terminus string
}

// Config mirrors the JSON `config` block. mtr_interval_ms and max_hops
// are recorded even though no probe runs yet — the report shape stays
// stable across slices.
type Config struct {
	RateBPS       int64  `json:"rate_bps"`
	PacketSize    int    `json:"packet_size"`
	MTRIntervalMS int64  `json:"mtr_interval_ms"`
	MaxHops       int    `json:"max_hops"`
	ReverseTrace  string `json:"reverse_trace"`
}

// lossPct returns Stream.Loss / (Stream.Recv + Stream.Loss) * 100, or 0
// when no traffic was observed. Shared between text and JSON renderers.
func (d Data) lossPct() float64 {
	total := d.Stream.Recv + d.Stream.Loss
	if total <= 0 {
		return 0
	}
	return float64(d.Stream.Loss) / float64(total) * 100
}
