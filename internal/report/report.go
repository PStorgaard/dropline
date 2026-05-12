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
	// TCPHopProbe summarizes TCP-mode hop probing for the session. nil
	// when the feature was off. When non-nil with Supported=false (e.g.
	// Windows) the renderer shows an "unsupported" notice rather than
	// an empty hop table. The forward / reverse hop tables themselves
	// are carried on this report struct so renderers can dim a TTL the
	// ICMP tables also rendered.
	TCPHopProbe *TCPHopProbeReport
}

// TCPHopProbeReport mirrors the TCP-mode hop probing surface in the
// renderer's type space. Port is the destination port the prober
// targeted. Supported is false on Windows (the stub emits
// Supported=false from its first snapshot). ForwardHops + ReverseHops
// are the latest per-TTL hop tables, parallel to Data.Hops /
// Data.ReversePath for the ICMP side.
type TCPHopProbeReport struct {
	Supported   bool               `json:"supported"`
	Port        uint16             `json:"port"`
	ForwardHops []HopReport        `json:"forward_hops"`
	ReverseHops []ReverseHopReport `json:"reverse_hops"`
}

// TCPCorroborateReport mirrors agg.TCPCorroborateView in the renderer's
// type space. All four byte/microsecond counters are kernel-cumulative
// on the probe socket; RetransPct is precomputed for the renderer.
//
// Supported is false when the local TCP_INFO syscall isn't available on
// this host (macOS/BSD fallback, pre-Win10-1703 Windows). Renderers
// branch on Supported so the operator-visible output says "unsupported"
// rather than implying the kernel observed zero retransmits. The
// byte/RTT fields are all zero when Supported is false.
type TCPCorroborateReport struct {
	Supported    bool    `json:"supported"`
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
	// Recv is the client's local count of reverse-stream packets received.
	// OutOfOrder and Duplicates are the client's local observations.
	Recv       int64 `json:"recv"`
	// Lost is the effective loss count: max(receiverGaps, Sent-Recv, 0).
	// The receiver only detects loss via a forward seq jump, so tail loss
	// (the last packets never arriving) leaves the receiver-gap count at
	// zero; falling back to Sent-Recv recovers the true count.
	Lost       int64 `json:"loss"`
	OutOfOrder int64 `json:"out_of_order"`
	Duplicates int64 `json:"duplicates"`
	// LossPct is Lost / Sent * 100 when Sent > 0 (server-authoritative
	// denominator), else Lost / (Recv + Lost) * 100, else 0.
	LossPct float64 `json:"loss_pct"`
	// JitterMS is the RFC 3550 EWMA jitter the client observed.
	JitterMS float64 `json:"jitter_ms"`
	// LocalDrops is the count of reverse-stream observations the client
	// dropped at its ring boundary. NOT network loss; same semantics as
	// Data.Stream.LocalDrops for the forward direction. Omitempty so the
	// JSON shape stays byte-identical to the v1 snapshot when zero.
	LocalDrops int64 `json:"local_drops,omitempty"`
	// DurationS is the wall-clock duration the server-side sender
	// actually ran for.
	DurationS float64 `json:"duration_s"`
}

// SuspectHopReport mirrors agg.SuspectHop in the renderer's type space so
// the report layer never imports internal/agg directly. The trace driver
// is the adapter between them. Source is the probe source(s) that
// flagged this hop: "icmp", "tcp", or "icmp+tcp" for cross-source
// corroboration. Empty Source is treated as "icmp" by renderers for
// backward compatibility.
type SuspectHopReport struct {
	TTL        int
	Confidence float64
	Evidence   string
	Source     string
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

// Config mirrors the JSON `config` block. The string-valued knobs record
// the user-chosen mode ("on", "off", or "auto") — not the negotiated
// outcome, which surfaces elsewhere (reverse_path_status,
// tcp_corroborate.supported, the presence/absence of reverse_stream).
// Recording the requested mode is what lets automation distinguish
// disabled-by-config from unavailable-on-this-host.
type Config struct {
	RateBPS               int64  `json:"rate_bps"`
	PacketSize            int    `json:"packet_size"`
	MTRIntervalMS         int64  `json:"mtr_interval_ms"`
	MaxHops               int    `json:"max_hops"`
	ReverseTrace          string `json:"reverse_trace"`
	ReverseStream         string `json:"reverse_stream"`
	TCPCorroborate        string `json:"tcp_corroborate"`
	TCPCorroborateRateBPS int64  `json:"tcp_corroborate_rate_bps"`
	// TCPHopProbe / TCPHopProbePort record the user's TCP-mode hop
	// probing knob ("on"/"off"/"auto") and chosen destination port.
	// Omitempty so legacy reports without these knobs stay byte-
	// identical when the feature isn't used.
	TCPHopProbe     string `json:"tcp_hop_probe,omitempty"`
	TCPHopProbePort uint16 `json:"tcp_hop_probe_port,omitempty"`
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
