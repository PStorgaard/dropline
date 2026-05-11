package control

// Type is the wire discriminator carried in every control message.
type Type string

// Type constants — wire discriminator values.
const (
	TypeHello            Type = "hello"
	TypeReady            Type = "ready"
	TypeError            Type = "error"
	TypeStats            Type = "stats"
	TypeFinal            Type = "final"
	TypeReverseHopUpdate Type = "reverse_hop_update"
	TypePause            Type = "pause"
)

// Reverse-path status values. Reported as `reverse_path_status` in the
// JSON document and consulted in renderers / aggregator state. Defined
// here so the same literals don't drift across the trace driver, the
// aggregator, and both renderers.
const (
	ReverseStatusOK               = "ok"
	ReverseStatusDisabledByServer = "disabled_by_server"
	ReverseStatusDisabledByClient = "disabled_by_client"
	ReverseStatusTerminatedAtNAT  = "terminated_at_nat"
)

// FriendlyReverseStatus maps the wire status string into the short
// human form shown in the TUI / text-report header. The JSON keeps the
// wire form (snake_case) for machine consumers.
func FriendlyReverseStatus(s string) string {
	switch s {
	case ReverseStatusTerminatedAtNAT:
		return "terminated at NAT"
	case ReverseStatusDisabledByServer:
		return "disabled by server"
	case ReverseStatusDisabledByClient:
		return "disabled by client"
	default:
		return s
	}
}

// Message is the marker interface implemented by every control message type.
type Message interface{ controlMessage() }

// Hello is sent by the client to initiate a session.
//
// ReverseTrace carries the client's reverse-trace preference: "on", "off",
// or "auto". Empty/absent on the wire means "auto" — that's what an old
// client (no flag awareness) effectively requests; the server honors it.
type Hello struct {
	Type         Type   `json:"type"`
	Version      int    `json:"version"`
	Mode         string `json:"mode"`
	RateBPS      int64  `json:"rate_bps"`
	DurationMS   int64  `json:"duration_ms"`
	PacketSize   int    `json:"packet_size"`
	FlowID       uint32 `json:"flow_id"`
	ReverseTrace string `json:"reverse_trace,omitempty"`
	// MTRIntervalMS is the client's --mtr-interval in milliseconds. The
	// server uses it for the reverse prober's TTL-walk cadence so the two
	// directions sweep in lockstep. Zero/missing (old client) falls back
	// to 1s server-side.
	MTRIntervalMS int64 `json:"mtr_interval_ms,omitempty"`
}

// Ready acknowledges a Hello and carries the server-assigned session ID.
//
// ReverseTrace is the server's resolved decision: "on" if a reverse trace
// will be streamed during the session, "off" otherwise. Empty/absent on
// the wire (old server) means "off" to a new client.
type Ready struct {
	Type         Type   `json:"type"`
	SessionID    string `json:"session_id"`
	ReverseTrace string `json:"reverse_trace,omitempty"`
}

// Error reports a session-fatal condition (server busy, version mismatch, …).
type Error struct {
	Type   Type   `json:"type"`
	Reason string `json:"reason"`
}

// Stats is sent by the server during a test, typically once per second.
type Stats struct {
	Type        Type    `json:"type"`
	T           float64 `json:"t"`
	Recv        int64   `json:"recv"`
	Loss        int64   `json:"loss"`
	JitterMS    float64 `json:"jitter_ms"`
	KernelDrops int64   `json:"kernel_drops"`
}

// FinalStats summarizes a completed test. Field set tracks the
// JSON-output schema in spec § JSON output.
type FinalStats struct {
	Sent        int64        `json:"sent"`
	Recv        int64        `json:"recv"`
	Loss        int64        `json:"loss"`
	OutOfOrder  int64        `json:"out_of_order"`
	Duplicates  int64        `json:"duplicates"`
	JitterMS    float64      `json:"jitter_ms"`
	KernelDrops int64        `json:"kernel_drops"`
	DurationS   float64      `json:"duration_s"`
	Bursts      BurstBuckets `json:"bursts"`
	RateTxBPS   int64        `json:"rate_tx_bps"`
	RateRxBPS   int64        `json:"rate_rx_bps"`
}

// BurstBuckets is the loss-burst histogram carried in FinalStats.
// Buckets per spec § internal/stream: 1, 2, 3-9, 10-99, 100+, plus
// max-burst length. Defined here (rather than imported from
// internal/stream) so internal/control stays free of stream's
// transport concerns.
type BurstBuckets struct {
	One       int64 `json:"1"`
	Two       int64 `json:"2"`
	Three9    int64 `json:"3-9"`
	Ten99     int64 `json:"10-99"`
	HundredUp int64 `json:"100+"`
	Max       int64 `json:"max"`
}

// Final is sent by the server at the end of a test and carries FinalStats.
type Final struct {
	Type  Type       `json:"type"`
	Stats FinalStats `json:"stats"`
}

// ReverseHopUpdate is a server→client message carrying one resolved hop
// from the server's reverse trace toward the client. Identical-shape
// to forward hops modulo two intentional differences: Addr in place of
// IP, no Suspect flag (the correlator only runs against forward-path
// stream loss).
//
// All rolling-stat fields are omitempty so an old (slim-shape) server's
// messages stay byte-for-byte identical on the wire. New clients
// reading old-server messages see zero values and degrade to the slim
// render naturally.
type ReverseHopUpdate struct {
	Type Type   `json:"type"`
	TTL  int    `json:"ttl"`
	Addr string `json:"addr"`
	// RTTMS is the latest sample (semantically equivalent to BestRTTMS
	// in the forward HopStat). Kept as a top-level field for backward
	// compatibility with the slim wire shape.
	RTTMS         float64 `json:"rtt_ms"`
	LossPct       float64 `json:"loss_pct"`
	Terminus      string  `json:"terminus,omitempty"`
	Sent          int64   `json:"sent,omitempty"`
	Recv          int64   `json:"recv,omitempty"`
	BestRTTMS     float64 `json:"rtt_ms_best,omitempty"`
	WorstRTTMS    float64 `json:"rtt_ms_worst,omitempty"`
	AvgRTTMS      float64 `json:"rtt_ms_avg,omitempty"`
	StdDevRTTMS   float64 `json:"rtt_ms_stddev,omitempty"`
	BaselineRTTMS float64 `json:"rtt_ms_baseline,omitempty"`
	// MaxTTL is the highest TTL the server's reverse prober is currently
	// probing — i.e. the detected terminus, or MaxHops if none. The client
	// prunes any cached reverse hop with TTL > MaxTTL on ingest so the
	// hop table contracts once the prober finds the path end.
	// Omitempty: old servers don't set it, in which case the client keeps
	// the legacy merge-only behavior.
	MaxTTL int `json:"max_ttl,omitempty"`
}

// Pause is a client→server toggle that halts (or resumes) the stats
// forwarder for the current session. Idempotent — re-sending the same
// state is a no-op. The first non-Hello message in the client→server
// direction; the server's recv watcher demuxes it, treating any other
// message as a fatal disconnect (preserves today's strict shape). The
// session's duration deadline is intentionally NOT paused — pause is
// for inspecting the dashboard, not extending the test.
type Pause struct {
	Type   Type `json:"type"`
	Paused bool `json:"paused"`
}

func (*Hello) controlMessage()            {}
func (*Ready) controlMessage()            {}
func (*Error) controlMessage()            {}
func (*Stats) controlMessage()            {}
func (*Final) controlMessage()            {}
func (*ReverseHopUpdate) controlMessage() {}
func (*Pause) controlMessage()            {}
