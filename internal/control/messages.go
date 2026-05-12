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
	// TypeTCPProbe is sent by a client opening a *second* TCP connection
	// to the dropline server port for TCP retransmit corroboration. The
	// server's accept loop dispatches by first-message type: Hello → a
	// loss-test session, TCPProbe → the probe handler that reads and
	// discards bytes for the test's duration so the client's TCP stack
	// has something to retransmit.
	TypeTCPProbe Type = "tcp_probe"
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
	// ReverseStream is the client's reverse-direction UDP loss preference:
	// "on", "off", or "auto". Empty/absent on the wire means the client
	// doesn't know about reverse-stream (pre-feature build) and the server
	// treats it as "off". Resolution mirrors ReverseTrace.
	ReverseStream string `json:"reverse_stream,omitempty"`
	// ReverseFlowID is the FlowID the server should stamp on its
	// reverse-direction UDP packets so the client's Receiver can filter
	// them out from any stray traffic on its dialed socket. Distinct from
	// FlowID (which is forward-direction). Zero/absent (old client) means
	// reverse-stream is off regardless of ReverseStream.
	ReverseFlowID uint32 `json:"reverse_flow_id,omitempty"`
	// TCPCorroborate is the client's TCP retransmit corroboration
	// preference: "on", "off", or "auto". When resolved "on", the client
	// opens a second TCP connection to the dropline server and streams
	// dummy bytes for the test duration, sampling its own TCP_INFO.
	// Empty/absent (old client) → server treats as "off".
	TCPCorroborate string `json:"tcp_corroborate,omitempty"`
	// TCPCorroborateRateBPS is the bit-rate the client intends to push on
	// the corroboration TCP probe. The server doesn't need this value to
	// function (it just reads-and-discards), but it's included so the
	// server can reject implausible rates if needed. Zero/absent means
	// the server-side handler doesn't enforce anything.
	TCPCorroborateRateBPS int64 `json:"tcp_corroborate_rate_bps,omitempty"`
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
	// ReverseStream is the server's resolved decision: "on" if the server
	// will stream UDP back to the client during this session, "off"
	// otherwise. Empty/absent (old server) means "off" to a new client.
	ReverseStream string `json:"reverse_stream,omitempty"`
	// ReverseFlowID echoes the client's request when accepted, or is zero
	// when the server declined reverse-stream. Distinct from FlowID
	// (which is the forward-direction id).
	ReverseFlowID uint32 `json:"reverse_flow_id,omitempty"`
	// TCPCorroborate is the server's resolved decision: "on" if the
	// server is prepared to accept a TCP corroboration probe connection
	// for this session, "off" otherwise. Empty/absent (old server) means
	// "off" to a new client.
	TCPCorroborate string `json:"tcp_corroborate,omitempty"`
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
//
// New-in-v2 fields (ReverseSent, ReverseDurationS, TCPBytesRetrans,
// TCPBytesOut) are omitempty so an old server's wire bytes remain
// byte-identical when the feature isn't active. A new client reading an
// old server's Final sees zero values, which renderers treat as "no
// reverse-stream / no tcp-corroborate data for this session" and skip
// the corresponding sections.
type FinalStats struct {
	Sent        int64        `json:"sent"`
	Recv        int64        `json:"recv"`
	Loss        int64        `json:"loss"`
	OutOfOrder  int64        `json:"out_of_order"`
	Duplicates  int64        `json:"duplicates"`
	JitterMS    float64      `json:"jitter_ms"`
	KernelDrops int64        `json:"kernel_drops"`
	// LocalDrops counts observations the receiver process dropped at its
	// ring boundary (hub flow queue or direct-conn stat channel). NOT
	// network loss: each dropped observation also creates a seq gap that
	// the aggregator counts as Lost, so a non-zero LocalDrops means the
	// reported Loss is inflated by however many local drops occurred and
	// the verdict should be flagged suspect. Omitempty preserves wire
	// compatibility with old servers that never set this field.
	LocalDrops  int64        `json:"local_drops,omitempty"`
	DurationS   float64      `json:"duration_s"`
	Bursts      BurstBuckets `json:"bursts"`
	RateTxBPS   int64        `json:"rate_tx_bps"`
	RateRxBPS   int64        `json:"rate_rx_bps"`
	// ReverseSent is the count of UDP packets the server transmitted back
	// to the client during the reverse-stream sub-test. Zero when reverse
	// stream was off for this session.
	ReverseSent int64 `json:"reverse_sent,omitempty"`
	// ReverseDurationS is the wall-clock duration the server's reverse
	// Sender actually ran for. Useful for the client to validate its own
	// recv-window. Zero when reverse stream was off.
	ReverseDurationS float64 `json:"reverse_duration_s,omitempty"`
	// TCPBytesRetrans is the cumulative count of TCP bytes the server's
	// kernel believes it retransmitted on the corroboration probe socket
	// for this session. Server-side observation; the client also samples
	// its own end independently. Zero when tcp-corroborate was off or
	// when the server's platform doesn't expose TCP_INFO.
	TCPBytesRetrans uint64 `json:"tcp_bytes_retrans,omitempty"`
	// TCPBytesOut is the cumulative count of TCP bytes the server's
	// kernel sent (including retransmits) on the corroboration probe
	// socket. Used by the client as a denominator if it wants the
	// server-side retransmit fraction. Zero when tcp-corroborate was off.
	TCPBytesOut uint64 `json:"tcp_bytes_out,omitempty"`
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

// TCPProbe is the first message a client sends on a *second* TCP
// connection to the dropline server port, opted-in via Hello's
// TCPCorroborate. The server's accept loop dispatches by first-message
// type: Hello → a normal loss-test session, TCPProbe → the probe
// handler that reads and discards bytes until the connection closes or
// the test's duration elapses, so the client's TCP stack has something
// to retransmit.
//
// SessionID matches the Ready.SessionID the client received on its
// control channel. The server doesn't strictly require it to function
// (each probe socket is independent) but logging it lets operators
// correlate probe traffic with a session in server-side traces.
//
// RateBPS is the bit rate the client intends to push on this probe.
// The server-side handler doesn't enforce it — the field is advisory.
type TCPProbe struct {
	Type      Type   `json:"type"`
	SessionID string `json:"session_id"`
	RateBPS   int64  `json:"rate_bps,omitempty"`
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
func (*TCPProbe) controlMessage()         {}
