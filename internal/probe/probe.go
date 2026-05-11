// Package probe implements the forward ICMP path-tracing layer.
//
// One Prober walks TTL 1..MaxHops once per MTRInterval tick and records
// per-hop rolling stats (sent, recv, last/best/worst/avg/stddev RTT,
// loss_pct). Public types (HopStat, Snapshot, Config) and the
// per-Prober contract (New / Run / Close) are platform-neutral; the
// transport differs by OS.
//
//   - Linux / macOS / *BSD (probe_unix.go) — raw ICMP socket via
//     golang.org/x/net/icmp, demuxing replies by echo id+seq.
//   - Windows (probe_windows.go) — iphlpapi.dll!IcmpSendEcho2Ex,
//     one synchronous call per TTL. Windows raw ICMP sockets drop
//     TimeExceeded (issue #1); IcmpSendEcho2Ex surfaces it correctly
//     and does not require Administrator.
//
// v1 is IPv4-only.
//
// Concurrency: business goroutines push immutable Snapshot structs into
// the out channel; the TUI consumes via a re-arming tea.Cmd. Probe code
// never touches the UI.
package probe

import (
	"net"
	"time"
)

// HopStat is one hop's rolling stats at the time a Snapshot was emitted.
// LossPct is in [0,100]; RTT fields are milliseconds. Addr is empty
// when no reply has ever been seen for this TTL.
type HopStat struct {
	TTL         int
	Addr        string
	Sent        int64
	Recv        int64
	LastRTTMS   float64
	BestRTTMS   float64
	WorstRTTMS  float64
	AvgRTTMS    float64
	StdDevRTTMS float64
	// BaselineRTTMS is the per-hop EWMA-smoothed RTT in milliseconds,
	// updated on every reply. Renderers use the (LastRTTMS,
	// BaselineRTTMS, StdDevRTTMS) triple to flag rising-RTT hops in the
	// live TUI without waiting for the end-of-test correlator.
	BaselineRTTMS float64
	LossPct       float64
	// Terminus is true when this hop has ever returned an EchoReply
	// (i.e. it is the destination, not an intermediate router).
	Terminus bool
}

// Snapshot is emitted once per completed sweep. Hops is sorted by TTL
// ascending and contains only hops that have been probed at least once.
// Hops past the first responding terminus are omitted.
type Snapshot struct {
	At   time.Time
	Hops []HopStat
}

// Config governs a Prober. Target must be an IPv4 address; v1 does not
// support v6.
type Config struct {
	Target      net.IP
	MTRInterval time.Duration
	MaxHops     int
}

// baseLossTimeout is the floor for the per-probe settle window. Mirrors
// Linux mtr's per-probe timeout (mtr's default is 10s; we use 2s —
// short enough to surface real loss inside a 60s test, long enough that
// a slow path doesn't false-positive at the 1Hz sweep cadence). For
// long-cadence sweeps the effective timeout scales with MTRInterval (see
// lossTimeout) so a 5s reverse-trace sweep gets a proportionally longer
// window before unreplied probes count as lost.
const baseLossTimeout = 2 * time.Second

// lossTimeout returns the effective per-probe settle window for the
// given sweep cadence. Until a probe ages past this, it must not be
// counted as lost.
func lossTimeout(mtrInterval time.Duration) time.Duration {
	if t := 2 * mtrInterval; t > baseLossTimeout {
		return t
	}
	return baseLossTimeout
}

// buildSnapshot constructs a Snapshot from the current hop state. Caller
// must hold any lock that guards hops/terminus; this function does not
// take the lock itself. Hops past the first-seen terminus are pruned;
// hops that have never been probed are skipped.
func buildSnapshot(hops []*hopState, terminus int, at time.Time) Snapshot {
	maxTTL := len(hops)
	if terminus > 0 {
		maxTTL = terminus
	}
	out := make([]HopStat, 0, maxTTL)
	for ttl := 1; ttl <= maxTTL; ttl++ {
		h := hops[ttl-1]
		if h.sent == 0 {
			continue
		}
		out = append(out, h.snapshot(ttl))
	}
	return Snapshot{At: at, Hops: out}
}
