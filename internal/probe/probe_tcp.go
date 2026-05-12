package probe

import (
	"net"
	"time"
)

// TCPSnapshot is emitted once per completed TCP-mode sweep. Same per-hop
// shape as Snapshot — the renderer / aggregator treat them as parallel
// signals. Supported is the "degrade silently on Windows" gate: a
// TCPProber whose platform can't do TCP-mode hop probing (currently
// just Windows, since iphlpapi has no TCP-traceroute equivalent and
// CGO_ENABLED=0 rules out WinDivert) emits a single Snapshot with
// Supported=false and empty Hops, then blocks on ctx. Renderers branch
// on Supported to show "(unsupported on this platform)" instead of an
// empty hop table.
type TCPSnapshot struct {
	At        time.Time
	Supported bool
	Port      uint16
	Hops      []HopStat
}

// TCPConfig governs a TCPProber. Target must be IPv4 (v1). Port is the
// destination port the SYN probes target; the destination does not need
// to be listening — intermediate routers still emit TimeExceeded for
// any TTL expiry, and a closed-port RST is treated as terminus the same
// way EchoReply is for ICMP probing.
type TCPConfig struct {
	Target      net.IP
	Port        uint16
	MTRInterval time.Duration
	MaxHops     int
}
