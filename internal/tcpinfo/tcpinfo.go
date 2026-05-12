// Package tcpinfo samples per-connection TCP retransmit + RTT counters
// from the host kernel. Used by the dropline TCP-corroboration probe to
// answer "is the observed UDP loss UDP-specific, or path-wide?".
//
// The API is intentionally tiny: one Stats struct, one Sampler
// interface, one New constructor. Platform splits live in tcpinfo_linux.go
// (TCP_INFO via getsockopt), tcpinfo_windows.go (SIO_TCP_INFO via
// WSAIoctl, TCP_INFO_v0 layout), and tcpinfo_other.go (no-op fallback
// for macOS / BSDs / unknown). Cross-platform call site stays unaware
// of which path it's on.
//
// The Sampler does not own the *net.TCPConn it inspects — closing the
// conn is the caller's responsibility, and Close on the Sampler is a
// no-op kept for API symmetry with kernel-resource samplers.
package tcpinfo

// Stats is the cross-platform subset of per-connection TCP metrics we
// care about for loss corroboration. Values are cumulative on the
// connection (BytesOut, BytesRetrans grow monotonically); the caller
// computes deltas between two Sample()s if it wants per-window stats.
//
// On Linux BytesRetrans is synthesized as `tcpi_total_retrans *
// tcpi_snd_mss` — a slight overestimate when retransmitted segments
// were smaller than the current MSS, but adequate as a "is retransmit
// activity happening?" corroboration signal. On Windows BytesRetrans
// is the kernel's exact byte counter from TCP_INFO_v0.BytesRetrans.
type Stats struct {
	// Supported is true when this Stats was filled by an actual
	// TCP_INFO read; false when the platform/host doesn't expose the
	// underlying syscall (macOS/BSD fallback, pre-Win10-1703 Windows
	// after SIO_TCP_INFO returned WSAEINVAL). When false the byte/RTT
	// fields are all zero and renderers should display "unsupported"
	// rather than "0 retransmits".
	Supported bool
	// BytesRetrans is the cumulative count of bytes the local TCP
	// stack believes it has retransmitted on this connection.
	BytesRetrans uint64
	// BytesOut is the cumulative count of bytes the local TCP stack
	// has transmitted (including retransmits). Used as a denominator
	// when the caller wants a retransmit-fraction.
	BytesOut uint64
	// RttUs is the current smoothed RTT estimate in microseconds, or
	// 0 if the platform doesn't expose it.
	RttUs uint32
	// MinRttUs is the minimum RTT observed on this connection in
	// microseconds, or 0 if the platform doesn't expose it.
	MinRttUs uint32
}

// Sampler reads TCP_INFO from one underlying connection on demand.
// Sample() is safe to call concurrently with the connection's own
// Read/Write; it issues a getsockopt-equivalent syscall and returns
// without touching the connection's data path.
//
// On platforms that don't support TCP_INFO retrieval the Sampler is a
// nopSampler that returns Stats{Supported: false} with nil error — call
// sites branch on Supported to distinguish "unsupported" from "no
// retransmits observed".
type Sampler interface {
	Sample() (Stats, error)
	Close() error
}

// nopSampler is the fallback used on platforms without a TCP_INFO
// retrieval path. Always returns Stats{Supported: false} and nil error
// so call sites can stay unconditional and surface "unsupported"
// distinctly from "zero retransmits".
type nopSampler struct{}

func (nopSampler) Sample() (Stats, error) { return Stats{Supported: false}, nil }
func (nopSampler) Close() error           { return nil }
