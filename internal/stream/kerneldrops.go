package stream

// KernelDropSampler reports the cumulative count of UDP datagrams dropped
// by the kernel for the receiver socket — distinct from packets lost on
// the network and from receiver-process ring overflow (LocalDrops).
//
// Platform implementations:
//   - Linux:   parses /proc/net/snmp Udp line "InErrors".
//   - Windows: GetUdpStatistics() from iphlpapi via golang.org/x/sys/windows.
//   - Other:   nopSampler — always returns 0.
//
// Use NewKernelDropSampler to construct the right one for the current
// platform; it is the only path callers should reach for.
type KernelDropSampler interface {
	Sample() (int64, error)
}

// nopSampler is the zero-value sampler: returns 0 with no error. Used on
// platforms without a real implementation, and as a default in
// ReceiverConfig.
type nopSampler struct{}

func (nopSampler) Sample() (int64, error) { return 0, nil }

// NewKernelDropSampler returns the platform's KernelDropSampler. Never
// nil; on unsupported platforms returns a nopSampler so the receiver's
// per-tick Sample() call still has a stable contract.
func NewKernelDropSampler() KernelDropSampler {
	return newPlatformSampler()
}

// baselineSampler wraps a KernelDropSampler and reports test-window
// deltas instead of the underlying absolute counter. The platform
// samplers read host-wide protocol counters (Linux /proc/net/snmp Udp
// InErrors, Windows GetUdpStatisticsEx) — those include drops from
// before the test started and from other UDP sockets on the host, so a
// clean test would otherwise inherit unrelated noise.
//
// The first Sample() call latches the underlying value as the baseline
// and returns 0; subsequent calls return current-baseline. If the
// underlying counter ever moves backward (counter reset, wrap on a
// long-lived host) the wrapper re-anchors to the new floor rather than
// returning a bogus large negative-looking number.
//
// Not safe for concurrent use; the Receiver.Run loop is the only caller.
type baselineSampler struct {
	inner    KernelDropSampler
	baseline int64
	ready    bool
}

func newBaselineSampler(inner KernelDropSampler) *baselineSampler {
	return &baselineSampler{inner: inner}
}

func (b *baselineSampler) Sample() (int64, error) {
	n, err := b.inner.Sample()
	if err != nil {
		return 0, err
	}
	if !b.ready {
		b.baseline = n
		b.ready = true
		return 0, nil
	}
	if n < b.baseline {
		b.baseline = n
		return 0, nil
	}
	return n - b.baseline, nil
}
