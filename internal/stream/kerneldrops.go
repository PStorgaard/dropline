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
