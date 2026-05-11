//go:build !linux && !windows

package stream

// newPlatformSampler is the fallback for platforms without a real
// kernel-drop counter. The receiver still calls Sample() each tick;
// it just always reads zero.
func newPlatformSampler() KernelDropSampler { return nopSampler{} }
