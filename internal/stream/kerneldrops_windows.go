//go:build windows

package stream

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows kernel-drop sampler. iphlpapi exposes GetUdpStatisticsEx which
// fills a MIB_UDPSTATS struct of five DWORDs; we read InErrors. Anything
// non-zero from the call surfaces as a sampler error and the receiver's
// Run loop logs nothing — the previous SetKernelDrops value sticks.
//
// We bind the DLL via windows.NewLazySystemDLL (safe against preloading
// attacks) so this stays pure Go and CGO_ENABLED=0 friendly.

type mibUDPStats struct {
	InDatagrams  uint32
	NoPorts      uint32
	InErrors     uint32
	OutDatagrams uint32
	NumAddrs     uint32
}

var (
	iphlpapiDLL            = windows.NewLazySystemDLL("iphlpapi.dll")
	procGetUdpStatisticsEx = iphlpapiDLL.NewProc("GetUdpStatisticsEx")
)

type windowsSampler struct{}

func (windowsSampler) Sample() (int64, error) {
	var stats mibUDPStats
	r1, _, _ := procGetUdpStatisticsEx.Call(
		uintptr(unsafe.Pointer(&stats)),
		uintptr(windows.AF_INET),
	)
	if r1 != 0 {
		return 0, fmt.Errorf("GetUdpStatisticsEx: status %d", r1)
	}
	return int64(stats.InErrors), nil
}

// newPlatformSampler probes iphlpapi.dll and its GetUdpStatisticsEx
// entry point eagerly. If either fails to load (Wine, stripped-down
// SKUs without the IP Helper service, etc.) the lazy proc's later
// .Call() would panic from inside x/sys/windows. Falling back to
// nopSampler keeps the receiver's per-tick Sample() contract stable
// at the cost of always reporting kernel_drops=0 on the affected host.
func newPlatformSampler() KernelDropSampler {
	if err := iphlpapiDLL.Load(); err != nil {
		return nopSampler{}
	}
	if err := procGetUdpStatisticsEx.Find(); err != nil {
		return nopSampler{}
	}
	return windowsSampler{}
}
