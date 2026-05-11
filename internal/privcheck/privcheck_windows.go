//go:build windows

package privcheck

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// iphlpapi procs the prober relies on. Mirrors the eager-load
// discipline in internal/stream/kerneldrops_windows.go.
var (
	iphlpapiDLL         = windows.NewLazySystemDLL("iphlpapi.dll")
	procIcmpCreateFile  = iphlpapiDLL.NewProc("IcmpCreateFile")
	procIcmpCloseHandle = iphlpapiDLL.NewProc("IcmpCloseHandle")
	procIcmpSendEcho2Ex = iphlpapiDLL.NewProc("IcmpSendEcho2Ex")
	requiredProcs       = []*windows.LazyProc{procIcmpCreateFile, procIcmpCloseHandle, procIcmpSendEcho2Ex}
)

// RawICMP reports whether this process can perform ICMP probing.
//
// dropline on Windows uses iphlpapi.dll!IcmpSendEcho2Ex (the same API
// tracert.exe uses); this requires neither raw sockets nor
// Administrator. The check therefore verifies that iphlpapi.dll loads
// and the procs we call resolve — the same eager-load pattern as
// internal/stream/kerneldrops_windows.go.
func RawICMP() Status {
	if err := iphlpapiDLL.Load(); err != nil {
		return Status{OK: false, Reason: fmt.Sprintf("iphlpapi.dll unavailable: %v", err)}
	}
	for _, p := range requiredProcs {
		if err := p.Find(); err != nil {
			return Status{OK: false, Reason: fmt.Sprintf("iphlpapi.dll missing %s: %v", p.Name, err)}
		}
	}
	return Status{OK: true}
}
