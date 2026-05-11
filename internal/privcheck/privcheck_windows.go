//go:build windows

package privcheck

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// RawICMP reports whether this process is running elevated. Raw ICMP on
// Windows requires Administrator; v1 does not fall back to IcmpSendEcho2.
func RawICMP() Status {
	var token windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_QUERY, &token); err != nil {
		return Status{OK: false, Reason: fmt.Sprintf("OpenProcessToken failed: %v", err)}
	}
	defer token.Close()
	if token.IsElevated() {
		return Status{OK: true}
	}
	return Status{OK: false, Reason: "raw ICMP requires Administrator (run from an elevated prompt)"}
}
