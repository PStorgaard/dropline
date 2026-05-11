//go:build !linux && !windows

package privcheck

// RawICMP is a stub for platforms outside the v1 support matrix
// (linux/amd64, linux/arm64, windows/amd64). Defends darwin dev environments.
func RawICMP() Status {
	return Status{OK: false, Reason: "privilege detection not implemented on this platform"}
}
