//go:build !linux

package stream

import "io"

// WarnIfRmemMaxBelow is a no-op on non-Linux platforms. Windows surfaces
// SO_RCVBUF caps differently (registry / per-process limits) and the
// receiver's SetReadBuffer call already fails silently when capped, which
// is the expected behavior for v1.
func WarnIfRmemMaxBelow(io.Writer, int) {}
