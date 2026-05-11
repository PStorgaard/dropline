//go:build linux

package stream

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const procRmemMax = "/proc/sys/net/core/rmem_max"

// WarnIfRmemMaxBelow reads /proc/sys/net/core/rmem_max and writes a single
// stderr-style warning line to w when the kernel's cap is smaller than
// want bytes. Silent on success, on missing /proc, and on unreadable /
// unparseable contents — the warning is best-effort guidance, not a
// pre-flight check.
func WarnIfRmemMaxBelow(w io.Writer, want int) {
	b, err := os.ReadFile(procRmemMax) // #nosec G304 -- fixed-path /proc file
	if err != nil {
		return
	}
	have, err := parseRmemMax(b)
	if err != nil || have >= want {
		return
	}
	fmt.Fprintf(w, "warning: SO_RCVBUF target %d exceeds net.core.rmem_max=%d; "+
		"receiver will be capped — set `sysctl -w net.core.rmem_max=%d` to lift\n",
		want, have, want)
}

// parseRmemMax extracts the integer rmem_max value from the file contents.
// /proc/sys/net/core/rmem_max is a single integer plus a trailing newline,
// but defensive parsing also tolerates leading/trailing whitespace.
func parseRmemMax(b []byte) (int, error) {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0, errors.New("rmem_max: empty")
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("rmem_max: parse %q: %w", s, err)
	}
	return n, nil
}
