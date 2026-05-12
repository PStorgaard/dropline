//go:build linux

package privcheck

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// capNetRaw is the bit position of CAP_NET_RAW in the Linux capability bitmap.
const capNetRaw = 13

// RawICMP reports whether this process effectively holds CAP_NET_RAW.
//
// Note: euid is NOT a shortcut. Containers and systemd sandboxes can
// drop CAP_NET_RAW from root via CapabilityBoundingSet / AmbientCaps /
// k8s securityContext.capabilities.drop, in which case raw ICMP open
// will fail despite uid 0. Parsing CapEff handles both cases correctly.
func RawICMP() Status {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return Status{OK: false, Reason: fmt.Sprintf("unable to read /proc/self/status: %v", err)}
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "CapEff:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return Status{OK: false, Reason: "malformed CapEff line in /proc/self/status"}
		}
		caps, err := strconv.ParseUint(fields[1], 16, 64)
		if err != nil {
			return Status{OK: false, Reason: fmt.Sprintf("unable to parse CapEff: %v", err)}
		}
		if caps&(1<<capNetRaw) != 0 {
			return Status{OK: true}
		}
		return Status{OK: false, Reason: "raw ICMP requires CAP_NET_RAW (run as root or grant: setcap cap_net_raw+ep <binary>)"}
	}
	return Status{OK: false, Reason: "CapEff line not found in /proc/self/status"}
}
