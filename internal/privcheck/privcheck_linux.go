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

// RawICMP reports whether this process holds CAP_NET_RAW (or is root).
func RawICMP() Status {
	if os.Geteuid() == 0 {
		return Status{OK: true}
	}
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
