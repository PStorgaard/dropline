//go:build linux

package stream

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Linux kernel-drop sampler. /proc/net/snmp lists per-protocol counter
// blocks as a paired "header / data" line set, both prefixed by the
// protocol name. We parse the Udp pair and surface InErrors.
//
// Why InErrors and not InErrors+RcvbufErrors per spec: the kernel's
// RcvbufErrors counter is a *subset* of InErrors (the buffer-overflow
// portion of all errored UDP receives). Summing would double-count.
// InErrors is the conservative drop number that satisfies spec intent
// ("flag network_loss as suspect when kernel drops > 0").

const procNetSNMP = "/proc/net/snmp"

type linuxSampler struct{ path string }

func (s linuxSampler) Sample() (int64, error) {
	path := s.path
	if path == "" {
		path = procNetSNMP
	}
	b, err := os.ReadFile(path) // #nosec G304 -- fixed-path /proc file
	if err != nil {
		return 0, err
	}
	return parseSnmpUDPInErrors(b)
}

// parseSnmpUDPInErrors walks /proc/net/snmp content and returns the value
// of the Udp block's "InErrors" column. The parser is robust to column
// reordering across kernel versions: it locates InErrors by name in the
// header line rather than assuming a fixed position.
func parseSnmpUDPInErrors(b []byte) (int64, error) {
	var headerCols []string
	var dataFields []string

	for _, raw := range strings.Split(string(b), "\n") {
		line := strings.TrimRight(raw, "\r")
		const prefix = "Udp: "
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(line[len(prefix):])
		if len(fields) == 0 {
			continue
		}
		// Header lines contain column names (non-numeric); data lines
		// contain integers. Detect by trying to parse the first field.
		if _, err := strconv.ParseInt(fields[0], 10, 64); err == nil {
			dataFields = fields
		} else {
			headerCols = fields
		}
		if headerCols != nil && dataFields != nil {
			break
		}
	}

	if headerCols == nil {
		return 0, errors.New("snmp: Udp header line not found")
	}
	if dataFields == nil {
		return 0, errors.New("snmp: Udp data line not found")
	}
	idx := -1
	for i, c := range headerCols {
		if c == "InErrors" {
			idx = i
			break
		}
	}
	if idx < 0 {
		return 0, errors.New("snmp: Udp.InErrors column not present")
	}
	if idx >= len(dataFields) {
		return 0, fmt.Errorf("snmp: Udp data shorter (%d) than header InErrors index (%d)", len(dataFields), idx)
	}
	n, err := strconv.ParseInt(dataFields[idx], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("snmp: parse Udp.InErrors %q: %w", dataFields[idx], err)
	}
	return n, nil
}

func newPlatformSampler() KernelDropSampler { return linuxSampler{} }
