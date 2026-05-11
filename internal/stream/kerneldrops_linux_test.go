//go:build linux

package stream

import (
	"strings"
	"testing"
)

const realisticSNMP = `Ip: Forwarding DefaultTTL InReceives ...
Ip: 1 64 12345 0
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors
Tcp: 1 200 120000 -1 100 50 0 0 5 5000 6000 10 0 0 0
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti
Udp: 12345 67 42 9876 13 0 0 0
UdpLite: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti
UdpLite: 0 0 0 0 0 0 0 0
`

func TestParseSnmpUDPInErrorsCanonical(t *testing.T) {
	got, err := parseSnmpUDPInErrors([]byte(realisticSNMP))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != 42 {
		t.Errorf("InErrors: want 42, got %d", got)
	}
}

func TestParseSnmpUDPInErrorsReorderedColumns(t *testing.T) {
	// Hypothetical kernel that swaps the InErrors column to the end.
	in := `Udp: InDatagrams NoPorts OutDatagrams RcvbufErrors InErrors
Udp: 12345 67 9876 13 99
`
	got, err := parseSnmpUDPInErrors([]byte(in))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != 99 {
		t.Errorf("InErrors after reorder: want 99, got %d", got)
	}
}

func TestParseSnmpUDPInErrorsMissingColumn(t *testing.T) {
	// Header advertises no InErrors column — sampler must surface an error
	// rather than silently report 0 for a different column.
	in := `Udp: InDatagrams NoPorts OutDatagrams RcvbufErrors
Udp: 12345 67 9876 13
`
	if _, err := parseSnmpUDPInErrors([]byte(in)); err == nil {
		t.Fatal("expected error for missing InErrors column, got nil")
	}
}

func TestParseSnmpUDPInErrorsMalformedValue(t *testing.T) {
	in := `Udp: InDatagrams NoPorts InErrors OutDatagrams
Udp: 1 2 not-a-number 4
`
	if _, err := parseSnmpUDPInErrors([]byte(in)); err == nil {
		t.Fatal("expected parse error for non-numeric InErrors, got nil")
	}
}

func TestParseSnmpUDPInErrorsHeaderOnly(t *testing.T) {
	in := `Udp: InDatagrams NoPorts InErrors OutDatagrams
`
	if _, err := parseSnmpUDPInErrors([]byte(in)); err == nil {
		t.Fatal("expected error when only header line is present, got nil")
	}
}

func TestParseSnmpUDPInErrorsNoUDPBlock(t *testing.T) {
	in := `Tcp: header
Tcp: 0 1 2
`
	if _, err := parseSnmpUDPInErrors([]byte(in)); err == nil {
		t.Fatal("expected error when no Udp block is present, got nil")
	}
}

func TestParseSnmpUDPInErrorsCRLF(t *testing.T) {
	// Defensive: \r\n line endings shouldn't sneak into the trailing field.
	in := strings.ReplaceAll(realisticSNMP, "\n", "\r\n")
	got, err := parseSnmpUDPInErrors([]byte(in))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != 42 {
		t.Errorf("InErrors with CRLF: want 42, got %d", got)
	}
}
