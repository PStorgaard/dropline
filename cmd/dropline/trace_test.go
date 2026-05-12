package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PStorgaard/dropline/internal/control"
	"github.com/PStorgaard/dropline/internal/report"
)

func TestOSC52Encode(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"empty", []byte("")},
		{"ascii", []byte(`{"target":"srv.lab"}`)},
		{"binary-ish", []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 'a', 'b'}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := osc52Encode(c.payload)
			const prefix = "\x1b]52;c;"
			if !bytes.HasPrefix(got, []byte(prefix)) {
				t.Errorf("missing OSC 52 prefix; got %q", got)
			}
			if got[len(got)-1] != '\a' {
				t.Errorf("missing BEL terminator; got last byte %q", got[len(got)-1])
			}
			body := got[len(prefix) : len(got)-1]
			decoded, err := base64.StdEncoding.DecodeString(string(body))
			if err != nil {
				t.Fatalf("base64 decode: %v", err)
			}
			if !bytes.Equal(decoded, c.payload) {
				t.Errorf("round-trip mismatch:\n want %q\n  got %q", c.payload, decoded)
			}
		})
	}
}

func TestMakeCopyFnNilDataReturnsError(t *testing.T) {
	var p atomic.Pointer[report.Data]
	var buf bytes.Buffer
	fn := makeCopyFn(&p, &buf)
	n, err := fn()
	if err == nil {
		t.Fatalf("expected error on nil builtData; got n=%d", n)
	}
	if buf.Len() != 0 {
		t.Errorf("nothing should be written on error; got %d bytes", buf.Len())
	}
}

func TestMakeCopyFnEmitsEscape(t *testing.T) {
	d := &report.Data{
		Version: 1,
		Target:  "srv.lab:5301",
	}
	var p atomic.Pointer[report.Data]
	p.Store(d)

	var buf bytes.Buffer
	fn := makeCopyFn(&p, &buf)
	n, err := fn()
	if err != nil {
		t.Fatalf("CopyFn: %v", err)
	}
	if n <= 0 {
		t.Errorf("byte count should be positive; got %d", n)
	}
	out := buf.Bytes()
	if !bytes.HasPrefix(out, []byte("\x1b]52;c;")) {
		t.Errorf("missing OSC52 prefix in output; got %q", out)
	}
	if out[len(out)-1] != '\a' {
		t.Errorf("missing BEL terminator; got last byte %q", out[len(out)-1])
	}
	// The base64 body must decode to a valid JSON document with the
	// target field intact — proves the wiring round-trips.
	body := out[len("\x1b]52;c;") : len(out)-1]
	decoded, err := base64.StdEncoding.DecodeString(string(body))
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	if !bytes.Contains(decoded, []byte(`"target": "srv.lab:5301"`)) {
		t.Errorf("clipboard payload missing target field; got\n%s", decoded)
	}
}

func TestSanitizePathSegment(t *testing.T) {
	cases := map[string]string{
		"plain":             "plain",
		"with/slash":        "with_slash",
		"back\\slash":       "back_slash",
		"colon:in:host":     "colon_in_host",
		"weird?<>|*\"":      "weird______",
		"control\x01char":   "control_char",
		"":                  "",
	}
	for in, want := range cases {
		if got := sanitizePathSegment(in); got != want {
			t.Errorf("sanitizePathSegment(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDefaultSavePathIncludesHost(t *testing.T) {
	got := defaultSavePath("srv.lab:5301")
	if !strings.HasPrefix(got, "dropline-srv.lab-") {
		t.Errorf("path missing host prefix: %q", got)
	}
	if !strings.HasSuffix(got, ".json") {
		t.Errorf("path missing .json suffix: %q", got)
	}
	// Hostless target falls back to a placeholder rather than a bare
	// `dropline--<unix>.json` that's hard to read at a glance.
	if got := defaultSavePath(""); !strings.HasPrefix(got, "dropline-target-") {
		t.Errorf("empty target should produce 'target' placeholder; got %q", got)
	}
}

func parse(t *testing.T, args []string, isTTY bool) traceConfig {
	t.Helper()
	cfg, err := parseTraceArgs(args, isTTY, io.Discard)
	if err != nil {
		t.Fatalf("parseTraceArgs(%v): %v", args, err)
	}
	return cfg
}

func parseErr(t *testing.T, args []string, isTTY bool) error {
	t.Helper()
	_, err := parseTraceArgs(args, isTTY, io.Discard)
	if err == nil {
		t.Fatalf("parseTraceArgs(%v): expected error, got nil", args)
	}
	return err
}

func TestParseTraceArgsDefaults(t *testing.T) {
	cfg := parse(t, []string{"1.2.3.4"}, true)
	if cfg.target != "1.2.3.4:5301" {
		t.Errorf("target = %q, want 1.2.3.4:5301", cfg.target)
	}
	if cfg.rateBPS != 10_000_000 {
		t.Errorf("rateBPS = %d, want 10000000", cfg.rateBPS)
	}
	if cfg.duration != 60*time.Second {
		t.Errorf("duration = %s, want 60s", cfg.duration)
	}
	if cfg.packetSize != 1200 {
		t.Errorf("packetSize = %d, want 1200", cfg.packetSize)
	}
	if cfg.mtrInterval != time.Second {
		t.Errorf("mtrInterval = %s, want 1s", cfg.mtrInterval)
	}
	if cfg.maxHops != 30 {
		t.Errorf("maxHops = %d, want 30", cfg.maxHops)
	}
	if cfg.output != outputTUI {
		t.Errorf("output = %s, want tui (TTY)", cfg.output)
	}
	if cfg.reverseTrace != reverseAuto {
		t.Errorf("reverseTrace = %s, want auto", cfg.reverseTrace)
	}
	if cfg.savePath != "" {
		t.Errorf("savePath = %q, want empty", cfg.savePath)
	}
}

func TestParseTraceArgsNonTTYDefaultsToReport(t *testing.T) {
	cfg := parse(t, []string{"1.2.3.4"}, false)
	if cfg.output != outputReport {
		t.Errorf("output = %s, want report (non-TTY)", cfg.output)
	}
}

func TestParseTraceArgsExplicitOutputModes(t *testing.T) {
	cases := []struct {
		flag string
		want outputMode
	}{
		{"--tui", outputTUI},
		{"--report", outputReport},
		{"--json", outputJSON},
	}
	for _, tc := range cases {
		cfg := parse(t, []string{tc.flag, "1.2.3.4"}, false)
		if cfg.output != tc.want {
			t.Errorf("%s → output = %s, want %s", tc.flag, cfg.output, tc.want)
		}
	}
}

func TestParseTraceArgsOutputModesMutuallyExclusive(t *testing.T) {
	cases := [][]string{
		{"--tui", "--report", "1.2.3.4"},
		{"--tui", "--json", "1.2.3.4"},
		{"--report", "--json", "1.2.3.4"},
	}
	for _, args := range cases {
		err := parseErr(t, args, true)
		if !strings.Contains(err.Error(), "only one of") {
			t.Errorf("args %v: error = %q, want 'only one of' message", args, err)
		}
	}
}

func TestParseRate(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"10M", 10_000_000, false},
		{"10m", 10_000_000, false},
		{"1G", 1_000_000_000, false},
		{"100K", 100_000, false},
		{"500", 500, false},
		{"0", 0, true},
		{"", 0, true},
		{"10X", 0, true},
		{"abc", 0, true},
		// Overflow: 99999999999G * 1e9 wraps a uint64 multiplication
		// and would silently cast to a negative int64 without the
		// bound check.
		{"99999999999G", 0, true},
		// Exact int64 ceiling.
		{"9999999999G", 0, true},
	}
	for _, tc := range cases {
		got, err := parseRate(tc.in)
		if (err != nil) != tc.err {
			t.Errorf("parseRate(%q): err = %v, want err? %v", tc.in, err, tc.err)
			continue
		}
		if !tc.err && got != tc.want {
			t.Errorf("parseRate(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestParseTraceArgsRateOverride(t *testing.T) {
	cfg := parse(t, []string{"--rate", "1G", "1.2.3.4"}, true)
	if cfg.rateBPS != 1_000_000_000 {
		t.Errorf("rateBPS = %d, want 1e9", cfg.rateBPS)
	}
}

// TestParseTraceArgsMTRIntervalBounds guards the floor and ceiling on
// --mtr-interval. The floor keeps the forward prober's 16-bit ICMP seq
// from wrapping inside the still-live lossTimeout window; the ceiling is
// a usability gate. Values inside the range parse cleanly; values outside
// must surface a "--mtr-interval must be in" error.
func TestParseTraceArgsMTRIntervalBounds(t *testing.T) {
	cases := []struct {
		name   string
		val    string
		reject bool
	}{
		{"zero_rejected", "0", true},
		{"below_floor", "50ms", true},
		{"at_floor", "100ms", false},
		{"default", "1s", false},
		{"at_ceiling", "60s", false},
		{"above_ceiling", "61s", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"--mtr-interval", tc.val, "1.2.3.4"}
			if tc.reject {
				err := parseErr(t, args, true)
				if !strings.Contains(err.Error(), "--mtr-interval") {
					t.Errorf("--mtr-interval=%s: error=%q, want substring \"--mtr-interval\"", tc.val, err)
				}
			} else {
				_ = parse(t, args, true)
			}
		})
	}
}

func TestNormalizeTarget(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"1.2.3.4", "1.2.3.4:5301"},
		{"host", "host:5301"},
		{"1.2.3.4:1234", "1.2.3.4:1234"},
		{"[::1]:9", "[::1]:9"},
		{"::1", "[::1]:5301"},
	}
	for _, tc := range cases {
		got, err := normalizeTarget(tc.in)
		if err != nil {
			t.Errorf("normalizeTarget(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("normalizeTarget(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if _, err := normalizeTarget(""); err == nil {
		t.Error("normalizeTarget(\"\"): expected error")
	}
}

func TestResolveTargetIPv4(t *testing.T) {
	// Bare IPv4 round-trips without a DNS lookup — net.LookupIP on a
	// dotted-quad returns the literal address.
	got, err := resolveTargetIPv4("1.2.3.4:5301")
	if err != nil {
		t.Fatalf("bare v4: %v", err)
	}
	if got != "1.2.3.4:5301" {
		t.Errorf("bare v4: got %q, want 1.2.3.4:5301", got)
	}

	// localhost is dual-stack on most systems; we must always pick the
	// IPv4 form so the rest of the trace pipeline (udp4, tcp4) lines up.
	got, err = resolveTargetIPv4("localhost:5301")
	if err != nil {
		t.Fatalf("localhost: %v", err)
	}
	host, _, err := net.SplitHostPort(got)
	if err != nil {
		t.Fatalf("split %q: %v", got, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil {
		t.Errorf("localhost resolved to %q, want an IPv4 address", host)
	}

	// IPv6 literal has no IPv4 form — must surface a clear error rather
	// than fall through to a bogus address.
	if _, err := resolveTargetIPv4("[::1]:5301"); err == nil {
		t.Error("IPv6 literal: expected error, got nil")
	}

	// Malformed input (no port) should surface a split error rather
	// than panic.
	if _, err := resolveTargetIPv4("noport"); err == nil {
		t.Error("missing port: expected error, got nil")
	}
}

func TestParseTraceArgsTargetRequired(t *testing.T) {
	err := parseErr(t, nil, true)
	if !strings.Contains(err.Error(), "missing TARGET") {
		t.Errorf("error = %q, want 'missing TARGET'", err)
	}
}

func TestParseTraceArgsRejectsExtraPositionals(t *testing.T) {
	err := parseErr(t, []string{"a", "b"}, true)
	if !strings.Contains(err.Error(), "expected one TARGET") {
		t.Errorf("error = %q, want 'expected one TARGET'", err)
	}
}

func TestParseTraceArgsPacketSizeBounds(t *testing.T) {
	if _, err := parseTraceArgs([]string{"--packet-size", "16", "h"}, true, io.Discard); err == nil {
		t.Error("packet-size 16: expected error")
	}
	if _, err := parseTraceArgs([]string{"--packet-size", "32", "h"}, true, io.Discard); err != nil {
		t.Errorf("packet-size 32: unexpected error %v", err)
	}
	if _, err := parseTraceArgs([]string{"--packet-size", "65507", "h"}, true, io.Discard); err != nil {
		t.Errorf("packet-size 65507: unexpected error %v", err)
	}
	if _, err := parseTraceArgs([]string{"--packet-size", "65508", "h"}, true, io.Discard); err == nil {
		t.Error("packet-size 65508: expected error")
	}
}

func TestParseTraceArgsMaxHopsBounds(t *testing.T) {
	if _, err := parseTraceArgs([]string{"--max-hops", "0", "h"}, true, io.Discard); err == nil {
		t.Error("max-hops 0: expected error")
	}
	if _, err := parseTraceArgs([]string{"--max-hops", "256", "h"}, true, io.Discard); err == nil {
		t.Error("max-hops 256: expected error")
	}
}

func TestParseTraceArgsReverseTrace(t *testing.T) {
	for _, v := range []string{"on", "off", "auto", "AUTO", "On"} {
		if _, err := parseTraceArgs([]string{"--reverse-trace", v, "h"}, true, io.Discard); err != nil {
			t.Errorf("--reverse-trace %s: unexpected error %v", v, err)
		}
	}
	if _, err := parseTraceArgs([]string{"--reverse-trace", "maybe", "h"}, true, io.Discard); err == nil {
		t.Error("--reverse-trace maybe: expected error")
	}
}

func TestParseTraceArgsDurationPositive(t *testing.T) {
	if _, err := parseTraceArgs([]string{"--duration", "0s", "h"}, true, io.Discard); err == nil {
		t.Error("duration 0s: expected error")
	}
	if _, err := parseTraceArgs([]string{"--mtr-interval", "0s", "h"}, true, io.Discard); err == nil {
		t.Error("mtr-interval 0s: expected error")
	}
}

func TestParseTraceArgsSavePath(t *testing.T) {
	cfg := parse(t, []string{"--save", "out.json", "1.2.3.4"}, true)
	if cfg.savePath != "out.json" {
		t.Errorf("savePath = %q, want out.json", cfg.savePath)
	}
}

// writeJSONFile must write atomically: a successful render leaves no
// .tmp sibling, and a render failure leaves neither the destination
// nor the .tmp behind.
func TestWriteJSONFileAtomicSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.json")
	d := report.Data{
		Version: 1,
		Target:  "1.2.3.4",
		Stream:  control.FinalStats{Recv: 10, DurationS: 1.0},
	}
	if err := writeJSONFile(path, d); err != nil {
		t.Fatalf("writeJSONFile: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected output file to exist: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("expected no .tmp leftover, stat err=%v", err)
	}
}

func TestWriteJSONFileAtomicFailureCleansUp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.json")
	// json.Encoder fails on NaN/Inf floats. Slip a NaN into a hop's
	// RTT field so RenderJSON returns an error partway through.
	d := report.Data{
		Version: 1,
		Target:  "1.2.3.4",
		Stream:  control.FinalStats{Recv: 10, DurationS: 1.0},
		Hops: []report.HopReport{
			{TTL: 1, IP: "10.0.0.1", BestRTTMS: math.NaN()},
		},
	}
	if err := writeJSONFile(path, d); err == nil {
		t.Fatalf("expected error from NaN payload")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("destination must not exist after failure, stat err=%v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf(".tmp must be cleaned up after failure, stat err=%v", err)
	}
}
