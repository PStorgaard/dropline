package tui

import (
	"regexp"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/PStorgaard/dropline/internal/agg"
	"github.com/PStorgaard/dropline/internal/control"
)

// ansiCSI strips ANSI CSI escape codes from rendered TUI output so
// assertions can run regardless of whether the test process inherits a
// color-capable terminal profile. lipgloss styles the test output with
// real ANSI sequences in some environments and bare characters in
// others; the visible content is what the assertions care about.
var ansiCSI = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripANSI(s string) string { return ansiCSI.ReplaceAllString(s, "") }

func TestSparkIndexThresholds(t *testing.T) {
	cases := []struct {
		pct  float64
		want int
	}{
		{0, 0},
		{-0.1, 0}, // negative defensively clamps to flat
		{0.05, 1},
		{0.3, 2},
		{0.9, 3},
		{1.5, 4},
		{4.99, 5},
		{9.99, 6},
		{10, 7},
		{50, 7},
	}
	for _, c := range cases {
		if got := sparkIndex(c.pct); got != c.want {
			t.Errorf("sparkIndex(%v) = %d, want %d", c.pct, got, c.want)
		}
	}
}

func TestSparkIndexMonotonic(t *testing.T) {
	// Strictly non-decreasing as pct grows. Catches bucket-boundary regressions.
	prev := -1
	for pct := 0.0; pct <= 50; pct += 0.05 {
		idx := sparkIndex(pct)
		if idx < prev {
			t.Fatalf("non-monotonic: pct=%.2f idx=%d after prev=%d", pct, idx, prev)
		}
		prev = idx
	}
}

func TestRenderSparklineEmpty(t *testing.T) {
	got := renderSparkline(nil, 80)
	// No buckets but plenty of width: should render the legend header
	// and a dotted placeholder on the bottom row.
	if !strings.HasPrefix(got, "  udp loss/s ") {
		t.Errorf("missing prefix; got %q", got)
	}
	if !strings.Contains(got, "·") {
		t.Errorf("expected dotted placeholder when no buckets; got %q", got)
	}
	// 3 chart rows under the header → 4 lines total.
	if lines := strings.Count(got, "\n"); lines != 3 {
		t.Errorf("expected 3 newlines (4 lines) in placeholder chart; got %d in %q", lines, got)
	}
}

func TestRenderSparklineNarrow(t *testing.T) {
	// Width below the prefix length should bail out cleanly with empty string.
	if got := renderSparkline([]agg.Bucket{{StreamLossPct: 5}}, 4); got != "" {
		t.Errorf("expected empty string at width=4, got %q", got)
	}
}

func TestRenderSparklineLengthCapped(t *testing.T) {
	// 100 buckets, width allows 30 chars on the bot row → only the last
	// 30 should appear. Each rune is wrapped in lossColor() ANSI; strip
	// before counting.
	buckets := make([]agg.Bucket, 100)
	for i := range buckets {
		buckets[i].StreamLossPct = float64(i % 10)
	}
	const indent = "             "
	const suffix = "  (last 60s)"
	width := len(indent) + 30 + len(suffix)
	got := stripANSI(renderSparkline(buckets, width))
	lines := strings.Split(got, "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines (legend + 3 chart rows), got %d in %q", len(lines), got)
	}
	bot := strings.TrimPrefix(lines[3], indent)
	bot = strings.TrimSuffix(bot, suffix)
	if n := len([]rune(bot)); n != 30 {
		t.Errorf("expected 30 spark runes on bottom row at width=%d, got %d (%q)", width, n, bot)
	}
}

func TestRenderSparklineAllZero(t *testing.T) {
	buckets := make([]agg.Bucket, 10)
	got := stripANSI(renderSparkline(buckets, 80))
	lines := strings.Split(got, "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines, got %d in %q", len(lines), got)
	}
	// Bottom row should be all ▁ across the 10 buckets.
	const indent = "             "
	bot := strings.TrimPrefix(lines[3], indent)
	bot = strings.TrimSuffix(bot, "  (last 60s)")
	bot = strings.TrimRight(bot, " ")
	if len([]rune(bot)) == 0 {
		t.Fatalf("expected bars on bottom row, got %q", got)
	}
	for _, r := range bot {
		if r != '▁' {
			t.Errorf("expected all flat (▁) for zero-loss buckets on bottom row, got %q", bot)
			break
		}
	}
	// Top and mid rows should be spaces for zero-loss buckets.
	for _, label := range []struct {
		row  string
		line string
	}{{"top", lines[1]}, {"mid", lines[2]}} {
		body := strings.TrimPrefix(label.line, indent)
		body = strings.TrimRight(body, " ")
		if body != "" {
			t.Errorf("%s row should be empty for all-zero buckets; got %q", label.row, body)
		}
	}
}

// Sparkline runes are color-tiered: a high-loss bucket carries a
// different ANSI style than a zero-loss bucket in the same chart. We
// verify the rendered output contains lossColor-styled output for each
// tier somewhere in the multi-row chart.
func TestRenderSparklineColorsByTier(t *testing.T) {
	buckets := []agg.Bucket{
		{StreamLossPct: 0},  // ok tier (≤1)
		{StreamLossPct: 2},  // amber tier (>1, ≤5)
		{StreamLossPct: 20}, // red tier (>5)
	}
	got := renderSparkline(buckets, 80)
	// Each tier's bottom-row glyph from sparkStack: L0=▁, L4=█, L7=█.
	// Spot-check that the rendered output contains each tier's color
	// styling on at least one rune.
	for _, want := range []string{
		okStyle.Render("▁"),
		amberStyle.Render("█"),
		redStyle.Render("█"),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("sparkline missing tiered styling %q\nin %q", want, got)
		}
	}
}

func TestLossColorTiers(t *testing.T) {
	// Style values aren't directly comparable, but the foreground TerminalColor
	// is. Lipgloss strips colors when the terminal profile is "no color" (e.g.
	// inside `go test`), so don't lean on rendered ANSI here.
	a := lossColor(0.5).GetForeground()
	b := lossColor(2.0).GetForeground()
	c := lossColor(10.0).GetForeground()
	if a == b || b == c || a == c {
		t.Errorf("expected three distinct tiers, got ok=%v amber=%v red=%v", a, b, c)
	}
}

// The reverse hop table reuses rttBaselineGlyphFor; a high LastRTT
// vs low baseline triggers ▲▲ in the inter-column slot.
func TestRenderReverseHopTableShowsBaselineGlyph(t *testing.T) {
	hops := []agg.ReverseHopView{
		{
			TTL: 1, Addr: "10.0.0.1", Sent: 4, Recv: 4, RTTMS: 14,
			BaselineRTTMS: 10, StdDevRTTMS: 1, AvgRTTMS: 11,
		},
	}
	got := stripANSI(renderReverseHopTable(hops, "ok", 100))
	if !strings.Contains(got, "▲▲") {
		t.Errorf("expected ▲▲ glyph for delta=4·stddev; got\n%s", got)
	}
}

// Slim-shape reverse hops (Baseline=0 / StdDev=0 — old server) skip
// the glyph entirely.
func TestRenderReverseHopTableSlimShapeNoGlyph(t *testing.T) {
	hops := []agg.ReverseHopView{
		{TTL: 1, Addr: "10.0.0.1", RTTMS: 14},
	}
	got := stripANSI(renderReverseHopTable(hops, "ok", 100))
	if strings.Contains(got, "▲") {
		t.Errorf("slim-shape hop should not render any ▲ glyph; got\n%s", got)
	}
}

// Width ladder: ≥80 includes Best/Worst, mid-width drops them but keeps
// Addr, narrow drops Addr too.
func TestRenderReverseHopTableWidthLadder(t *testing.T) {
	hops := []agg.ReverseHopView{
		{TTL: 1, Addr: "10.0.0.1", Sent: 4, Recv: 4, RTTMS: 0.4, AvgRTTMS: 0.4, BestRTTMS: 0.3, WorstRTTMS: 0.6},
	}
	wide := renderReverseHopTable(hops, "ok", 100)
	if !strings.Contains(wide, "Worst") {
		t.Errorf("wide reverse table should include Worst column; got\n%s", wide)
	}
	mid := renderReverseHopTable(hops, "ok", 60)
	if strings.Contains(mid, "Worst") {
		t.Errorf("mid reverse table should drop Worst column; got\n%s", mid)
	}
	if !strings.Contains(mid, "Addr") {
		t.Errorf("mid reverse table should keep Addr column; got\n%s", mid)
	}
	narrow := renderReverseHopTable(hops, "ok", 40)
	if strings.Contains(narrow, "Addr") {
		t.Errorf("narrow reverse table should drop Addr column; got\n%s", narrow)
	}
}

// reverseStatusStyle splits the four wire status strings (plus the
// empty fallback) into two visual tiers. ok / disabled_by_client share
// the steady-state dim style; disabled_by_server / terminated_at_nat /
// empty share the amber attention style. We assert via foreground
// identity (the same trick TestLossColorTiers uses) so the test is
// resilient to whether ANSI escapes are emitted under the current
// terminal profile.
func TestReverseStatusStyle(t *testing.T) {
	dimFG := dimStyle.GetForeground()
	amberFG := amberStyle.GetForeground()

	cases := []struct {
		status string
		want   any
	}{
		{"ok", dimFG},
		{"disabled_by_client", dimFG},
		{"disabled_by_server", amberFG},
		{"terminated_at_nat", amberFG},
		{"", amberFG},
		{"future_unknown", amberFG},
	}
	for _, c := range cases {
		t.Run(c.status, func(t *testing.T) {
			if got := reverseStatusStyle(c.status).GetForeground(); got != c.want {
				t.Errorf("reverseStatusStyle(%q) fg = %v, want %v", c.status, got, c.want)
			}
		})
	}
}

func TestRenderReverseHopTableStatusVariants(t *testing.T) {
	hops := []agg.ReverseHopView{
		{TTL: 1, Addr: "10.0.0.1", RTTMS: 0.4},
		{TTL: 2, Addr: "203.0.113.4", RTTMS: 12.0, Terminus: "nat"},
	}
	got := renderReverseHopTable(hops, "terminated_at_nat", 80)
	if !strings.Contains(got, "terminated at NAT") {
		t.Errorf("expected friendly NAT phrasing in header; got\n%s", got)
	}
	if !strings.Contains(got, "2 hops") {
		t.Errorf("expected hop count in header; got\n%s", got)
	}

	got = renderReverseHopTable(nil, "disabled_by_client", 80)
	if !strings.Contains(got, "disabled by client") {
		t.Errorf("expected friendly disabled-by-client phrasing; got\n%s", got)
	}
	if !strings.Contains(got, "0 hops") {
		t.Errorf("expected zero-hop count in header; got\n%s", got)
	}

	got = renderReverseHopTable(nil, "", 80)
	if !strings.Contains(got, "disabled by server") {
		t.Errorf("empty status should fall back to disabled by server; got\n%s", got)
	}

	// Style tier check: disabled_by_server / terminated_at_nat carry an
	// amber annotation; ok / disabled_by_client stay dim. Compare the
	// rendered output's escape-sequence content rather than visible
	// text — the style differs even when the status word is the same
	// length.
	server := renderReverseHopTable(nil, "disabled_by_server", 80)
	client := renderReverseHopTable(nil, "disabled_by_client", 80)
	okHdr := renderReverseHopTable([]agg.ReverseHopView{{TTL: 1, Addr: "10.0.0.1"}}, "ok", 80)
	nat := renderReverseHopTable([]agg.ReverseHopView{{TTL: 1, Addr: "10.0.0.1"}}, "terminated_at_nat", 80)
	if server == client {
		t.Errorf("disabled_by_server and disabled_by_client should render in different styles; both gave\n%s", server)
	}
	if okHdr == nat {
		t.Errorf("ok and terminated_at_nat should render in different styles; both gave\n%s", okHdr)
	}
}

func TestRenderReverseHopTableLeadingFilteredHint(t *testing.T) {
	// Cloud egress shape: TTLs 1–3 silent, then responsive hops. Hint
	// should call out the silent run so the operator doesn't read the
	// table as a broken trace.
	hops := []agg.ReverseHopView{
		{TTL: 1, Addr: ""},
		{TTL: 2, Addr: ""},
		{TTL: 3, Addr: ""},
		{TTL: 4, Addr: "13.106.210.176", RTTMS: 20},
		{TTL: 5, Addr: "104.44.231.0", RTTMS: 41},
	}
	got := stripANSI(renderReverseHopTable(hops, "ok", 100))
	if !strings.Contains(got, "TTLs 1–3 silent") {
		t.Errorf("expected leading-filtered hint; got\n%s", got)
	}
	if !strings.Contains(got, "first responding hop is TTL 4") {
		t.Errorf("expected hint to point at TTL 4; got\n%s", got)
	}

	// Single silent leading hop uses singular phrasing.
	hops = []agg.ReverseHopView{
		{TTL: 1, Addr: ""},
		{TTL: 2, Addr: "10.0.0.1"},
	}
	got = stripANSI(renderReverseHopTable(hops, "ok", 100))
	if !strings.Contains(got, "TTL 1 silent") {
		t.Errorf("expected singular hint; got\n%s", got)
	}

	// No leading silent run → no hint.
	hops = []agg.ReverseHopView{
		{TTL: 1, Addr: "10.0.0.1"},
		{TTL: 2, Addr: ""},
	}
	got = stripANSI(renderReverseHopTable(hops, "ok", 100))
	if strings.Contains(got, "filtered upstream") {
		t.Errorf("hint should not render when first hop responded; got\n%s", got)
	}

	// All silent → no hint (nothing to contrast against; the empty trace
	// speaks for itself).
	hops = []agg.ReverseHopView{
		{TTL: 1, Addr: ""},
		{TTL: 2, Addr: ""},
	}
	got = stripANSI(renderReverseHopTable(hops, "ok", 100))
	if strings.Contains(got, "filtered upstream") {
		t.Errorf("hint should not render when no later hop responds; got\n%s", got)
	}
}

func TestFriendlyReverseStatus(t *testing.T) {
	cases := map[string]string{
		"":                    "",
		"ok":                  "ok",
		"terminated_at_nat":   "terminated at NAT",
		"disabled_by_server":  "disabled by server",
		"disabled_by_client":  "disabled by client",
		"unknown_future_form": "unknown_future_form",
	}
	for in, want := range cases {
		if got := control.FriendlyReverseStatus(in); got != want {
			t.Errorf("FriendlyReverseStatus(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRenderHopTableEmpty(t *testing.T) {
	got := renderHopTable(nil, 80)
	if !strings.Contains(got, "no hops resolved") {
		t.Errorf("expected placeholder for empty hops; got %q", got)
	}
}

// renderHopTable adds a red "! " gutter on rows where Suspect is set.
// We can't assert on ANSI escape codes (lipgloss strips them under no-color
// test profiles), so we assert the literal "! " marker appears in the
// rendered row.
func TestRenderHopTableSuspectGutter(t *testing.T) {
	hops := []agg.HopView{
		{TTL: 1, IP: "10.0.0.1"},
		{TTL: 2, IP: "10.0.0.2", Suspect: true},
	}
	got := stripANSI(renderHopTable(hops, 100))
	if !strings.Contains(got, "!2") {
		t.Errorf("expected '!' suspect prefix on TTL 2 row; got\n%s", got)
	}
	if strings.Contains(got, "!1") {
		t.Errorf("non-suspect TTL 1 row should not carry the '!' prefix; got\n%s", got)
	}
}

// Silent (no-IP) hops never carry the '!' suspect prefix even if
// Suspect=true somehow leaks in — they have no actionable evidence on
// either signal. Defensive guard against stale Suspect values from older
// binaries or future code paths.
func TestRenderHopTableSilentHopSuppressesSuspectGutter(t *testing.T) {
	hops := []agg.HopView{
		{TTL: 1, IP: "", Suspect: true, Sent: 10},
	}
	got := stripANSI(renderHopTable(hops, 100))
	if strings.Contains(got, "!1") {
		t.Errorf("silent hop should not render the '!' prefix; got\n%s", got)
	}
}

// A high-loss hop whose downstream responsive hop is clean is ICMP
// rate-limited, not a real loss source. Its loss cell must render in
// the dim style rather than red/amber so the eye doesn't read it as a
// fault. We assert via foreground identity (the same trick
// TestLossColorTiers uses) so the test is resilient to color-strip test
// profiles.
func TestRenderHopTableDimsRateLimitedLossCell(t *testing.T) {
	hops := []agg.HopView{
		{TTL: 1, IP: "10.0.0.1", Sent: 100, Recv: 60, LossPct: 40, LastRTTMS: 10, AvgRTTMS: 10, BestRTTMS: 9, WorstRTTMS: 12},
		{TTL: 2, IP: "10.0.0.2", Sent: 100, Recv: 99, LossPct: 1, LastRTTMS: 12, AvgRTTMS: 12, BestRTTMS: 11, WorstRTTMS: 13},
	}
	got := renderHopTable(hops, 100)

	// Hop 1's 40% loss would normally land in redStyle. With rate-limit
	// suppression it should be dimStyle. Compare via lipgloss render of
	// the substring "40.00%" in each candidate style.
	if want := dimStyle.Render("40.00%"); !strings.Contains(got, want) {
		t.Errorf("expected rate-limited hop loss cell to render in dim style; got\n%s", got)
	}
	if unwant := redStyle.Render("40.00%"); strings.Contains(got, unwant) {
		t.Errorf("rate-limited hop loss cell should not render in red style; got\n%s", got)
	}

	// Sanity: when the downstream hop is also lossy, suppression should
	// not kick in and the original red coloring stays.
	hops[1].LossPct = 39
	got = renderHopTable(hops, 100)
	if want := redStyle.Render("40.00%"); !strings.Contains(got, want) {
		t.Errorf("expected non-rate-limited hop loss cell in red; got\n%s", got)
	}
}

// rttBaselineGlyphFor tiers map (last - baseline) / stddev to
// "" / "▲" / "▲▲". Pure unit test against the helper so we don't have
// to reason about column placement.
func TestRTTBaselineGlyphTiers(t *testing.T) {
	cases := []struct {
		name                  string
		last, baseline, stddev float64
		want                  string
	}{
		{"no_baseline", 50, 0, 1, ""},
		{"no_stddev", 50, 10, 0, ""},
		{"in_band", 11, 10, 1, ""},
		{"amber_above_1_5", 12, 10, 1, "▲"},
		{"red_above_3", 14, 10, 1, "▲▲"},
		{"below_baseline", 5, 10, 1, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := stripANSI(rttBaselineGlyphFor(c.last, c.baseline, c.stddev))
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

// renderHopTable threads the glyph into the row layout. Spot-check that a
// glyphed row contains the marker.
func TestRenderHopTableShowsGlyph(t *testing.T) {
	hops := []agg.HopView{
		{TTL: 1, IP: "10.0.0.1", LastRTTMS: 14, BaselineRTTMS: 10, StdDevRTTMS: 1},
	}
	got := stripANSI(renderHopTable(hops, 100))
	if !strings.Contains(got, "▲▲") {
		t.Errorf("expected ▲▲ glyph for delta=4·stddev; got\n%s", got)
	}
}

// Footer composes from the active SaveFn / ResetFn callbacks and the
// done state. We exercise the helper directly to avoid coupling to
// bubbletea's View output.
func TestModelFooterTextLiveAndDone(t *testing.T) {
	noop := func() {}
	noopSave := func() (string, error) { return "x", nil }
	noopCopy := func() (int, error) { return 0, nil }

	live := model{opts: Options{ResetFn: noop, SaveFn: noopSave}}
	got := live.footerText()
	for _, want := range []string{"[s] save", "[r] reset", "[q] quit"} {
		if !strings.Contains(got, want) {
			t.Errorf("live footer missing %q; got %q", want, got)
		}
	}

	done := model{opts: Options{SaveFn: noopSave}, done: true}
	got = done.footerText()
	if !strings.Contains(got, "test complete") || !strings.Contains(got, "[s] save") || !strings.Contains(got, "[q] exit") {
		t.Errorf("done footer with SaveFn missing parts; got %q", got)
	}

	doneNoSave := model{done: true}
	got = doneNoSave.footerText()
	if strings.Contains(got, "[s] save") {
		t.Errorf("done footer without SaveFn should not advertise save; got %q", got)
	}

	withStatus := model{status: "wrote: foo.json"}
	if got := withStatus.footerText(); got != "wrote: foo.json" {
		t.Errorf("active status should override hint; got %q", got)
	}

	// CopyFn surfaces in both live and done footers when set.
	liveWithCopy := model{opts: Options{ResetFn: noop, SaveFn: noopSave, CopyFn: noopCopy}}
	if got := liveWithCopy.footerText(); !strings.Contains(got, "[c] copy") {
		t.Errorf("live footer with CopyFn should include [c] copy; got %q", got)
	}
	doneWithCopy := model{opts: Options{SaveFn: noopSave, CopyFn: noopCopy}, done: true}
	if got := doneWithCopy.footerText(); !strings.Contains(got, "[c] copy") {
		t.Errorf("done footer with CopyFn should include [c] copy; got %q", got)
	}
	// And not advertised when CopyFn is nil.
	if got := done.footerText(); strings.Contains(got, "[c] copy") {
		t.Errorf("done footer without CopyFn should not advertise copy; got %q", got)
	}

	// PauseFn surfaces in the live footer; paused state flips the
	// label to "[p] resume" and prepends "[PAUSED]".
	noopPause := func() (bool, error) { return false, nil }
	liveWithPause := model{opts: Options{ResetFn: noop, SaveFn: noopSave, PauseFn: noopPause}}
	if got := liveWithPause.footerText(); !strings.Contains(got, "[p] pause") {
		t.Errorf("live footer with PauseFn should include [p] pause; got %q", got)
	}
	pausedModel := model{opts: Options{ResetFn: noop, SaveFn: noopSave, PauseFn: noopPause}, paused: true}
	got = pausedModel.footerText()
	if !strings.Contains(got, "[PAUSED]") {
		t.Errorf("paused footer should be prefixed with [PAUSED]; got %q", got)
	}
	if !strings.Contains(got, "[p] resume") {
		t.Errorf("paused footer should advertise [p] resume; got %q", got)
	}
	if strings.Contains(got, "[p] pause ") || strings.HasSuffix(got, "[p] pause") {
		t.Errorf("paused footer should not advertise [p] pause; got %q", got)
	}
	// PauseFn omitted from the done view (pause is mid-test only).
	if got := done.footerText(); strings.Contains(got, "[p]") {
		t.Errorf("done footer should not advertise [p] anything; got %q", got)
	}
}

// renderKPI surfaces the verdict text inferred from KernelDrops. Clean
// network = green ✓ message; suspect = amber ⚠ message.
func TestRenderKPIVerdict(t *testing.T) {
	opts := Options{Target: "srv.lab", RateBPS: 10_000_000, Duration: time.Minute}
	// Width 120: the verdict-substring assertion would otherwise be
	// fragile to lipgloss wrapping the metrics line at narrow widths.
	clean := stripANSI(renderKPI(opts, agg.StreamView{LossPct: 0.01, KernelDrops: 0}, 5*time.Second, false, "[bar]", 120))
	if !strings.Contains(clean, "✓ network loss is real") {
		t.Errorf("expected clean verdict; got\n%s", clean)
	}
	suspect := stripANSI(renderKPI(opts, agg.StreamView{LossPct: 0.5, KernelDrops: 12}, 5*time.Second, false, "[bar]", 120))
	if !strings.Contains(suspect, "⚠ network loss is suspect") {
		t.Errorf("expected suspect verdict; got\n%s", suspect)
	}
	// LocalDrops alone (KernelDrops==0) must also flip the verdict, since a
	// non-zero count means receiver overload inflated the loss reading.
	localSuspect := stripANSI(renderKPI(opts, agg.StreamView{LossPct: 0.5, LocalDrops: 7}, 5*time.Second, false, "[bar]", 120))
	if !strings.Contains(localSuspect, "⚠ network loss is suspect") {
		t.Errorf("expected suspect verdict from LocalDrops alone; got\n%s", localSuspect)
	}
	if !strings.Contains(localSuspect, "local drops: 7") {
		t.Errorf("expected 'local drops: 7' in KPI body; got\n%s", localSuspect)
	}
}

// renderKPI tags the elapsed line with [PAUSED] when paused=true so the
// section reads "paused" at a glance without the user having to look at
// the footer.
func TestRenderKPIPausedPrefix(t *testing.T) {
	opts := Options{Target: "srv.lab", RateBPS: 10_000_000, Duration: time.Minute}
	got := stripANSI(renderKPI(opts, agg.StreamView{}, 5*time.Second, true, "[bar]", 80))
	if !strings.Contains(got, "[PAUSED]") {
		t.Errorf("expected [PAUSED] in KPI when paused; got\n%s", got)
	}
	got = stripANSI(renderKPI(opts, agg.StreamView{}, 5*time.Second, false, "[bar]", 80))
	if strings.Contains(got, "[PAUSED]") {
		t.Errorf("did not expect [PAUSED] when paused=false; got\n%s", got)
	}
}

// renderKPI carries the target, rate, and burst histogram in its body
// so the operator has every stream counter in one place.
func TestRenderKPIIncludesTitleAndBursts(t *testing.T) {
	opts := Options{Target: "srv.lab", RateBPS: 10_000_000, Duration: time.Minute}
	view := agg.StreamView{Recv: 4200, Lost: 1}
	view.Bursts.One = 3
	view.Bursts.Max = 1
	got := stripANSI(renderKPI(opts, view, 5*time.Second, false, "[bar]", 100))
	for _, want := range []string{"dropline · srv.lab", "10.00 Mbps", "bursts:", "recv 4200"} {
		if !strings.Contains(got, want) {
			t.Errorf("KPI missing %q; got\n%s", want, got)
		}
	}
}

// `?` toggles the help overlay in both directions; `esc` closes it
// without quitting. Other keys are inert while the overlay is visible
// so the user can't accidentally trigger save/copy/reset from inside it.
func TestHelpOverlayToggle(t *testing.T) {
	saved := 0
	m := newModel(Options{SaveFn: func() (string, error) { saved++; return "ok", nil }}, nil)
	m.done = true // make [s] eligible so the inert-check has real teeth

	press := func(s string) tea.KeyPressMsg { return tea.KeyPressMsg{Code: rune(s[0]), Text: s} }

	updated, _ := m.Update(press("?"))
	m = updated.(model)
	if !m.helpVisible {
		t.Fatalf("expected helpVisible=true after first ?")
	}

	updated, _ = m.Update(press("s"))
	m = updated.(model)
	if saved != 0 {
		t.Errorf("save should be inert while help overlay is visible (saved=%d)", saved)
	}

	updated, _ = m.Update(press("?"))
	m = updated.(model)
	if m.helpVisible {
		t.Errorf("expected helpVisible=false after second ?")
	}

	updated, _ = m.Update(press("?"))
	m = updated.(model)
	if !m.helpVisible {
		t.Fatalf("expected helpVisible=true again")
	}
	updated, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEscape, Text: "esc"})
	m = updated.(model)
	if m.helpVisible {
		t.Errorf("esc should close the help overlay")
	}
}

// Footer footers gain a [?] help entry now that the overlay exists.
func TestFooterIncludesHelpHint(t *testing.T) {
	live := model{opts: Options{}}
	if got := live.footerText(); !strings.Contains(got, "[?] help") {
		t.Errorf("live footer missing [?] help; got %q", got)
	}
	done := model{opts: Options{}, done: true}
	if got := done.footerText(); !strings.Contains(got, "[?] help") {
		t.Errorf("done footer missing [?] help; got %q", got)
	}
}

// collapseSilentRuns folds runs of ≥2 contiguous silent hops; singletons
// (silent or not) pass through unchanged.
func TestCollapseSilentRuns(t *testing.T) {
	cases := []struct {
		name   string
		silent []bool
		want   []hopItem
	}{
		{
			name:   "no_silent",
			silent: []bool{false, false, false},
			want: []hopItem{
				{runStart: -1, runEnd: -1, single: 0},
				{runStart: -1, runEnd: -1, single: 1},
				{runStart: -1, runEnd: -1, single: 2},
			},
		},
		{
			name:   "single_silent_passthrough",
			silent: []bool{false, true, false},
			want: []hopItem{
				{runStart: -1, runEnd: -1, single: 0},
				{runStart: -1, runEnd: -1, single: 1},
				{runStart: -1, runEnd: -1, single: 2},
			},
		},
		{
			name:   "trailing_run",
			silent: []bool{false, false, true, true, true},
			want: []hopItem{
				{runStart: -1, runEnd: -1, single: 0},
				{runStart: -1, runEnd: -1, single: 1},
				{runStart: 2, runEnd: 4, single: -1},
			},
		},
		{
			name:   "leading_run_then_responsive",
			silent: []bool{true, true, true, false},
			want: []hopItem{
				{runStart: 0, runEnd: 2, single: -1},
				{runStart: -1, runEnd: -1, single: 3},
			},
		},
		{
			name:   "all_silent",
			silent: []bool{true, true, true, true},
			want: []hopItem{
				{runStart: 0, runEnd: 3, single: -1},
			},
		},
		{
			name:   "two_silent_run_at_min_threshold",
			silent: []bool{false, true, true, false},
			want: []hopItem{
				{runStart: -1, runEnd: -1, single: 0},
				{runStart: 1, runEnd: 2, single: -1},
				{runStart: -1, runEnd: -1, single: 3},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := collapseSilentRuns(len(c.silent), func(i int) bool { return c.silent[i] })
			if len(got) != len(c.want) {
				t.Fatalf("len = %d, want %d (got=%v)", len(got), len(c.want), got)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("item[%d] = %+v, want %+v", i, got[i], c.want[i])
				}
			}
		})
	}
}

// Forward hop table collapses a trailing run of silent hops into a
// single summary row, leaves responsive hops intact, and never folds a
// single isolated silent hop.
func TestRenderHopTableCollapsesSilentRun(t *testing.T) {
	hops := []agg.HopView{
		{TTL: 1, IP: "10.0.0.1", Sent: 8, Recv: 8, LastRTTMS: 1, AvgRTTMS: 1, BestRTTMS: 1, WorstRTTMS: 1},
		{TTL: 2, IP: "10.0.0.2", Sent: 8, Recv: 8, LastRTTMS: 2, AvgRTTMS: 2, BestRTTMS: 2, WorstRTTMS: 2},
		{TTL: 3, IP: "", Sent: 8},
		{TTL: 4, IP: "", Sent: 8},
		{TTL: 5, IP: "", Sent: 8},
		{TTL: 6, IP: "10.0.0.6", Sent: 8, Recv: 8, LastRTTMS: 6, AvgRTTMS: 6, BestRTTMS: 6, WorstRTTMS: 6},
	}
	got := stripANSI(renderHopTable(hops, 120))
	if !strings.Contains(got, "3–5") {
		t.Errorf("expected TTL range 3–5 in collapsed row; got\n%s", got)
	}
	if !strings.Contains(got, "⋯ 3 silent hops") {
		t.Errorf("expected '⋯ 3 silent hops' label; got\n%s", got)
	}
	// Endpoints must still render normally.
	if !strings.Contains(got, "10.0.0.1") || !strings.Contains(got, "10.0.0.6") {
		t.Errorf("responsive hops missing; got\n%s", got)
	}
	// No standalone "3" / "4" / "5" TTL row remains. Each line in the
	// table body that's not the collapsed row should not be a silent
	// row for those TTLs.
	for _, ttl := range []string{" 3 ", " 4 ", " 5 "} {
		// Allow occurrences as part of the range "3–5"; check standalone.
		if strings.Contains(got, ttl+" *") {
			t.Errorf("expected silent TTL row %q to be collapsed; got\n%s", ttl, got)
		}
	}
}

// A single silent hop sandwiched between responsive hops is NOT
// collapsed — the row stays in place.
func TestRenderHopTableDoesNotCollapseSingleSilent(t *testing.T) {
	hops := []agg.HopView{
		{TTL: 1, IP: "10.0.0.1", Sent: 8, Recv: 8},
		{TTL: 2, IP: "", Sent: 8},
		{TTL: 3, IP: "10.0.0.3", Sent: 8, Recv: 8},
	}
	got := stripANSI(renderHopTable(hops, 120))
	if strings.Contains(got, "silent hops") {
		t.Errorf("single silent hop should not produce summary row; got\n%s", got)
	}
	// The bare "*" silent row should still be there.
	if !strings.Contains(got, "*") {
		t.Errorf("expected '*' for the single silent hop; got\n%s", got)
	}
}

// Reverse path leading silent run collapses to a single summary row,
// AND the existing leadingFilteredHint still renders above the table
// (it draws from the original hops slice).
func TestRenderReverseHopTableCollapsesLeadingRun(t *testing.T) {
	hops := []agg.ReverseHopView{
		{TTL: 1, Addr: "", Sent: 17},
		{TTL: 2, Addr: "", Sent: 17},
		{TTL: 3, Addr: "", Sent: 17},
		{TTL: 4, Addr: "13.106.210.176", Sent: 17, Recv: 17, RTTMS: 20, AvgRTTMS: 20, BestRTTMS: 19, WorstRTTMS: 22},
	}
	got := stripANSI(renderReverseHopTable(hops, "ok", 120))
	if !strings.Contains(got, "TTLs 1–3 silent") {
		t.Errorf("expected leading-filtered hint to remain; got\n%s", got)
	}
	if !strings.Contains(got, "1–3") {
		t.Errorf("expected TTL range 1–3 in collapsed row; got\n%s", got)
	}
	if !strings.Contains(got, "⋯ 3 silent hops") {
		t.Errorf("expected '⋯ 3 silent hops' label; got\n%s", got)
	}
	if !strings.Contains(got, "13.106.210.176") {
		t.Errorf("responsive reverse hop missing; got\n%s", got)
	}
}

// All-silent collapses into a single summary row.
func TestRenderHopTableAllSilentCollapses(t *testing.T) {
	hops := []agg.HopView{
		{TTL: 1, IP: "", Sent: 5},
		{TTL: 2, IP: "", Sent: 5},
		{TTL: 3, IP: "", Sent: 5},
	}
	got := stripANSI(renderHopTable(hops, 120))
	if !strings.Contains(got, "1–3") {
		t.Errorf("expected TTL range 1–3; got\n%s", got)
	}
	if !strings.Contains(got, "⋯ 3 silent hops") {
		t.Errorf("expected '⋯ 3 silent hops' label; got\n%s", got)
	}
	// Should be exactly one row in the table body (plus header line).
	// Count lines containing the silent label.
	if n := strings.Count(got, "silent hops"); n != 1 {
		t.Errorf("expected exactly one collapsed row; got %d in\n%s", n, got)
	}
}

func TestRenderHopTableColumnsByWidth(t *testing.T) {
	hops := []agg.HopView{
		{TTL: 1, IP: "10.0.0.1", Sent: 10, Recv: 10, LastRTTMS: 0.4, AvgRTTMS: 0.4, BestRTTMS: 0.3, WorstRTTMS: 0.6},
	}
	wide := renderHopTable(hops, 100)
	if !strings.Contains(wide, "Worst") {
		t.Errorf("wide table should include Worst column; got\n%s", wide)
	}
	mid := renderHopTable(hops, 60)
	if strings.Contains(mid, "Worst") {
		t.Errorf("mid table should drop Worst column; got\n%s", mid)
	}
	if !strings.Contains(mid, "IP") {
		t.Errorf("mid table should keep IP column; got\n%s", mid)
	}
	narrow := renderHopTable(hops, 40)
	if strings.Contains(narrow, "IP") {
		t.Errorf("narrow table should drop IP header; got\n%s", narrow)
	}
}
