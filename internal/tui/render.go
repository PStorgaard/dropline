package tui

import (
	"fmt"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"

	"github.com/PStorgaard/dropline/internal/agg"
	"github.com/PStorgaard/dropline/internal/control"
)

// sparkRunes maps the eight intensity buckets used by renderSparkline. Index
// 0 is "no loss"; index 7 is "≥10% loss in this second".
var sparkRunes = []rune("▁▂▃▄▅▆▇█")

// sparkStack maps each sparkIndex tier to the three runes drawn in the
// stacked bar chart, top to bottom. Bottom row carries at least ▁ for
// zero-loss buckets so a quiet network still shows a flat baseline.
var sparkStack = [8][3]rune{
	{' ', ' ', '▁'}, // L0: zero
	{' ', ' ', '▂'}, // L1: <0.1%
	{' ', ' ', '▄'}, // L2: <0.5%
	{' ', ' ', '▆'}, // L3: <1%
	{' ', '▂', '█'}, // L4: <2.5%
	{' ', '▅', '█'}, // L5: <5%
	{'▂', '█', '█'}, // L6: <10%
	{'▆', '█', '█'}, // L7: ≥10%
}

var (
	okStyle          = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	amberStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
	redStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	dimStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	headerStyle      = lipgloss.NewStyle().Bold(true)
	barFilledStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("12"))
	tableCellStyle   = lipgloss.NewStyle().PaddingRight(2)
	tableHeaderStyle = tableCellStyle.Inherit(headerStyle)
)

// lossColor maps a percentage to one of three lipgloss styles. Spec § TUI:
// >1% amber, >5% red, otherwise green.
func lossColor(pct float64) lipgloss.Style {
	switch {
	case pct > 5:
		return redStyle
	case pct > 1:
		return amberStyle
	default:
		return okStyle
	}
}

// renderRule wraps body with a single dim horizontal rule that carries
// the section title (`── title ──…─`). No bottom border, no left/right
// chrome — visual grouping comes from the rule + a 2-space body indent
// the caller is responsible for applying. titleStyle styles the title
// text only; the rule itself stays dim.
func renderRule(title string, titleStyle lipgloss.Style, body string, width int) string {
	if width < 10 {
		return body
	}
	dash := dimStyle.Render("─")
	prefix := dash + dash + " "
	suffix := " "
	styledTitle := titleStyle.Render(title)
	titleWidth := lipgloss.Width(prefix) + lipgloss.Width(styledTitle) + lipgloss.Width(suffix)
	fill := width - titleWidth
	if fill < 0 {
		fill = 0
	}
	rule := prefix + styledTitle + suffix + dimStyle.Render(strings.Repeat("─", fill))
	return rule + "\n" + body
}

// indentBlock prepends n spaces to each non-empty line of s. ANSI
// escape sequences never contain '\n' so splitting on newline is safe
// even when cells carry color codes.
func indentBlock(s string, n int) string {
	if n <= 0 || s == "" {
		return s
	}
	indent := strings.Repeat(" ", n)
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		lines[i] = indent + line
	}
	return strings.Join(lines, "\n")
}

// renderBar produces a thin progress bar of the given visible width,
// filled to pct ∈ [0,1]. Replaces bubbles/v2/progress so we get a
// single-row "━" glyph instead of the half-block "▌" the lib renders.
// The empty remainder is a dim "─" so the bar's track stays visible at
// zero progress.
func renderBar(pct float64, width int) string {
	if width < 1 {
		return ""
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 1 {
		pct = 1
	}
	fill := int(pct * float64(width))
	if fill > width {
		fill = width
	}
	return barFilledStyle.Render(strings.Repeat("━", fill)) +
		dimStyle.Render(strings.Repeat("─", width-fill))
}

// renderKPI is the top dashboard card: target/rate title, an
// elapsed-vs-duration progress bar, the loss/jitter line, the kernel/
// local drops + bursts strip, and a right-aligned verdict on its own
// line so it doesn't compete with the loss% for visual weight.
func renderKPI(opts Options, view agg.StreamView, elapsed time.Duration, paused bool, bar string, width int) string {
	var sb strings.Builder
	titleLeft := fmt.Sprintf("dropline · %s · %s", opts.Target, agg.FormatRate(opts.RateBPS))

	// line 1: elapsed [bar] duration (with [PAUSED] prefix when applicable)
	pausedTag := ""
	if paused {
		pausedTag = redStyle.Render("[PAUSED]") + "  "
	}
	fmt.Fprintf(&sb, "  %selapsed %s  %s  duration %s\n",
		pausedTag, formatDuration(elapsed), bar, formatDuration(opts.Duration))

	// line 2: udp loss / jitter / counters
	lossCell := lossColor(view.LossPct).Render(fmt.Sprintf("%.3f%%", view.LossPct))
	fmt.Fprintf(&sb, "  udp loss  %s   jitter %.2f ms   recv %d   lost %d\n",
		lossCell, view.JitterMS, view.Recv, view.Lost)

	// line 3: kernel/local drops + burst summary
	b := view.Bursts
	fmt.Fprintf(&sb, "  kernel drops %d   local drops %d   bursts %s",
		view.KernelDrops, view.LocalDrops,
		agg.FormatBurstSummary(b.Ten99, b.HundredUp, b.Max))

	// line 4: right-aligned verdict (its own line so it doesn't crowd
	// the metrics row). Width-fallback: if the verdict alone wouldn't
	// fit with at least 4 columns of padding ahead of it, emit it on a
	// fresh left-indented line instead.
	verdict := okStyle.Render("✓ network loss is real")
	if view.KernelDrops > 0 || view.LocalDrops > 0 {
		verdict = amberStyle.Render("⚠ network loss is suspect")
	}
	verdictW := lipgloss.Width(verdict)
	sb.WriteByte('\n')
	if width >= verdictW+4 {
		sb.WriteString(lipgloss.PlaceHorizontal(width, lipgloss.Right, verdict))
	} else {
		sb.WriteString("  ")
		sb.WriteString(verdict)
	}

	return renderRule(titleLeft, headerStyle, sb.String(), width)
}

// renderReverseKPI is the secondary KPI card surfaced only when the
// session is running a reverse-direction UDP stream (server→client).
// It's a single line in its own section frame so the existing forward
// KPI block stays untouched and the reverse stats are unmistakable.
// Returns "" when view is nil so callers can append unconditionally.
func renderReverseKPI(view *agg.StreamView, width int) string {
	if view == nil {
		return ""
	}
	lossCell := lossColor(view.LossPct).Render(fmt.Sprintf("%.3f%%", view.LossPct))
	body := fmt.Sprintf("  reverse loss  %s   jitter %.2f ms   recv %d   lost %d",
		lossCell, view.JitterMS, view.Recv, view.Lost)
	return renderRule("reverse stream (server→client)", headerStyle, body, width)
}

// renderTCPCorroborateKPI is the diagnostic card for the dedicated TCP
// retransmit probe. Surfaces bytes-out and bytes-retrans plus the
// derived percentage. Returns "" when view is nil (probe disabled by
// config or never started) so the caller can append unconditionally.
// When the local TCP_INFO syscall is unavailable on this host the view
// is non-nil with Supported=false; we render an "unsupported" body so
// the operator doesn't read the blank section as "0 retransmits".
func renderTCPCorroborateKPI(view *agg.TCPCorroborateView, width int) string {
	if view == nil {
		return ""
	}
	if !view.Supported {
		return renderRule("tcp corroborate (probe socket)", headerStyle,
			"  unsupported on this host (TCP_INFO unavailable)", width)
	}
	pctCell := lossColor(view.RetransPct).Render(fmt.Sprintf("%.3f%%", view.RetransPct))
	body := fmt.Sprintf("  tcp retransmits %d / %d bytes  (%s)",
		view.BytesRetrans, view.BytesOut, pctCell)
	if view.RttUs > 0 {
		body += fmt.Sprintf("   rtt %.2f ms (min %.2f ms)",
			float64(view.RttUs)/1000.0, float64(view.MinRttUs)/1000.0)
	}
	return renderRule("tcp corroborate (probe socket)", headerStyle, body, width)
}

func formatDuration(d time.Duration) string {
	s := int(d.Seconds())
	return fmt.Sprintf("%02d:%02d", s/60, s%60)
}

// columnLadder picks the column set for the given terminal width. Used
// by both forward and reverse hop tables so they collapse in lockstep.
type columnLadder struct {
	showAddr bool // include the IP/Addr column
	showWide bool // include Best/Worst columns
}

func ladderFor(width int) columnLadder {
	return columnLadder{
		showAddr: width >= 50,
		showWide: width >= 80,
	}
}

// renderHopTable formats the forward hop table inside its section frame.
// Loss% colors and the RTT-vs-baseline ▲ glyph cascade through the table
// package's StyleFunc and per-cell content respectively. Suspect rows
// carry a red "!" prefix on the TTL cell.
func renderHopTable(hops []agg.HopView, width int) string {
	if len(hops) == 0 {
		return renderRule("forward path", headerStyle, "  "+dimStyle.Render("(no hops resolved yet)"), width)
	}
	ladder := ladderFor(width)
	headers := hopHeaders(ladder, "IP", "ICMP Loss%")
	items := collapseSilentRuns(len(hops), func(i int) bool { return hops[i].IP == "" })
	rows := make([][]string, 0, len(items))
	for _, it := range items {
		if it.runEnd >= 0 {
			rows = append(rows, silentSummaryRow(ladder, hops[it.runStart].TTL, hops[it.runEnd].TTL, hops[it.runStart].Sent))
			continue
		}
		i := it.single
		h := hops[i]
		silent := h.IP == ""
		ttl := fmt.Sprintf("%d", h.TTL)
		// Silent hops carry no actionable loss/RTT evidence; the correlator
		// already skips them, but guard here too so a stale Suspect=true
		// from an older binary or future code path doesn't render an "!".
		if h.Suspect && !silent {
			ttl = redStyle.Render("!") + ttl
		}
		ip := h.IP
		if silent {
			ip = "*"
		}
		var lossCell string
		switch {
		case silent:
			lossCell = dimStyle.Render("—")
		case agg.RateLimitedICMP(hops, i):
			// Downstream responsive hop has materially lower loss, so the
			// elevated number here is ICMP rate-limiting on the router's
			// control plane — not a data-plane drop. Dim the cell so it
			// doesn't read as a fault.
			lossCell = dimStyle.Render(fmt.Sprintf("%.2f%%", h.LossPct))
		default:
			lossCell = lossColor(h.LossPct).Render(fmt.Sprintf("%.2f%%", h.LossPct))
		}
		last := fmt.Sprintf("%.2f", h.LastRTTMS)
		if g := rttBaselineGlyphFor(h.LastRTTMS, h.BaselineRTTMS, h.StdDevRTTMS); strings.TrimSpace(stripAnsiInline(g)) != "" {
			last = g + last
		}
		row := hopRow(ladder,
			ttl, ip,
			fmt.Sprintf("%d", h.Sent),
			lossCell,
			last,
			fmt.Sprintf("%.2f", h.AvgRTTMS),
			fmt.Sprintf("%.2f", h.BestRTTMS),
			fmt.Sprintf("%.2f", h.WorstRTTMS),
		)
		if silent && !h.Suspect {
			row = dimRow(row)
		}
		rows = append(rows, row)
	}
	body := buildHopTable(headers, rows)
	return renderRule("forward path", headerStyle, indentBlock(body, 2), width)
}

// renderReverseHopTable mirrors renderHopTable but for the server's
// reverse trace. Status string drives the header annotation style; no
// suspect gutter is supported (the correlator only flags forward hops).
func renderReverseHopTable(hops []agg.ReverseHopView, status string, width int) string {
	if status == "" {
		status = control.ReverseStatusDisabledByServer
	}
	annStyle := reverseStatusStyle(status)
	title := "reverse path" + annStyle.Render(fmt.Sprintf(" (status: %s, %d hops)", control.FriendlyReverseStatus(status), len(hops)))

	if len(hops) == 0 {
		return renderRule(title, headerStyle, "  "+annStyle.Render("(no reverse hops)"), width)
	}
	ladder := ladderFor(width)
	headers := hopHeaders(ladder, "Addr", "ICMP Loss%")
	items := collapseSilentRuns(len(hops), func(i int) bool { return hops[i].Addr == "" })
	rows := make([][]string, 0, len(items))
	for _, it := range items {
		if it.runEnd >= 0 {
			rows = append(rows, silentSummaryRow(ladder, hops[it.runStart].TTL, hops[it.runEnd].TTL, hops[it.runStart].Sent))
			continue
		}
		h := hops[it.single]
		silent := h.Addr == ""
		addr := h.Addr
		if silent {
			addr = "*"
		}
		var lossCell string
		if silent {
			lossCell = dimStyle.Render("—")
		} else {
			lossCell = lossColor(h.LossPct).Render(fmt.Sprintf("%.2f%%", h.LossPct))
		}
		last := fmt.Sprintf("%.2f", h.RTTMS)
		if g := rttBaselineGlyphFor(h.RTTMS, h.BaselineRTTMS, h.StdDevRTTMS); strings.TrimSpace(stripAnsiInline(g)) != "" {
			last = g + last
		}
		row := hopRow(ladder,
			fmt.Sprintf("%d", h.TTL), addr,
			fmt.Sprintf("%d", h.Sent),
			lossCell,
			last,
			fmt.Sprintf("%.2f", h.AvgRTTMS),
			fmt.Sprintf("%.2f", h.BestRTTMS),
			fmt.Sprintf("%.2f", h.WorstRTTMS),
		)
		if silent {
			row = dimRow(row)
		}
		rows = append(rows, row)
	}
	body := buildHopTable(headers, rows)
	if hint := leadingFilteredHint(hops); hint != "" {
		body = dimStyle.Render(hint) + "\n" + body
	}
	return renderRule(title, headerStyle, indentBlock(body, 2), width)
}

// leadingFilteredHint returns a one-line caption when the reverse trace
// opens with a run of silent hops followed by at least one responsive
// hop — the well-known signature of an upstream that filters ICMP
// TTL-exceeded but lets data through. Without the hint, an operator
// reading hops 1–8 as `*` would assume the reverse trace itself is
// broken; in practice it just couldn't see past the cloud's egress
// scrubber. Returns empty string when the leading run is zero, or when
// no later hop responds (then the trace really is dark, and the empty
// table speaks for itself).
func leadingFilteredHint(hops []agg.ReverseHopView) string {
	leading := 0
	for _, h := range hops {
		if h.Addr != "" {
			break
		}
		leading++
	}
	if leading == 0 || leading == len(hops) {
		return ""
	}
	first := hops[leading]
	if leading == 1 {
		return fmt.Sprintf("TTL 1 silent — first responding hop is TTL %d (likely filtered upstream)", first.TTL)
	}
	return fmt.Sprintf("TTLs %d–%d silent — first responding hop is TTL %d (likely filtered upstream)",
		hops[0].TTL, hops[leading-1].TTL, first.TTL)
}

// dimRow re-styles every cell with dimStyle. Used to fade rows for
// hops that never replied to ICMP — a non-replying router is not a
// fault, just informational, so it should not pull the eye. Cells
// already carrying their own styling (the suspect `!` prefix) are
// preserved by callers via the silent && !Suspect gate upstream.
func dimRow(cells []string) []string {
	out := make([]string, len(cells))
	for i, c := range cells {
		out[i] = dimStyle.Render(c)
	}
	return out
}

// hopItem is one entry in the rendered hop sequence. A run carries
// runStart/runEnd as inclusive indices into the source hops slice
// (runEnd-runStart+1 >= 2, all entries silent) and a -1 single. A
// non-run entry carries single as the hop index and -1 for both run
// fields.
type hopItem struct {
	runStart int
	runEnd   int
	single   int
}

// collapseSilentRuns folds contiguous runs of silent hops (≥2) into a
// single summary item; everything else passes through as a single
// item. Caller provides len(hops) and a silent-predicate so the helper
// stays agnostic to the forward/reverse HopView shape.
func collapseSilentRuns(n int, isSilent func(int) bool) []hopItem {
	items := make([]hopItem, 0, n)
	for i := 0; i < n; {
		if isSilent(i) {
			j := i + 1
			for j < n && isSilent(j) {
				j++
			}
			if j-i >= 2 {
				items = append(items, hopItem{runStart: i, runEnd: j - 1, single: -1})
				i = j
				continue
			}
		}
		items = append(items, hopItem{runStart: -1, runEnd: -1, single: i})
		i++
	}
	return items
}

// silentSummaryRow builds the single collapsed row that stands in for
// a run of silent hops. Sent is the representative per-hop probe count
// (all members of a silent run share the same Sent value since the
// prober TTL-walks them in parallel).
func silentSummaryRow(l columnLadder, startTTL, endTTL int, sent int64) []string {
	ttl := fmt.Sprintf("%d–%d", startTTL, endTTL)
	label := dimStyle.Render(fmt.Sprintf("⋯ %d silent hops", endTTL-startTTL+1))
	dash := dimStyle.Render("—")
	row := hopRow(l,
		ttl, label,
		fmt.Sprintf("%d", sent),
		dash, dash, dash, dash, dash,
	)
	return dimRow(row)
}

// hopHeaders returns the header slice for the chosen ladder rung.
// addrLabel parameterises the column name so the forward and reverse
// tables can share the layout while keeping their distinct vocabulary
// ("IP" vs "Addr"). lossLabel is the loss-column heading — typically
// "ICMP Loss%" for the ICMP probers and "TCP Loss%" for the TCP-mode
// prober — so the operator can tell at a glance which probe source's
// per-hop loss is rendered.
func hopHeaders(l columnLadder, addrLabel, lossLabel string) []string {
	switch {
	case l.showWide:
		return []string{"TTL", addrLabel, "Sent", lossLabel, "Last", "Avg", "Best", "Worst"}
	case l.showAddr:
		return []string{"TTL", addrLabel, "Sent", lossLabel, "Last", "Avg"}
	default:
		return []string{"TTL", "Sent", lossLabel, "Last", "Avg"}
	}
}

func hopRow(l columnLadder, ttl, addr, sent, loss, last, avg, best, worst string) []string {
	switch {
	case l.showWide:
		return []string{ttl, addr, sent, loss, last, avg, best, worst}
	case l.showAddr:
		return []string{ttl, addr, sent, loss, last, avg}
	default:
		return []string{ttl, sent, loss, last, avg}
	}
}

// buildHopTable renders a hop table via lipgloss/v2/table with all chrome
// disabled (the surrounding section border carries the visual frame).
// Header row gets a bold style; everything else stays whatever the
// caller embedded in cell strings (lossColor/redStyle/etc).
func buildHopTable(headers []string, rows [][]string) string {
	return table.New().
		Border(lipgloss.NormalBorder()).
		BorderTop(false).
		BorderBottom(false).
		BorderLeft(false).
		BorderRight(false).
		BorderHeader(false).
		BorderRow(false).
		BorderColumn(false).
		Headers(headers...).
		Rows(rows...).
		StyleFunc(func(row, _ int) lipgloss.Style {
			if row == table.HeaderRow {
				return tableHeaderStyle
			}
			return tableCellStyle
		}).
		Render()
}

// stripAnsiInline drops ANSI CSI escape sequences from s. Used to check
// whether the baseline glyph reduced to whitespace (no glyph) so we can
// skip prepending it when it would just add visual noise.
func stripAnsiInline(s string) string {
	for {
		i := strings.IndexByte(s, '\x1b')
		if i < 0 {
			return s
		}
		j := i + 1
		if j < len(s) && s[j] == '[' {
			for j < len(s) && s[j] != 'm' {
				j++
			}
			if j < len(s) {
				j++
			}
			s = s[:i] + s[j:]
		} else {
			return s
		}
	}
}

// rttBaselineGlyphFor tiers (last - baseline) / stddev:
// delta > 3 → red ▲▲, delta > 1.5 → amber ▲, otherwise empty. Zero
// baseline or stddev (no rolling data yet, or a slim-shape reverse hop
// from an old server) skips the glyph entirely.
func rttBaselineGlyphFor(last, baseline, stddev float64) string {
	if baseline <= 0 || stddev <= 0 {
		return ""
	}
	delta := (last - baseline) / stddev
	switch {
	case delta > 3:
		return redStyle.Render("▲▲")
	case delta > 1.5:
		return amberStyle.Render("▲")
	default:
		return ""
	}
}

// renderSparkline returns the per-second UDP-loss chart that sits
// directly under the KPI card. It produces a 3-row stacked bar chart
// (top/mid/bot rows fill in as loss climbs through the sparkIndex
// tiers) topped with a color legend so the green/amber/red mapping is
// self-documenting. Returns the empty string when terminal is too
// narrow.
func renderSparkline(buckets []agg.Bucket, width int) string {
	const prefix = "  udp loss/s "
	const indent = "             "
	const suffix = "  (last 60s)"
	if width < len(prefix)+1 {
		return ""
	}

	legend := okStyle.Render("▁") + " ≤1%  " +
		amberStyle.Render("▄") + " 1–5%  " +
		redStyle.Render("▇") + " >5%"
	headerLine := prefix + legend

	avail := width - len(indent) - len(suffix)
	if avail < 1 {
		avail = width - len(indent)
	}
	if avail < 1 {
		avail = 1
	}
	n := 60
	if avail < n {
		n = avail
	}
	if len(buckets) < n {
		n = len(buckets)
	}
	if n == 0 {
		fill := 20
		if avail < fill {
			fill = avail
		}
		placeholder := dimStyle.Render(strings.Repeat("·", fill))
		return headerLine + "\n" +
			indent + strings.Repeat(" ", fill) + "\n" +
			indent + strings.Repeat(" ", fill) + "\n" +
			indent + placeholder
	}

	last := buckets[len(buckets)-n:]
	var top, mid, bot strings.Builder
	for _, b := range last {
		stack := sparkStack[sparkIndex(b.StreamLossPct)]
		style := lossColor(b.StreamLossPct)
		top.WriteString(style.Render(string(stack[0])))
		mid.WriteString(style.Render(string(stack[1])))
		bot.WriteString(style.Render(string(stack[2])))
	}
	botLine := indent + bot.String()
	if avail >= n+len(suffix) {
		botLine += dimStyle.Render(suffix)
	}
	return headerLine + "\n" +
		indent + top.String() + "\n" +
		indent + mid.String() + "\n" +
		botLine
}

// reverseStatusStyle picks the lipgloss style applied to the reverse
// path's `(status: …, N hops)` annotation and the empty-body line.
// `ok` and `disabled_by_client` stay dim — the steady-state happy path
// and an intentional user-driven disable both belong in chrome.
// `disabled_by_server` and `terminated_at_nat` (plus the empty/unknown
// fallback that the renderer normalizes to `disabled_by_server`) get
// amber so an operator glancing at an empty `reverse_path` table sees
// the explanation without re-reading the header.
func reverseStatusStyle(status string) lipgloss.Style {
	switch status {
	case control.ReverseStatusOK, control.ReverseStatusDisabledByClient:
		return dimStyle
	default:
		return amberStyle
	}
}

// renderTCPHopTable mirrors renderHopTable for the TCP-mode hop prober.
// Wrapped in its own section frame labelled with the probed
// destination port so the operator can distinguish it from the ICMP
// table at a glance. Suspect rows carry the same red "!" gutter prefix
// as the ICMP table; the agg correlator's MergeSuspects step ensures
// "icmp+tcp" verdicts highlight the same TTL in both tables.
//
// supported=false (Windows stub) renders a one-line notice in place of
// the table so the section is visible (the operator knows the feature
// was on) without misleading them about real loss.
func renderTCPHopTable(hops []agg.HopView, port uint16, supported bool, width int) string {
	title := fmt.Sprintf("tcp hop probe (forward, port %d)", port)
	if !supported {
		return renderRule(title, headerStyle, "  "+amberStyle.Render("(unsupported on this host)"), width)
	}
	if len(hops) == 0 {
		return renderRule(title, headerStyle, "  "+dimStyle.Render("(no hops resolved yet)"), width)
	}
	ladder := ladderFor(width)
	headers := hopHeaders(ladder, "IP", "TCP Loss%")
	items := collapseSilentRuns(len(hops), func(i int) bool { return hops[i].IP == "" })
	rows := make([][]string, 0, len(items))
	for _, it := range items {
		if it.runEnd >= 0 {
			rows = append(rows, silentSummaryRow(ladder, hops[it.runStart].TTL, hops[it.runEnd].TTL, hops[it.runStart].Sent))
			continue
		}
		i := it.single
		h := hops[i]
		silent := h.IP == ""
		ttl := fmt.Sprintf("%d", h.TTL)
		if h.Suspect && !silent {
			ttl = redStyle.Render("!") + ttl
		}
		ip := h.IP
		if silent {
			ip = "*"
		}
		var lossCell string
		switch {
		case silent:
			lossCell = dimStyle.Render("—")
		case agg.RateLimitedICMP(hops, i):
			lossCell = dimStyle.Render(fmt.Sprintf("%.2f%%", h.LossPct))
		default:
			lossCell = lossColor(h.LossPct).Render(fmt.Sprintf("%.2f%%", h.LossPct))
		}
		last := fmt.Sprintf("%.2f", h.LastRTTMS)
		if g := rttBaselineGlyphFor(h.LastRTTMS, h.BaselineRTTMS, h.StdDevRTTMS); strings.TrimSpace(stripAnsiInline(g)) != "" {
			last = g + last
		}
		row := hopRow(ladder,
			ttl, ip,
			fmt.Sprintf("%d", h.Sent),
			lossCell,
			last,
			fmt.Sprintf("%.2f", h.AvgRTTMS),
			fmt.Sprintf("%.2f", h.BestRTTMS),
			fmt.Sprintf("%.2f", h.WorstRTTMS),
		)
		if silent && !h.Suspect {
			row = dimRow(row)
		}
		rows = append(rows, row)
	}
	body := buildHopTable(headers, rows)
	return renderRule(title, headerStyle, indentBlock(body, 2), width)
}

// renderReverseTCPHopTable mirrors renderReverseHopTable for the
// TCP-mode reverse hop prober. Status is folded into the parent
// ICMP-side reverse status — TCP and ICMP reverse-trace share the same
// privcheck.RawICMP gate, so they go on/off together.
func renderReverseTCPHopTable(hops []agg.ReverseHopView, port uint16, width int) string {
	title := fmt.Sprintf("tcp hop probe (reverse, port %d, %d hops)", port, len(hops))
	if len(hops) == 0 {
		return renderRule(title, headerStyle, "  "+dimStyle.Render("(no reverse hops)"), width)
	}
	ladder := ladderFor(width)
	headers := hopHeaders(ladder, "Addr", "TCP Loss%")
	items := collapseSilentRuns(len(hops), func(i int) bool { return hops[i].Addr == "" })
	rows := make([][]string, 0, len(items))
	for _, it := range items {
		if it.runEnd >= 0 {
			rows = append(rows, silentSummaryRow(ladder, hops[it.runStart].TTL, hops[it.runEnd].TTL, hops[it.runStart].Sent))
			continue
		}
		h := hops[it.single]
		silent := h.Addr == ""
		addr := h.Addr
		if silent {
			addr = "*"
		}
		var lossCell string
		if silent {
			lossCell = dimStyle.Render("—")
		} else {
			lossCell = lossColor(h.LossPct).Render(fmt.Sprintf("%.2f%%", h.LossPct))
		}
		last := fmt.Sprintf("%.2f", h.RTTMS)
		if g := rttBaselineGlyphFor(h.RTTMS, h.BaselineRTTMS, h.StdDevRTTMS); strings.TrimSpace(stripAnsiInline(g)) != "" {
			last = g + last
		}
		row := hopRow(ladder,
			fmt.Sprintf("%d", h.TTL), addr,
			fmt.Sprintf("%d", h.Sent),
			lossCell,
			last,
			fmt.Sprintf("%.2f", h.AvgRTTMS),
			fmt.Sprintf("%.2f", h.BestRTTMS),
			fmt.Sprintf("%.2f", h.WorstRTTMS),
		)
		if silent {
			row = dimRow(row)
		}
		rows = append(rows, row)
	}
	body := buildHopTable(headers, rows)
	return renderRule(title, headerStyle, indentBlock(body, 2), width)
}

// sparkIndex maps per-second loss% to a slot in sparkRunes. Thresholds are
// absolute (not relative-to-max) so zero loss always renders flat regardless
// of how spiky any single bucket gets.
func sparkIndex(pct float64) int {
	switch {
	case pct <= 0:
		return 0
	case pct < 0.1:
		return 1
	case pct < 0.5:
		return 2
	case pct < 1:
		return 3
	case pct < 2.5:
		return 4
	case pct < 5:
		return 5
	case pct < 10:
		return 6
	default:
		return 7
	}
}
