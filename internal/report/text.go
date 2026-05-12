package report

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/PStorgaard/dropline/internal/agg"
	"github.com/PStorgaard/dropline/internal/control"
)

// RenderText writes the human-readable summary form of d to w. Section
// order: header (target / started / duration / config), forward hop
// table (omitted when empty), stream stats, burst histogram.
// Correlation is omitted until the suspect-hop correlator lands.
//
// We build the whole report into a single string before the one Write
// call so error handling stays a one-liner and partial output never
// reaches w on failure.
func RenderText(w io.Writer, d Data) error {
	var sb strings.Builder

	sb.WriteString("dropline trace report\n")
	fmt.Fprintf(&sb, "  target:       %s\n", d.Target)
	fmt.Fprintf(&sb, "  started:      %s\n", d.StartedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(&sb, "  duration:     %.2fs\n", d.DurationS)
	fmt.Fprintf(&sb, "  rate (tx):    %s\n", agg.FormatRate(d.Stream.RateTxBPS))
	fmt.Fprintf(&sb, "  packet size:  %d B\n", d.Config.PacketSize)

	if len(d.Hops) > 0 {
		writeHopTable(&sb, d.Hops)
	}

	writeReversePath(&sb, d)
	writeSuspectHops(&sb, d)

	kdNote := ""
	if d.Stream.KernelDrops > 0 {
		kdNote = "  ! "
	}
	ldNote := ""
	if d.Stream.LocalDrops > 0 {
		ldNote = "  ! receiver overload — udp loss above is suspect"
	}

	sb.WriteString("\nudp stream:\n")
	fmt.Fprintf(&sb, "  sent:         %d\n", d.Stream.Sent)
	fmt.Fprintf(&sb, "  received:     %d\n", d.Stream.Recv)
	fmt.Fprintf(&sb, "  lost:         %d (%.3f%% udp loss)\n", d.Stream.Loss, d.lossPct())
	fmt.Fprintf(&sb, "  out of order: %d\n", d.Stream.OutOfOrder)
	fmt.Fprintf(&sb, "  duplicates:   %d\n", d.Stream.Duplicates)
	fmt.Fprintf(&sb, "  jitter:       %.2f ms\n", d.Stream.JitterMS)
	fmt.Fprintf(&sb, "  kernel drops: %d%s\n", d.Stream.KernelDrops, kdNote)
	fmt.Fprintf(&sb, "  local drops:  %d%s\n", d.Stream.LocalDrops, ldNote)
	fmt.Fprintf(&sb, "  rate (rx):    %s\n", agg.FormatRate(d.Stream.RateRxBPS))

	b := d.Stream.Bursts
	fmt.Fprintf(&sb, "  bursts:       %s\n",
		agg.FormatBurstSummary(b.Ten99, b.HundredUp, b.Max))

	if c := d.TCPCorroborate; c != nil {
		sb.WriteString("\ntcp corroborate (probe socket):\n")
		if !c.Supported {
			sb.WriteString("  unsupported on this host (TCP_INFO unavailable)\n")
		} else {
			fmt.Fprintf(&sb, "  bytes out:    %d\n", c.BytesOut)
			fmt.Fprintf(&sb, "  bytes retrans:%d (%.3f%%)\n", c.BytesRetrans, c.RetransPct)
			if c.RttUs > 0 {
				fmt.Fprintf(&sb, "  rtt:          %.2f ms (min %.2f ms)\n",
					float64(c.RttUs)/1000.0, float64(c.MinRttUs)/1000.0)
			}
		}
	}

	if t := d.TCPHopProbe; t != nil {
		writeTCPHopProbe(&sb, t)
	}

	if r := d.ReverseStream; r != nil {
		sb.WriteString("\nreverse udp stream (server→client):\n")
		fmt.Fprintf(&sb, "  sent (server):%d\n", r.Sent)
		fmt.Fprintf(&sb, "  received:     %d\n", r.Recv)
		fmt.Fprintf(&sb, "  lost:         %d (%.3f%% udp loss)\n", r.Lost, r.LossPct)
		fmt.Fprintf(&sb, "  out of order: %d\n", r.OutOfOrder)
		fmt.Fprintf(&sb, "  duplicates:   %d\n", r.Duplicates)
		fmt.Fprintf(&sb, "  jitter:       %.2f ms\n", r.JitterMS)
		if r.LocalDrops > 0 {
			fmt.Fprintf(&sb, "  local drops:  %d  ! receiver overload — reverse udp loss above is suspect\n", r.LocalDrops)
		}
		fmt.Fprintf(&sb, "  duration:     %.2fs\n", r.DurationS)
	}

	_, err := io.WriteString(w, sb.String())
	return err
}

// writeHopTable renders the forward hop table. Columns: TTL, IP, Sent,
// Recv, Loss%, Last/Best/Worst/Avg/StdDev RTT (ms). The table follows
// the header section and precedes the stream summary.
// Column header is "ICMP Loss%" rather than "Loss%" so the reader doesn't
// conflate per-hop probe loss with the UDP stream loss in the summary —
// a router that rate-limits ICMP reads as 100% here but passes user
// traffic cleanly. The suspect-hop list below is the authoritative
// "this hop is actually losing your traffic" signal.
func writeHopTable(sb *strings.Builder, hops []HopReport) {
	sb.WriteString("\nforward path:\n")
	fmt.Fprintf(sb, "  %-3s  %-15s  %5s  %5s  %11s  %7s  %7s  %7s  %7s  %7s\n",
		"TTL", "IP", "Sent", "Recv", "ICMP Loss%", "Last", "Best", "Worst", "Avg", "StDev")
	for _, h := range hops {
		ip := h.IP
		if ip == "" {
			ip = "*"
		}
		var lossCell string
		if h.IP == "" {
			lossCell = fmt.Sprintf("%11s", "—")
		} else {
			lossCell = fmt.Sprintf("%10.2f%%", h.LossPct)
		}
		fmt.Fprintf(sb, "  %-3d  %-15s  %5d  %5d  %s  %7.2f  %7.2f  %7.2f  %7.2f  %7.2f\n",
			h.TTL, ip, h.Sent, h.Recv, lossCell,
			h.LastRTTMS, h.BestRTTMS, h.WorstRTTMS, h.AvgRTTMS, h.StdDevRTTMS)
	}
}

// writeReversePath renders the reverse-trace section. Always prints a
// status line so the user can see why an empty reverse_path is empty;
// the table itself is omitted when no hops were merged. Empty status
// renders as "disabled_by_server" to match the JSON renderer's default.
func writeReversePath(sb *strings.Builder, d Data) {
	status := d.ReversePathStatus
	if status == "" {
		status = control.ReverseStatusDisabledByServer
	}
	fmt.Fprintf(sb, "\nreverse path (status: %s):\n", control.FriendlyReverseStatus(status))
	if len(d.ReversePath) == 0 {
		return
	}
	if hint := leadingFilteredHint(d.ReversePath); hint != "" {
		fmt.Fprintf(sb, "  %s\n", hint)
	}
	fmt.Fprintf(sb, "  %-3s  %-15s  %5s  %5s  %11s  %7s  %7s  %7s  %7s\n",
		"TTL", "Addr", "Sent", "Recv", "ICMP Loss%", "Last", "Best", "Worst", "Avg")
	for _, h := range d.ReversePath {
		addr := h.Addr
		if addr == "" {
			addr = "*"
		}
		var lossCell string
		if h.Addr == "" {
			lossCell = fmt.Sprintf("%11s", "—")
		} else {
			lossCell = fmt.Sprintf("%10.2f%%", h.LossPct)
		}
		fmt.Fprintf(sb, "  %-3d  %-15s  %5d  %5d  %s  %7.2f  %7.2f  %7.2f  %7.2f\n",
			h.TTL, addr, h.Sent, h.Recv, lossCell,
			h.RTTMS, h.BestRTTMS, h.WorstRTTMS, h.AvgRTTMS)
	}
}

// leadingFilteredHint mirrors the TUI helper of the same name: when the
// reverse trace opens with silent hops followed by responsive ones, the
// data plane works but ICMP TTL-exceeded is filtered upstream. The hint
// prevents the operator from misreading the leading stars as a broken
// trace. Empty string when the leading run is zero or when no later hop
// responds (the trace really is dark).
func leadingFilteredHint(hops []ReverseHopReport) string {
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

// writeSuspectHops renders the correlator's ranked suspect-hop list when
// non-empty. Skipped silently otherwise — the JSON output is the
// authoritative consumer; the text section is just operator-friendly.
// Source ("icmp", "tcp", "icmp+tcp") is appended in brackets when
// non-default so the operator can see which probe(s) flagged the hop.
func writeSuspectHops(sb *strings.Builder, d Data) {
	if len(d.Correlation) == 0 {
		return
	}
	sb.WriteString("\nsuspect hops:\n")
	for _, h := range d.Correlation {
		src := h.Source
		if src == "" || src == "icmp" {
			fmt.Fprintf(sb, "  ttl=%d  confidence=%.2f  %s\n", h.TTL, h.Confidence, h.Evidence)
			continue
		}
		fmt.Fprintf(sb, "  ttl=%d  confidence=%.2f  [%s]  %s\n", h.TTL, h.Confidence, src, h.Evidence)
	}
}

// writeTCPHopProbe renders the TCP-mode hop probe section. Mirrors the
// forward + reverse ICMP sections — same column layout, same loss-cell
// formatting for silent hops — labeled with the probed destination
// port. Supported tracks the *forward* prober's platform support
// (false on Windows); when false we emit a one-line notice in place of
// the forward table but still render the reverse table when the
// server has populated it — a Windows client paired with a Linux
// server can receive valid ReverseTCPHopUpdate data.
func writeTCPHopProbe(sb *strings.Builder, t *TCPHopProbeReport) {
	fmt.Fprintf(sb, "\ntcp hop probe (port %d):\n", t.Port)
	if !t.Supported {
		sb.WriteString("  forward: unsupported on this host (raw-ICMP TCP demux unavailable)\n")
	} else if len(t.ForwardHops) > 0 {
		sb.WriteString("  forward:\n")
		fmt.Fprintf(sb, "    %-3s  %-15s  %5s  %5s  %11s  %7s  %7s  %7s  %7s  %7s\n",
			"TTL", "IP", "Sent", "Recv", "TCP Loss%", "Last", "Best", "Worst", "Avg", "StDev")
		for _, h := range t.ForwardHops {
			ip := h.IP
			if ip == "" {
				ip = "*"
			}
			var lossCell string
			if h.IP == "" {
				lossCell = fmt.Sprintf("%11s", "—")
			} else {
				lossCell = fmt.Sprintf("%10.2f%%", h.LossPct)
			}
			fmt.Fprintf(sb, "    %-3d  %-15s  %5d  %5d  %s  %7.2f  %7.2f  %7.2f  %7.2f  %7.2f\n",
				h.TTL, ip, h.Sent, h.Recv, lossCell,
				h.LastRTTMS, h.BestRTTMS, h.WorstRTTMS, h.AvgRTTMS, h.StdDevRTTMS)
		}
	}
	if len(t.ReverseHops) > 0 {
		sb.WriteString("  reverse:\n")
		fmt.Fprintf(sb, "    %-3s  %-15s  %5s  %5s  %11s  %7s  %7s  %7s  %7s\n",
			"TTL", "Addr", "Sent", "Recv", "TCP Loss%", "Last", "Best", "Worst", "Avg")
		for _, h := range t.ReverseHops {
			addr := h.Addr
			if addr == "" {
				addr = "*"
			}
			var lossCell string
			if h.Addr == "" {
				lossCell = fmt.Sprintf("%11s", "—")
			} else {
				lossCell = fmt.Sprintf("%10.2f%%", h.LossPct)
			}
			fmt.Fprintf(sb, "    %-3d  %-15s  %5d  %5d  %s  %7.2f  %7.2f  %7.2f  %7.2f\n",
				h.TTL, addr, h.Sent, h.Recv, lossCell,
				h.RTTMS, h.BestRTTMS, h.WorstRTTMS, h.AvgRTTMS)
		}
	}
}
