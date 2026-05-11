package agg

import "fmt"

// FormatBurstSummary renders the burst histogram as a single
// plain-English line. Used by both the live TUI and the saved text
// report so the two surfaces stay in sync.
//
// The histogram counts *runs* of consecutive lost packets in five
// length buckets. The raw notation (1×N 2×N 3-9×N ...) reads as noise
// to anyone not already steeped in the spec, so we collapse it into a
// tier that names the worst observed run and, when relevant, flags
// runs long enough to disturb real-time applications.
//
// Tiers:
//   - max == 0: no loss runs at all.
//   - max ≤ 2:  only isolated single/double drops, usually invisible.
//   - max ≤ 9:  short bursts; TCP/UDP apps absorb these with retries.
//   - max ≥ 10: at least one run long enough to stall VoIP/video.
//     Counts ten99 + hundredUp to quantify how often.
func FormatBurstSummary(ten99, hundredUp, max int64) string {
	switch {
	case max == 0:
		return "none"
	case max <= 2:
		return fmt.Sprintf("worst %d packets (isolated losses)", max)
	case max <= 9:
		return fmt.Sprintf("worst %d packets, no app-impacting runs", max)
	default:
		appImpacting := ten99 + hundredUp
		return fmt.Sprintf("worst %d packets · %d runs ≥10 (likely affects VoIP/video)",
			max, appImpacting)
	}
}

// FormatRate renders bits-per-second as a human-friendly string with
// the largest unit that produces a value ≥ 1.
func FormatRate(bps int64) string {
	switch {
	case bps >= 1_000_000_000:
		return fmt.Sprintf("%.2f Gbps", float64(bps)/1e9)
	case bps >= 1_000_000:
		return fmt.Sprintf("%.2f Mbps", float64(bps)/1e6)
	case bps >= 1_000:
		return fmt.Sprintf("%.2f kbps", float64(bps)/1e3)
	default:
		return fmt.Sprintf("%d bps", bps)
	}
}
