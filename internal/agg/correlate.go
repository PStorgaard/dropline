package agg

import (
	"fmt"
	"math"
	"sort"
)

// SuspectHop is one entry in the correlator's ranked output. Confidence is
// the fraction of loss-event seconds in which this hop's RTT or per-hop
// loss crossed its threshold (range (0.5, 1.0]). Evidence is a short
// human-readable string suitable for the JSON `evidence` field and the
// text renderer's "suspect hops:" line.
type SuspectHop struct {
	TTL        int
	Confidence float64
	Evidence   string
}

// Correlate runs the spec § Aggregator + correlation algorithm against
// the finalized timeline and the per-hop final state. Returns the suspect
// hops ranked by confidence (descending). Empty slice when there are no
// loss events, no hops, or no hop crosses the >50% co-occurrence bar.
//
// The algorithm:
//  1. Loss-event detection — bucket is a loss event if its
//     StreamLossPct > max(0.5, p95(all bucket loss%)). The 0.5% floor
//     keeps a clean test from flagging hops just because p95==0.
//  2. Hop scoring — for each *responsive* hop in finalHops (silent hops
//     have no actionable signal and are skipped), walk the loss-event
//     buckets and count co-occurrences. RTT signal: bucket-snapshot's
//     LastRTTMS > final.AvgRTTMS + 3·final.StdDevRTTMS. Loss signal:
//     bucket-snapshot's LossPct > 1.0, with the extra constraint that
//     the data plane doesn't contradict it — see rateLimitedICMP.
//  3. Ranking — confidence = max(rttHits, lossHits) / lossEvents. Above
//     0.5 → suspect. Output is sorted descending by confidence.
//
// The correlator is pure: no aggregator state mutation, no IO. Caller
// (typically the trace driver at end-of-test) is responsible for
// flipping per-hop Suspect flags and feeding the result to the
// renderers.
func Correlate(buckets []Bucket, finalHops []HopView) []SuspectHop {
	if len(buckets) == 0 || len(finalHops) == 0 {
		return nil
	}
	losses := make([]float64, len(buckets))
	for i, b := range buckets {
		losses[i] = b.StreamLossPct
	}
	// Floor at 1.0% per second: a clean test with overall loss well under
	// 1% can still have individual seconds spiking to 0.5–1% (one packet
	// in a few hundred). Those aren't operationally interesting, but at
	// 0.5% the floor catches them and any RTT variance on a slow path
	// reads as a correlated suspect. 1% requires a real glitch in that
	// second before the correlator considers it.
	threshold := math.Max(1.0, percentile(losses, 0.95))

	// We use ≥ rather than strict > so a uniform-loss timeline (every
	// second showing the same loss) still surfaces correlation. Strict
	// > would yield zero loss events in that case despite a clear
	// signal, because the spike's own value sets p95.
	var lossEvents []Bucket
	for _, b := range buckets {
		if b.StreamLossPct >= threshold {
			lossEvents = append(lossEvents, b)
		}
	}
	if len(lossEvents) == 0 {
		return nil
	}

	type hopScore struct {
		rttHits      int
		lossHits     int
		rttBaseline  float64
		suppressLoss bool
	}
	scores := make(map[int]*hopScore, len(finalHops))
	for i, h := range finalHops {
		// Silent hops carry no usable signal: their bucket-snapshot
		// LossPct is 100% (they never reply), which would otherwise trip
		// the loss check on every event and falsely flag every star — a
		// router filtering ICMP is not the same as one losing your
		// packets. Skip them entirely; the renderer dims them anyway.
		if h.IP == "" {
			continue
		}
		scores[h.TTL] = &hopScore{
			rttBaseline: h.AvgRTTMS + 3*h.StdDevRTTMS,
			// Suppress the loss signal when the data plane contradicts
			// it. The RTT signal stays — actual queueing delay shows up
			// regardless of ICMP throttling.
			suppressLoss: RateLimitedICMP(finalHops, i),
		}
	}
	if len(scores) == 0 {
		return nil
	}

	for _, b := range lossEvents {
		for _, bh := range b.Hops {
			sc, ok := scores[bh.TTL]
			if !ok {
				continue
			}
			if sc.rttBaseline > 0 && bh.LastRTTMS > sc.rttBaseline {
				sc.rttHits++
			}
			if !sc.suppressLoss && bh.LossPct > 1.0 {
				sc.lossHits++
			}
		}
	}

	out := make([]SuspectHop, 0, len(finalHops))
	for _, h := range finalHops {
		sc := scores[h.TTL]
		if sc == nil {
			continue
		}
		hits := sc.rttHits
		if sc.lossHits > hits {
			hits = sc.lossHits
		}
		conf := float64(hits) / float64(len(lossEvents))
		if conf <= 0.5 {
			continue
		}
		out = append(out, SuspectHop{
			TTL:        h.TTL,
			Confidence: conf,
			Evidence:   formatEvidence(sc.rttHits, sc.lossHits, len(lossEvents), sc.rttBaseline),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Confidence > out[j].Confidence
	})
	return out
}

// RateLimitedICMP reports whether hops[i]'s elevated final ICMP loss is
// contradicted by the data plane: if any responsive downstream hop shows
// materially lower final loss, packets *did* reach it through hops[i] —
// so hops[i] cannot have been dropping data, and what reads as loss here
// is its TTL-exceeded ICMP generation being throttled. The 5pp slack
// absorbs ordinary measurement noise; sources below 5% loss are not high
// enough to be plausible rate-limiters worth suppressing in the first
// place. Caller passes hops sorted by TTL ascending (the only producer,
// IngestForwardHop, preserves probe.Snapshot ordering). Exported so the
// renderers can soften the loss-column color on the same hops the
// correlator already discounts.
func RateLimitedICMP(hops []HopView, i int) bool {
	src := hops[i]
	if src.LossPct < 5 {
		return false
	}
	for j := i + 1; j < len(hops); j++ {
		d := hops[j]
		if d.IP == "" {
			continue
		}
		if d.LossPct+5 < src.LossPct {
			return true
		}
	}
	return false
}

// formatEvidence builds the short human-readable string for a suspect hop.
// Picks the dominant signal (RTT vs loss) by hit count; ties favor the
// RTT phrasing because it's the more specific statement (carries the
// baseline number the operator will check first).
func formatEvidence(rttHits, lossHits, lossEvents int, rttBaseline float64) string {
	if rttHits >= lossHits {
		return fmt.Sprintf("RTT spike >%.1fms in %d/%d stream-loss seconds", rttBaseline, rttHits, lossEvents)
	}
	return fmt.Sprintf("hop loss >1%% in %d/%d stream-loss seconds", lossHits, lossEvents)
}

// percentile returns the qth percentile (0 ≤ q ≤ 1) of values using
// linear interpolation between adjacent ranks (the "type 7" definition
// matching numpy / pandas defaults). Returns 0 for an empty input. We
// prefer linear interpolation over nearest-rank because nearest-rank on
// small N lands the threshold *on* the spike value, which then fails any
// strict-greater-than comparison. Linear interpolation gives a usable
// threshold even when the timeline has only a handful of buckets.
func percentile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if q <= 0 {
		q = 0
	}
	if q >= 1 {
		q = 1
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (pos-float64(lo))*(sorted[hi]-sorted[lo])
}
