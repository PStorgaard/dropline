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
//
// Source is the probe source that produced the suspect score: "icmp"
// for the default ICMP TTL-walker, "tcp" for the TCP-mode hop prober,
// or "icmp+tcp" when MergeSuspects has merged matching TTLs from both
// sources (the strongest verdict — both protocols see the same hop
// dropping). Empty Source on legacy code paths is treated as "icmp".
type SuspectHop struct {
	TTL        int
	Confidence float64
	Evidence   string
	Source     string
}

// Correlate runs the spec § Aggregator + correlation algorithm against
// the finalized timeline and the per-hop final ICMP state. Returns the
// suspect hops ranked by confidence (descending). Empty slice when there
// are no loss events, no hops, or no hop crosses the >50% co-occurrence
// bar.
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
// renderers. Suspect.Source is set to "icmp".
func Correlate(buckets []Bucket, finalHops []HopView) []SuspectHop {
	return correlateImpl(buckets, finalHops, func(b Bucket) []HopView { return b.Hops }, "icmp")
}

// CorrelateTCP mirrors Correlate but reads each bucket's TCPHops
// snapshot instead of Hops. Loss-event detection is shared (same
// stream-loss buckets); per-hop scoring uses the TCP-mode prober's
// per-bucket samples. Suspect.Source is set to "tcp".
//
// Callers typically run both Correlate and CorrelateTCP at end-of-test
// and pass the two slices through MergeSuspects, which folds matching
// TTLs into an "icmp+tcp" verdict (the strongest evidence).
func CorrelateTCP(buckets []Bucket, finalHops []HopView) []SuspectHop {
	return correlateImpl(buckets, finalHops, func(b Bucket) []HopView { return b.TCPHops }, "tcp")
}

// correlateImpl is the shared implementation of Correlate and
// CorrelateTCP. The bucketHops accessor picks which per-bucket hop
// slice to score against (Hops vs TCPHops); everything else is
// identical, including the RateLimitedICMP suppression — a router
// throttling its TimeExceeded generation does the same thing whether
// the inner packet was ICMP or TCP.
func correlateImpl(buckets []Bucket, finalHops []HopView, bucketHops func(Bucket) []HopView, source string) []SuspectHop {
	if len(buckets) == 0 || len(finalHops) == 0 {
		return nil
	}
	losses := make([]float64, len(buckets))
	for i, b := range buckets {
		losses[i] = b.StreamLossPct
	}
	threshold := math.Max(1.0, percentile(losses, 0.95))

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
		if h.IP == "" {
			continue
		}
		scores[h.TTL] = &hopScore{
			rttBaseline:  h.AvgRTTMS + 3*h.StdDevRTTMS,
			suppressLoss: RateLimitedICMP(finalHops, i),
		}
	}
	if len(scores) == 0 {
		return nil
	}

	for _, b := range lossEvents {
		for _, bh := range bucketHops(b) {
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
			Source:     source,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Confidence > out[j].Confidence
	})
	return out
}

// MergeSuspects combines the ICMP and TCP correlator outputs into a
// single ranked list. TTLs flagged by both correlators are folded into
// one entry with Source="icmp+tcp", Confidence = max of the two, and
// Evidence reading "RTT/loss correlated in both ICMP and TCP probes"
// — the strongest verdict the correlator can produce. TTLs flagged by
// only one source keep that source's confidence and evidence. Result
// is sorted by Confidence descending; "icmp+tcp" entries break ties
// against single-source entries.
func MergeSuspects(icmp, tcp []SuspectHop) []SuspectHop {
	if len(icmp) == 0 && len(tcp) == 0 {
		return nil
	}
	byTTL := make(map[int]SuspectHop, len(icmp)+len(tcp))
	for _, s := range icmp {
		if s.Source == "" {
			s.Source = "icmp"
		}
		byTTL[s.TTL] = s
	}
	for _, s := range tcp {
		if s.Source == "" {
			s.Source = "tcp"
		}
		if existing, ok := byTTL[s.TTL]; ok {
			// Same TTL flagged by both — upgrade to the joint verdict.
			conf := existing.Confidence
			if s.Confidence > conf {
				conf = s.Confidence
			}
			byTTL[s.TTL] = SuspectHop{
				TTL:        s.TTL,
				Confidence: conf,
				Evidence:   "RTT/loss correlated in both ICMP and TCP probes",
				Source:     "icmp+tcp",
			}
			continue
		}
		byTTL[s.TTL] = s
	}
	out := make([]SuspectHop, 0, len(byTTL))
	for _, v := range byTTL {
		out = append(out, v)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Confidence != out[j].Confidence {
			return out[i].Confidence > out[j].Confidence
		}
		// Tie-breaker: "icmp+tcp" outranks single-source so the strongest
		// evidence floats to the top.
		return sourceRank(out[i].Source) < sourceRank(out[j].Source)
	})
	return out
}

func sourceRank(s string) int {
	switch s {
	case "icmp+tcp":
		return 0
	case "tcp":
		return 1
	case "icmp":
		return 2
	}
	return 3
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
