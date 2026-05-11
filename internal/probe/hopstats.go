package probe

import (
	"math"
	"net"
)

// hopState tracks rolling per-hop counters for a single TTL across the
// entire test. Spec § Probe layer: sent, recv, last_rtt, best, worst,
// avg, stddev, loss_pct.
//
// All access is serialized by Prober.mu — no internal locking.
type hopState struct {
	addr net.IP
	sent int64
	recv int64
	// inflight is the count of probes sent but neither replied nor aged
	// out yet. Excluded from the loss denominator: a probe that hasn't
	// had time to come back must not be counted as lost. Decremented in
	// recvLoop on a matched reply and in Prober.emit when an inflight
	// entry exceeds the loss timeout (then truly lost — sent stays,
	// recv doesn't, inflight drops to 0 so the gap surfaces as loss).
	inflight int64
	last     float64
	best     float64
	worst    float64
	mean     float64 // running mean for avg (Welford)
	m2       float64 // running sum of squared deviations (Welford)
	ewma     float64 // exponentially-weighted recent baseline RTT (ms)
	ewmaInit bool    // true once ewma has been seeded by the first reply
	terminus bool
}

// ewmaAlpha is the smoothing factor for the per-hop rolling RTT
// baseline. Higher α tracks recent samples faster; lower α is steadier
// against single outliers. 0.2 reaches ~63% of a step change after ~5
// samples — about 5s at the canonical 1Hz sweep cadence.
const ewmaAlpha = 0.2

// recordReply folds one ICMP reply into the hop's rolling stats. rttMS
// is the round-trip in milliseconds. terminus is true when the reply
// was an EchoReply (this hop is the destination), false for
// TimeExceeded (intermediate hop). The first reply seen for a hop fixes
// addr; later replies from a different responder (load-balanced paths)
// are ignored for the address column to keep the table stable.
func (h *hopState) recordReply(addr net.IP, rttMS float64, terminus bool) {
	if h.addr == nil && addr != nil {
		h.addr = append(net.IP(nil), addr...)
	}
	h.recv++
	h.last = rttMS
	if h.recv == 1 {
		h.best = rttMS
		h.worst = rttMS
		h.mean = rttMS
	} else {
		if rttMS < h.best {
			h.best = rttMS
		}
		if rttMS > h.worst {
			h.worst = rttMS
		}
		// Welford's online algorithm: stable single-pass mean+variance.
		delta := rttMS - h.mean
		h.mean += delta / float64(h.recv)
		delta2 := rttMS - h.mean
		h.m2 += delta * delta2
	}
	if !h.ewmaInit {
		h.ewma = rttMS
		h.ewmaInit = true
	} else {
		h.ewma = ewmaAlpha*rttMS + (1-ewmaAlpha)*h.ewma
	}
	if terminus {
		h.terminus = true
	}
}

// stddev returns the sample standard deviation of recorded RTTs in ms.
// Returns 0 for fewer than two samples (the spec sample uses 0 in that
// case).
func (h *hopState) stddev() float64 {
	if h.recv < 2 {
		return 0
	}
	return math.Sqrt(h.m2 / float64(h.recv-1))
}

// snapshot returns an immutable HopStat for ttl. lossPct mirrors
// Linux mtr's net_loss: the denominator is "settled" probes
// (sent - inflight), so a probe that hasn't had time to come back is
// not counted as lost. Returns 0 when no probes have settled yet
// (settled == 0 and sent == 0 are the same case at startup).
func (h *hopState) snapshot(ttl int) HopStat {
	settled := h.sent - h.inflight
	lossPct := 0.0
	if settled > 0 {
		lossPct = float64(settled-h.recv) / float64(settled) * 100
	}
	addr := ""
	if h.addr != nil {
		addr = h.addr.String()
	}
	return HopStat{
		TTL:           ttl,
		Addr:          addr,
		Sent:          h.sent,
		Recv:          h.recv,
		LastRTTMS:     h.last,
		BestRTTMS:     h.best,
		WorstRTTMS:    h.worst,
		AvgRTTMS:      h.mean,
		StdDevRTTMS:   h.stddev(),
		BaselineRTTMS: h.ewma,
		LossPct:       lossPct,
		Terminus:      h.terminus,
	}
}
