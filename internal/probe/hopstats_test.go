package probe

import (
	"math"
	"net"
	"testing"
)

func TestHopState_NoSends(t *testing.T) {
	h := &hopState{}
	got := h.snapshot(1)
	if got.Sent != 0 || got.Recv != 0 {
		t.Fatalf("expected zeroed counts, got sent=%d recv=%d", got.Sent, got.Recv)
	}
	if got.LossPct != 0 {
		t.Fatalf("LossPct on empty hop should be 0, got %v", got.LossPct)
	}
	if got.Addr != "" {
		t.Fatalf("Addr should be empty on empty hop, got %q", got.Addr)
	}
}

func TestHopState_LossPct(t *testing.T) {
	h := &hopState{}
	for i := 0; i < 10; i++ {
		h.sent++
	}
	// Three replies — 7 lost out of 10. inflight stays 0 because this
	// test models the post-timeout state (the prober's emit-time prune
	// has already aged out the unreplied probes).
	addr := net.ParseIP("10.0.0.1")
	h.recordReply(addr, 1.0, false)
	h.recordReply(addr, 1.0, false)
	h.recordReply(addr, 1.0, false)
	got := h.snapshot(1)
	if got.LossPct < 69.99 || got.LossPct > 70.01 {
		t.Fatalf("expected 70%% loss, got %v", got.LossPct)
	}
}

// Inflight probes (sent but neither replied nor timed out) must be
// excluded from the loss denominator. Mirrors Linux mtr's net_loss
// formula: loss% = (settled - returned) / settled, settled = sent -
// transit. A reply hasn't had time to come back yet ≠ probe lost.
func TestHopState_LossExcludesInflight(t *testing.T) {
	// 10 sent, 8 replied, 2 still in flight: 0% loss (no settled gap).
	h := &hopState{sent: 10, inflight: 2}
	addr := net.ParseIP("10.0.0.1")
	for i := 0; i < 8; i++ {
		h.recordReply(addr, 1.0, false)
	}
	if got := h.snapshot(1).LossPct; got != 0 {
		t.Fatalf("8/10 replied + 2 inflight: want 0%% loss, got %v", got)
	}

	// 10 sent, 7 replied, 3 still in flight, 0 timed out: also 0%.
	h = &hopState{sent: 10, inflight: 3}
	for i := 0; i < 7; i++ {
		h.recordReply(addr, 1.0, false)
	}
	if got := h.snapshot(1).LossPct; got != 0 {
		t.Fatalf("7 replied + 3 inflight: want 0%% loss, got %v", got)
	}

	// 10 sent, 5 replied, 2 still in flight, 3 truly lost (timed out —
	// inflight already decremented by the prune): 3/8 = 37.5% loss.
	h = &hopState{sent: 10, inflight: 2}
	for i := 0; i < 5; i++ {
		h.recordReply(addr, 1.0, false)
	}
	got := h.snapshot(1).LossPct
	if got < 37.49 || got > 37.51 {
		t.Fatalf("5 replied + 2 inflight + 3 timed-out: want 37.5%% loss, got %v", got)
	}

	// All inflight, none settled yet → 0%, not divide-by-zero.
	h = &hopState{sent: 4, inflight: 4}
	if got := h.snapshot(1).LossPct; got != 0 {
		t.Fatalf("all inflight: want 0%% loss, got %v", got)
	}
}

func TestHopState_RTTStats(t *testing.T) {
	h := &hopState{}
	h.sent = 5
	addr := net.ParseIP("10.0.0.2")
	for _, rtt := range []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0} {
		h.recordReply(addr, rtt, false)
	}
	got := h.snapshot(2)
	if got.LastRTTMS != 9.0 {
		t.Fatalf("LastRTTMS: got %v want 9.0", got.LastRTTMS)
	}
	if got.BestRTTMS != 2.0 {
		t.Fatalf("BestRTTMS: got %v want 2.0", got.BestRTTMS)
	}
	if got.WorstRTTMS != 9.0 {
		t.Fatalf("WorstRTTMS: got %v want 9.0", got.WorstRTTMS)
	}
	// Mean of {2,4,4,4,5,5,7,9} = 40/8 = 5.0
	if math.Abs(got.AvgRTTMS-5.0) > 1e-9 {
		t.Fatalf("AvgRTTMS: got %v want 5.0", got.AvgRTTMS)
	}
	// Sample stddev of that sequence ≈ 2.138 (sqrt(32/7)).
	want := math.Sqrt(32.0 / 7.0)
	if math.Abs(got.StdDevRTTMS-want) > 1e-9 {
		t.Fatalf("StdDevRTTMS: got %v want %v", got.StdDevRTTMS, want)
	}
}

func TestHopState_StdDevSingleSample(t *testing.T) {
	h := &hopState{}
	h.sent = 1
	h.recordReply(net.ParseIP("10.0.0.3"), 4.2, false)
	if got := h.snapshot(1).StdDevRTTMS; got != 0 {
		t.Fatalf("StdDev with one sample should be 0, got %v", got)
	}
}

func TestHopState_TerminusSticky(t *testing.T) {
	h := &hopState{}
	h.sent = 3
	addr := net.ParseIP("10.0.0.4")
	h.recordReply(addr, 1.0, false)
	h.recordReply(addr, 1.0, true) // echo reply once
	h.recordReply(addr, 1.0, false)
	if !h.snapshot(1).Terminus {
		t.Fatalf("Terminus should remain true once observed")
	}
}

// EWMA: first sample seeds the baseline at the sample value; subsequent
// samples blend with α from ewmaAlpha. Tolerance is generous because
// the math is float64 but the test keeps the ratio shape exact.
func TestHopState_EWMABaseline(t *testing.T) {
	h := &hopState{}
	h.sent = 1
	addr := net.ParseIP("10.0.0.6")

	h.recordReply(addr, 10.0, false)
	if got := h.snapshot(1).BaselineRTTMS; math.Abs(got-10.0) > 1e-9 {
		t.Fatalf("first sample: BaselineRTTMS = %v, want 10.0", got)
	}

	h.sent = 2
	h.recordReply(addr, 20.0, false)
	// α=0.2 → 0.2*20 + 0.8*10 = 12.0
	want := 12.0
	if got := h.snapshot(1).BaselineRTTMS; math.Abs(got-want) > 1e-9 {
		t.Fatalf("second sample: BaselineRTTMS = %v, want %v", got, want)
	}

	// A long run of identical samples converges to that value.
	for i := 0; i < 100; i++ {
		h.sent++
		h.recordReply(addr, 5.0, false)
	}
	if got := h.snapshot(1).BaselineRTTMS; math.Abs(got-5.0) > 1e-3 {
		t.Fatalf("after long run at 5ms: BaselineRTTMS = %v, want ~5.0", got)
	}
}

// BaselineRTTMS stays zero when no replies have arrived; renderers use
// that as the gate for showing the ▲ indicator.
func TestHopState_EWMAZeroBeforeFirstReply(t *testing.T) {
	h := &hopState{}
	if got := h.snapshot(1).BaselineRTTMS; got != 0 {
		t.Fatalf("BaselineRTTMS before any reply: got %v, want 0", got)
	}
}

func TestHopState_AddrFixedOnFirstReply(t *testing.T) {
	h := &hopState{}
	h.sent = 2
	h.recordReply(net.ParseIP("10.0.0.5"), 1.0, false)
	h.recordReply(net.ParseIP("10.0.0.99"), 1.0, false) // ignored for addr
	if got := h.snapshot(1).Addr; got != "10.0.0.5" {
		t.Fatalf("Addr should pin on first reply, got %q", got)
	}
}
