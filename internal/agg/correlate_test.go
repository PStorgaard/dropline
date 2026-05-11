package agg

import (
	"strings"
	"testing"
)

func TestPercentile(t *testing.T) {
	// Linear interpolation: pos = q*(n-1); result = lo + (pos-lo)*(hi-lo)
	cases := []struct {
		name string
		in   []float64
		q    float64
		want float64
	}{
		{"empty", nil, 0.95, 0},
		{"single", []float64{42}, 0.95, 42},
		// pos = 19*0.95 = 18.05; sorted[18]=18, sorted[19]=19; 18 + 0.05 = 18.05
		{"sorted-20", []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, 0.95, 18.05},
		// sorted=[1,2,3,5,8,9]; pos = 5*0.95 = 4.75; sorted[4]=8, sorted[5]=9; 8+0.75 = 8.75
		{"unsorted", []float64{5, 3, 8, 1, 9, 2}, 0.95, 8.75},
		{"q=0", []float64{5, 3, 8, 1, 9}, 0, 1},
		{"q=1", []float64{5, 3, 8, 1, 9}, 1, 9},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := percentile(c.in, c.q); got != c.want {
				t.Errorf("percentile(%v, %v) = %v, want %v", c.in, c.q, got, c.want)
			}
		})
	}
}

func TestCorrelateNoEvents(t *testing.T) {
	// Clean test — every bucket has 0 loss; threshold floor is 1.0%; no
	// loss events, so no suspects.
	buckets := make([]Bucket, 10)
	for i := range buckets {
		buckets[i] = Bucket{T: i, StreamRecv: 1000, StreamLossPct: 0}
	}
	hops := []HopView{{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1}}
	if got := Correlate(buckets, hops); len(got) != 0 {
		t.Errorf("clean test should produce no suspects, got %+v", got)
	}
}

func TestCorrelateNoBuckets(t *testing.T) {
	if got := Correlate(nil, []HopView{{TTL: 1}}); got != nil {
		t.Errorf("no buckets should produce nil, got %+v", got)
	}
}

func TestCorrelateNoHops(t *testing.T) {
	buckets := []Bucket{{T: 0, StreamLossPct: 5}}
	if got := Correlate(buckets, nil); got != nil {
		t.Errorf("no hops should produce nil, got %+v", got)
	}
}

func TestCorrelateRTTOnlySuspect(t *testing.T) {
	// 4 buckets, one loss event (idx 2 has 10% loss). Hop's final stats
	// give baseline = 1.0 + 3*0.1 = 1.3ms. The bucket-2 snapshot has
	// LastRTTMS=50, well over the threshold → 1/1 = 100% confidence.
	hops := []HopView{{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1}}
	bucketHops := []HopView{{TTL: 1, LastRTTMS: 50.0, LossPct: 0}}
	buckets := []Bucket{
		{T: 0, StreamLossPct: 0},
		{T: 1, StreamLossPct: 0},
		{T: 2, StreamLossPct: 10, Hops: bucketHops},
		{T: 3, StreamLossPct: 0},
	}
	got := Correlate(buckets, hops)
	if len(got) != 1 {
		t.Fatalf("want 1 suspect, got %d (%+v)", len(got), got)
	}
	if got[0].TTL != 1 || got[0].Confidence != 1.0 {
		t.Errorf("suspect: %+v", got[0])
	}
	if !strings.Contains(got[0].Evidence, "RTT spike") {
		t.Errorf("evidence should be RTT-flavored, got %q", got[0].Evidence)
	}
}

func TestCorrelateLossOnlySuspect(t *testing.T) {
	// Same shape but the bucket hop snapshot has a loss spike, not RTT.
	hops := []HopView{{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 5.0, StdDevRTTMS: 1.0}}
	bucketHops := []HopView{{TTL: 1, LastRTTMS: 5.0, LossPct: 8}}
	buckets := []Bucket{
		{T: 0, StreamLossPct: 0},
		{T: 1, StreamLossPct: 10, Hops: bucketHops},
	}
	got := Correlate(buckets, hops)
	if len(got) != 1 {
		t.Fatalf("want 1 suspect, got %d (%+v)", len(got), got)
	}
	if !strings.Contains(got[0].Evidence, "hop loss") {
		t.Errorf("evidence should be loss-flavored, got %q", got[0].Evidence)
	}
}

func TestCorrelateRanking(t *testing.T) {
	// Two hops, both suspect, hop 2 has higher confidence — must come
	// first. We use uniform-loss buckets (5%) which all qualify as loss
	// events under the ≥-threshold semantics; hop 2 trips RTT in 4/4
	// buckets, hop 1 in 3/4.
	hops := []HopView{
		{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1},
		{TTL: 2, IP: "10.0.0.2", AvgRTTMS: 1.0, StdDevRTTMS: 0.1},
	}
	makeBucket := func(t int, lossPct float64, h1RTT, h2RTT float64) Bucket {
		return Bucket{
			T: t, StreamLossPct: lossPct,
			Hops: []HopView{
				{TTL: 1, LastRTTMS: h1RTT},
				{TTL: 2, LastRTTMS: h2RTT},
			},
		}
	}
	buckets := []Bucket{
		makeBucket(0, 5, 50, 50),
		makeBucket(1, 5, 50, 50),
		makeBucket(2, 5, 50, 50),
		makeBucket(3, 5, 1.0, 50), // hop 1 doesn't trip RTT in this one
	}
	got := Correlate(buckets, hops)
	if len(got) != 2 {
		t.Fatalf("want 2 suspects, got %d (%+v)", len(got), got)
	}
	if got[0].TTL != 2 {
		t.Errorf("expected hop 2 first (higher confidence), got %+v", got)
	}
	if got[0].Confidence <= got[1].Confidence {
		t.Errorf("ranking violated: %v <= %v", got[0].Confidence, got[1].Confidence)
	}
}

func TestCorrelateConfidenceBoundary(t *testing.T) {
	// Exactly 50% co-occurrence (2 out of 4) must NOT be flagged — spec
	// requires strictly greater than 50%.
	hops := []HopView{{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1}}
	mk := func(t int, lossPct, hopRTT float64) Bucket {
		return Bucket{T: t, StreamLossPct: lossPct, Hops: []HopView{{TTL: 1, LastRTTMS: hopRTT}}}
	}
	buckets := []Bucket{
		mk(0, 5, 50),
		mk(1, 5, 50),
		mk(2, 5, 1.0),
		mk(3, 5, 1.0),
	}
	if got := Correlate(buckets, hops); len(got) != 0 {
		t.Errorf("50%% should not flag, got %+v", got)
	}
}

func TestCorrelateFloorActivates(t *testing.T) {
	// Most buckets at 0.1% loss, one at 1.5%. p95 sits well below 1.0%,
	// so the floor lifts the threshold to 1.0%. The 1.5% bucket is the
	// only loss event; hop trips RTT in it → 1/1 = 100% confidence.
	hops := []HopView{{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1}}
	buckets := make([]Bucket, 30)
	for i := range buckets {
		buckets[i] = Bucket{T: i, StreamLossPct: 0.1}
	}
	buckets[15].StreamLossPct = 1.5
	buckets[15].Hops = []HopView{{TTL: 1, LastRTTMS: 50}}
	got := Correlate(buckets, hops)
	if len(got) != 1 || got[0].Confidence != 1.0 {
		t.Fatalf("expected single 100%% suspect from 1/1 floor-driven loss event, got %+v", got)
	}
}

// Sub-1% per-second spikes on an otherwise-clean test must not produce
// suspects. Mirrors the real-world case where overall stream loss is
// ~0.5% but individual seconds spike to 0.7–0.9% — those are operational
// noise, not glitches worth flagging middle hops for.
func TestCorrelateFloorRejectsSubPercentSpikes(t *testing.T) {
	hops := []HopView{{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1}}
	buckets := make([]Bucket, 30)
	for i := range buckets {
		buckets[i] = Bucket{T: i, StreamLossPct: 0.1}
	}
	for _, i := range []int{5, 12, 20} {
		buckets[i].StreamLossPct = 0.8
		buckets[i].Hops = []HopView{{TTL: 1, LastRTTMS: 50}}
	}
	if got := Correlate(buckets, hops); len(got) != 0 {
		t.Errorf("sub-1%% spikes should not trip the floor, got %+v", got)
	}
}

// A silent (no-IP) hop must not be flagged even though its bucket-snapshot
// LossPct is 100% in every loss event — a router filtering ICMP carries no
// usable signal about whether user traffic is dropping.
func TestCorrelateSkipsSilentHops(t *testing.T) {
	hops := []HopView{{TTL: 1, IP: ""}}
	bucketHops := []HopView{{TTL: 1, LossPct: 100}}
	buckets := []Bucket{
		{T: 0, StreamLossPct: 5, Hops: bucketHops},
		{T: 1, StreamLossPct: 5, Hops: bucketHops},
	}
	if got := Correlate(buckets, hops); len(got) != 0 {
		t.Errorf("silent hop should not be a suspect, got %+v", got)
	}
}

// A hop showing high ICMP loss while a downstream responsive hop is clean
// is almost certainly ICMP rate-limiting, not a real loss source — packets
// reaching the downstream hop must have transited the upstream one. The
// loss signal is suppressed for such hops; the RTT signal still counts.
func TestCorrelateSuppressesRateLimitedICMPLoss(t *testing.T) {
	// Hop 1 has 30% final ICMP loss; hop 2 is clean (0%). The bucket
	// snapshot has hop 1 at 30% loss in every loss event — under the old
	// algorithm that would yield 100% confidence. With suppression, hop 1
	// scores zero and is dropped.
	hops := []HopView{
		{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1, LossPct: 30},
		{TTL: 2, IP: "10.0.0.2", AvgRTTMS: 1.0, StdDevRTTMS: 0.1, LossPct: 0},
	}
	mk := func(t int) Bucket {
		return Bucket{T: t, StreamLossPct: 5, Hops: []HopView{
			{TTL: 1, LastRTTMS: 1.0, LossPct: 30},
			{TTL: 2, LastRTTMS: 1.0, LossPct: 0},
		}}
	}
	buckets := []Bucket{mk(0), mk(1), mk(2), mk(3)}
	if got := Correlate(buckets, hops); len(got) != 0 {
		t.Errorf("rate-limited hop with clean downstream should not be suspect, got %+v", got)
	}
}

// Suppression must not gate on RTT — when the hop's loss is contradicted
// by a clean downstream but its RTT spikes during loss events, RTT alone
// can still mark it suspect.
func TestCorrelateSuppressedLossKeepsRTTSignal(t *testing.T) {
	hops := []HopView{
		{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1, LossPct: 30},
		{TTL: 2, IP: "10.0.0.2", AvgRTTMS: 1.0, StdDevRTTMS: 0.1, LossPct: 0},
	}
	mk := func(t int, rtt float64) Bucket {
		return Bucket{T: t, StreamLossPct: 5, Hops: []HopView{
			{TTL: 1, LastRTTMS: rtt, LossPct: 30},
			{TTL: 2, LastRTTMS: 1.0, LossPct: 0},
		}}
	}
	buckets := []Bucket{mk(0, 50), mk(1, 50), mk(2, 50), mk(3, 50)}
	got := Correlate(buckets, hops)
	if len(got) != 1 || got[0].TTL != 1 {
		t.Fatalf("RTT-only suspect after loss suppression: got %+v", got)
	}
	if !strings.Contains(got[0].Evidence, "RTT spike") {
		t.Errorf("evidence should be RTT-flavored, got %q", got[0].Evidence)
	}
}

// Suppression requires a *responsive* downstream hop. A high-loss hop
// followed only by silent stars cannot be ruled out — keep the signal.
func TestCorrelateNoSuppressionWhenAllDownstreamSilent(t *testing.T) {
	hops := []HopView{
		{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1, LossPct: 30},
		{TTL: 2, IP: ""},
		{TTL: 3, IP: ""},
	}
	mk := func(t int) Bucket {
		return Bucket{T: t, StreamLossPct: 5, Hops: []HopView{
			{TTL: 1, LastRTTMS: 1.0, LossPct: 30},
		}}
	}
	buckets := []Bucket{mk(0), mk(1)}
	got := Correlate(buckets, hops)
	if len(got) != 1 || got[0].TTL != 1 {
		t.Fatalf("loss signal should survive when no responsive downstream contradicts it, got %+v", got)
	}
}

func TestCorrelateBothSignalsMaxWins(t *testing.T) {
	// One hop with RTT signal in 1/2 events and loss signal in 2/2 events.
	// max wins → 100% confidence, evidence is loss-flavored.
	hops := []HopView{{TTL: 1, IP: "10.0.0.1", AvgRTTMS: 1.0, StdDevRTTMS: 0.1}}
	buckets := []Bucket{
		{T: 0, StreamLossPct: 5, Hops: []HopView{{TTL: 1, LastRTTMS: 50, LossPct: 5}}},
		{T: 1, StreamLossPct: 5, Hops: []HopView{{TTL: 1, LastRTTMS: 1.0, LossPct: 5}}},
	}
	got := Correlate(buckets, hops)
	if len(got) != 1 || got[0].Confidence != 1.0 {
		t.Fatalf("want single 100%% suspect, got %+v", got)
	}
	if !strings.Contains(got[0].Evidence, "hop loss") {
		t.Errorf("loss should dominate evidence, got %q", got[0].Evidence)
	}
}
