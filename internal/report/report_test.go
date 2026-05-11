package report

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/PStorgaard/dropline/internal/agg"
	"github.com/PStorgaard/dropline/internal/control"
)

func fixture() Data {
	return Data{
		Version:   1,
		Target:    "srv.lab:5201",
		StartedAt: time.Date(2026, 5, 9, 8, 42, 11, 0, time.UTC),
		DurationS: 60.0,
		Config: Config{
			RateBPS:       10_000_000,
			PacketSize:    1200,
			MTRIntervalMS: 1000,
			MaxHops:       30,
			ReverseTrace:  "auto",
		},
		Stream: control.FinalStats{
			Sent:        3_000_000,
			Recv:        2_994_231,
			Loss:        5_769,
			OutOfOrder:  12,
			Duplicates:  0,
			JitterMS:    0.41,
			KernelDrops: 0,
			DurationS:   60.0,
			Bursts: control.BurstBuckets{
				One: 187, Two: 42, Three9: 11, Ten99: 2, HundredUp: 0, Max: 34,
			},
			RateTxBPS: 10_000_000,
			RateRxBPS: 9_980_000,
		},
		Timeline: []agg.Bucket{
			{T: 0, StreamRecv: 49890, StreamLost: 110},
			{T: 1, StreamRecv: 50000, StreamLost: 0},
		},
	}
}

func TestRenderText_ContainsKeyFields(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderText(&buf, fixture()); err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	out := buf.String()
	mustContain := []string{
		"dropline trace report",
		"srv.lab:5201",
		"2026-05-09T08:42:11Z",
		"60.00s",
		"10.00 Mbps",                  // tx rate
		"9.98 Mbps",                   // rx rate
		"5769 (0.192% udp loss)",      // loss count + computed pct
		"jitter:       0.41 ms",
		"worst 34 packets",            // burst summary tier
		"2 runs ≥10",                  // app-impacting count = Ten99 + HundredUp
	}
	for _, w := range mustContain {
		if !strings.Contains(out, w) {
			t.Errorf("text output missing %q.\nfull output:\n%s", w, out)
		}
	}
}

func TestRenderText_KernelDropFlag(t *testing.T) {
	d := fixture()
	d.Stream.KernelDrops = 7
	var buf bytes.Buffer
	if err := RenderText(&buf, d); err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	// "! " marker after the count signals operator attention.
	if !strings.Contains(buf.String(), "kernel drops: 7  ! ") {
		t.Errorf("expected kernel-drop flag, got:\n%s", buf.String())
	}
}

func TestRenderJSON_Schema(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderJSON(&buf, fixture()); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}

	if v, ok := got["version"].(float64); !ok || int(v) != 1 {
		t.Errorf("version: want 1, got %v", got["version"])
	}
	if got["target"] != "srv.lab:5201" {
		t.Errorf("target: %v", got["target"])
	}
	if got["started_at"] != "2026-05-09T08:42:11Z" {
		t.Errorf("started_at: %v", got["started_at"])
	}

	hops, ok := got["hops"].([]any)
	if !ok || len(hops) != 0 {
		t.Errorf("hops: want empty slice, got %v", got["hops"])
	}

	corr, ok := got["correlation"].(map[string]any)
	if !ok {
		t.Fatalf("correlation: want object, got %T", got["correlation"])
	}
	sus, ok := corr["suspect_hops"].([]any)
	if !ok || len(sus) != 0 {
		t.Errorf("correlation.suspect_hops: want empty slice, got %v", corr["suspect_hops"])
	}

	stream, ok := got["stream"].(map[string]any)
	if !ok {
		t.Fatalf("stream: want object, got %T", got["stream"])
	}
	if stream["sent"].(float64) != 3_000_000 {
		t.Errorf("stream.sent: %v", stream["sent"])
	}
	// loss_pct = 5769 / (2994231 + 5769) * 100 = 0.1923
	pct := stream["loss_pct"].(float64)
	if pct < 0.19 || pct > 0.20 {
		t.Errorf("stream.loss_pct: want ~0.192, got %v", pct)
	}
	if stream["kernel_drops"].(float64) != 0 {
		t.Errorf("stream.kernel_drops: %v", stream["kernel_drops"])
	}

	// Bursts collapsed to {1,2,3-9,10+,max}; 10+ folds 10-99 + 100+.
	bursts, ok := stream["bursts"].(map[string]any)
	if !ok {
		t.Fatalf("stream.bursts: want object, got %T", stream["bursts"])
	}
	if got, want := bursts["10+"].(float64), float64(2); got != want {
		t.Errorf("bursts.10+: want %v, got %v", want, got)
	}
	if _, hasOld := bursts["10-99"]; hasOld {
		t.Errorf("bursts must not include the internal 10-99/100+ split")
	}

	timeline, ok := got["timeline"].([]any)
	if !ok || len(timeline) != 2 {
		t.Fatalf("timeline: want 2 entries, got %v", got["timeline"])
	}
	row0 := timeline[0].(map[string]any)
	if row0["t"].(float64) != 0 || row0["stream_loss"].(float64) != 110 {
		t.Errorf("timeline[0]: %v", row0)
	}
	if hops, ok := row0["hops"].([]any); !ok || len(hops) != 0 {
		t.Errorf("timeline[0].hops: want empty slice, got %v", row0["hops"])
	}
}

func TestRenderJSON_EmptyTimeline(t *testing.T) {
	d := fixture()
	d.Timeline = nil
	var buf bytes.Buffer
	if err := RenderJSON(&buf, d); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	// nil should still serialize as `[]`, not `null`.
	if !strings.Contains(buf.String(), `"timeline": []`) {
		t.Errorf("expected empty timeline array, got:\n%s", buf.String())
	}
}

func TestRenderText_HopTable(t *testing.T) {
	d := fixture()
	d.Hops = []HopReport{
		{TTL: 1, IP: "10.0.0.1", Sent: 60, Recv: 60, LossPct: 0,
			LastRTTMS: 0.4, BestRTTMS: 0.3, WorstRTTMS: 0.6, AvgRTTMS: 0.4, StdDevRTTMS: 0.05},
		{TTL: 2, IP: "10.0.0.2", Sent: 60, Recv: 58, LossPct: 3.33,
			LastRTTMS: 1.2, BestRTTMS: 1.0, WorstRTTMS: 1.5, AvgRTTMS: 1.2, StdDevRTTMS: 0.1},
	}
	var buf bytes.Buffer
	if err := RenderText(&buf, d); err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"forward path:",
		"TTL", "IP", "Loss%", "StDev",
		"10.0.0.1",
		"10.0.0.2",
		"3.33%",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("hop table missing %q.\nfull output:\n%s", want, out)
		}
	}
	// Header section still ends before the hop table; stream section
	// still appears after.
	if i, j := strings.Index(out, "forward path:"), strings.Index(out, "stream:"); !(i > 0 && j > i) {
		t.Errorf("section ordering: forward path must come before stream; got idx %d, %d", i, j)
	}
}

func TestRenderJSON_PopulatedHops(t *testing.T) {
	d := fixture()
	d.Hops = []HopReport{
		{TTL: 1, IP: "10.0.0.1", Sent: 60, Recv: 60, LossPct: 0,
			LastRTTMS: 0.4, BestRTTMS: 0.3, WorstRTTMS: 0.6, AvgRTTMS: 0.4, StdDevRTTMS: 0.05},
	}
	var buf bytes.Buffer
	if err := RenderJSON(&buf, d); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	hops := got["hops"].([]any)
	if len(hops) != 1 {
		t.Fatalf("hops: want 1 entry, got %d", len(hops))
	}
	h := hops[0].(map[string]any)
	if h["ttl"].(float64) != 1 || h["ip"] != "10.0.0.1" {
		t.Errorf("hop fields wrong: %+v", h)
	}
	if h["suspect"].(bool) {
		t.Errorf("suspect should default false until correlator ships")
	}
	rtt := h["rtt_ms"].(map[string]any)
	for _, key := range []string{"last", "best", "worst", "avg", "stddev"} {
		if _, ok := rtt[key]; !ok {
			t.Errorf("rtt_ms missing %q: %+v", key, rtt)
		}
	}
}

func TestRenderJSON_LossPctZeroNoTraffic(t *testing.T) {
	d := fixture()
	d.Stream.Recv = 0
	d.Stream.Loss = 0
	var buf bytes.Buffer
	if err := RenderJSON(&buf, d); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	_ = json.Unmarshal(buf.Bytes(), &got)
	stream := got["stream"].(map[string]any)
	if stream["loss_pct"].(float64) != 0 {
		t.Errorf("loss_pct with no traffic: want 0, got %v", stream["loss_pct"])
	}
}

// renderJSONMap is a small helper that renders a Data and returns the
// decoded JSON document as a map for shape assertions.
func renderJSONMap(t *testing.T, d Data) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	if err := RenderJSON(&buf, d); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

func TestRenderJSONReversePathOK(t *testing.T) {
	d := fixture()
	d.ReversePathStatus = "ok"
	d.ReversePath = []ReverseHopReport{
		{
			TTL: 1, Addr: "10.0.0.1", RTTMS: 0.4, LossPct: 0,
			Sent: 4, Recv: 4, BestRTTMS: 0.3, WorstRTTMS: 0.6,
			AvgRTTMS: 0.4, StdDevRTTMS: 0.05, BaselineRTTMS: 0.42,
		},
		{TTL: 2, Addr: "10.0.0.2", RTTMS: 1.2, LossPct: 1.5},
	}
	got := renderJSONMap(t, d)
	if got["reverse_path_status"] != "ok" {
		t.Errorf("status: want ok, got %v", got["reverse_path_status"])
	}
	rp, ok := got["reverse_path"].([]any)
	if !ok || len(rp) != 2 {
		t.Fatalf("reverse_path: want 2 entries, got %v", got["reverse_path"])
	}
	first := rp[0].(map[string]any)
	if first["ttl"].(float64) != 1 || first["addr"] != "10.0.0.1" {
		t.Errorf("first hop mismatch: %+v", first)
	}
	// rtt_ms is now an object identical in shape to hops[].rtt_ms.
	rtt, ok := first["rtt_ms"].(map[string]any)
	if !ok {
		t.Fatalf("rtt_ms: want object, got %T (%v)", first["rtt_ms"], first["rtt_ms"])
	}
	for _, key := range []string{"last", "best", "worst", "avg", "stddev"} {
		if _, ok := rtt[key]; !ok {
			t.Errorf("rtt_ms missing %q: %+v", key, rtt)
		}
	}
	if rtt["last"].(float64) != 0.4 || rtt["best"].(float64) != 0.3 {
		t.Errorf("rtt_ms values mismatch: %+v", rtt)
	}
	if first["sent"].(float64) != 4 || first["recv"].(float64) != 4 {
		t.Errorf("sent/recv: %+v", first)
	}
}

func TestRenderJSONReversePathDisabledByClient(t *testing.T) {
	d := fixture()
	d.ReversePathStatus = "disabled_by_client"
	got := renderJSONMap(t, d)
	if got["reverse_path_status"] != "disabled_by_client" {
		t.Errorf("status: want disabled_by_client, got %v", got["reverse_path_status"])
	}
	rp := got["reverse_path"].([]any)
	if len(rp) != 0 {
		t.Errorf("reverse_path: want empty, got %d entries", len(rp))
	}
}

func TestRenderJSONReversePathDisabledByServer(t *testing.T) {
	d := fixture()
	d.ReversePathStatus = "disabled_by_server"
	got := renderJSONMap(t, d)
	if got["reverse_path_status"] != "disabled_by_server" {
		t.Errorf("status: want disabled_by_server, got %v", got["reverse_path_status"])
	}
}

func TestRenderJSONReversePathStatusDefault(t *testing.T) {
	// Empty ReversePathStatus must surface as "disabled_by_server" so the
	// JSON shape is stable when an old server doesn't echo a reverse_trace
	// field in Ready and the trace driver leaves the status unresolved.
	d := fixture()
	d.ReversePathStatus = ""
	got := renderJSONMap(t, d)
	if got["reverse_path_status"] != "disabled_by_server" {
		t.Errorf("default status: want disabled_by_server, got %v", got["reverse_path_status"])
	}
	if rp, ok := got["reverse_path"].([]any); !ok {
		t.Errorf("reverse_path missing or not a JSON array: %T", got["reverse_path"])
	} else if len(rp) != 0 {
		t.Errorf("reverse_path with no hops should be empty, got %d entries", len(rp))
	}
}

func TestRenderJSONSuspectHopsPopulated(t *testing.T) {
	d := fixture()
	d.Correlation = []SuspectHopReport{
		{TTL: 4, Confidence: 0.83, Evidence: "RTT spike >50.0ms in 19/23 stream-loss seconds"},
	}
	got := renderJSONMap(t, d)
	corr, ok := got["correlation"].(map[string]any)
	if !ok {
		t.Fatalf("correlation missing or wrong shape: %v", got["correlation"])
	}
	suspects, ok := corr["suspect_hops"].([]any)
	if !ok || len(suspects) != 1 {
		t.Fatalf("suspect_hops: want 1 entry, got %v", corr["suspect_hops"])
	}
	first := suspects[0].(map[string]any)
	if first["ttl"].(float64) != 4 || first["confidence"].(float64) != 0.83 {
		t.Errorf("suspect entry mismatch: %+v", first)
	}
	if !strings.Contains(first["evidence"].(string), "RTT spike") {
		t.Errorf("evidence missing: %v", first["evidence"])
	}
}

func TestRenderJSONSuspectHopsEmptySliceNotNull(t *testing.T) {
	d := fixture()
	d.Correlation = nil
	got := renderJSONMap(t, d)
	corr := got["correlation"].(map[string]any)
	if _, ok := corr["suspect_hops"].([]any); !ok {
		t.Errorf("suspect_hops should be a JSON array (even when empty), got %T", corr["suspect_hops"])
	}
}

func TestRenderJSONTimelineHopsPopulated(t *testing.T) {
	d := fixture()
	// Add a per-bucket hop snapshot to the second timeline row.
	d.Timeline[1].Hops = []agg.HopView{
		{TTL: 1, IP: "10.0.0.1", Sent: 1, Recv: 1, LastRTTMS: 0.4, AvgRTTMS: 0.4, BestRTTMS: 0.3, WorstRTTMS: 0.6},
	}
	got := renderJSONMap(t, d)
	tl := got["timeline"].([]any)
	row1 := tl[1].(map[string]any)
	hops := row1["hops"].([]any)
	if len(hops) != 1 {
		t.Fatalf("timeline[1].hops: want 1, got %d", len(hops))
	}
	first := hops[0].(map[string]any)
	if first["ttl"].(float64) != 1 || first["ip"] != "10.0.0.1" {
		t.Errorf("timeline hop mismatch: %+v", first)
	}
	// Bucket 0 has no hops — must still render as an empty JSON array, not null.
	row0 := tl[0].(map[string]any)
	if _, ok := row0["hops"].([]any); !ok {
		t.Errorf("timeline[0].hops should be empty JSON array, got %T", row0["hops"])
	}
}

func TestRenderTextSuspectHopsSection(t *testing.T) {
	d := fixture()
	d.Correlation = []SuspectHopReport{
		{TTL: 4, Confidence: 0.83, Evidence: "RTT spike >50.0ms in 19/23 stream-loss seconds"},
	}
	var buf bytes.Buffer
	if err := RenderText(&buf, d); err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "suspect hops:") {
		t.Errorf("missing suspect hops heading; got:\n%s", out)
	}
	if !strings.Contains(out, "ttl=4") || !strings.Contains(out, "confidence=0.83") {
		t.Errorf("missing ttl/confidence row; got:\n%s", out)
	}
}

func TestRenderTextReversePathSection(t *testing.T) {
	d := fixture()
	d.ReversePathStatus = "ok"
	d.ReversePath = []ReverseHopReport{
		{
			TTL: 1, Addr: "10.0.0.1", RTTMS: 0.4,
			Sent: 4, Recv: 4, BestRTTMS: 0.3, WorstRTTMS: 0.6, AvgRTTMS: 0.4,
		},
	}
	var buf bytes.Buffer
	if err := RenderText(&buf, d); err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "reverse path (status: ok)") {
		t.Errorf("missing reverse path heading; got:\n%s", out)
	}
	if !strings.Contains(out, "10.0.0.1") {
		t.Errorf("missing reverse hop addr; got:\n%s", out)
	}
	for _, want := range []string{"Sent", "Last", "Best", "Worst", "Avg"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing rich column %q; got:\n%s", want, out)
		}
	}
}

func TestRenderTextReversePathLeadingFilteredHint(t *testing.T) {
	d := fixture()
	d.ReversePathStatus = "ok"
	d.ReversePath = []ReverseHopReport{
		{TTL: 1, Addr: "", Sent: 60},
		{TTL: 2, Addr: "", Sent: 60},
		{TTL: 3, Addr: "10.0.0.1", Sent: 60, Recv: 60, RTTMS: 1},
	}
	var buf bytes.Buffer
	if err := RenderText(&buf, d); err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "TTLs 1–2 silent") {
		t.Errorf("missing leading-filtered hint; got:\n%s", out)
	}
	if !strings.Contains(out, "first responding hop is TTL 3") {
		t.Errorf("hint should point at TTL 3; got:\n%s", out)
	}

	// No leading silent run → hint suppressed.
	d.ReversePath = []ReverseHopReport{
		{TTL: 1, Addr: "10.0.0.1", Sent: 60, Recv: 60, RTTMS: 1},
	}
	buf.Reset()
	if err := RenderText(&buf, d); err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	if strings.Contains(buf.String(), "filtered upstream") {
		t.Errorf("hint should not render when first hop responded; got:\n%s", buf.String())
	}
}
