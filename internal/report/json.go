package report

import (
	"encoding/json"
	"io"
	"time"

	"github.com/PStorgaard/dropline/internal/agg"
	"github.com/PStorgaard/dropline/internal/control"
)

// jsonDoc mirrors spec § Output formats — the JSON schema. We define a
// dedicated struct (rather than marshaling Data directly) so the wire
// shape stays decoupled from internal types and so loss_pct is a
// computed float instead of the raw int64 count carried in
// control.FinalStats.
type jsonDoc struct {
	Version           int                   `json:"version"`
	Target            string                `json:"target"`
	StartedAt         string                `json:"started_at"`
	DurationS         float64               `json:"duration_s"`
	Config            Config                `json:"config"`
	Hops              []jsonHop             `json:"hops"`
	ReversePath       []jsonReverseHop      `json:"reverse_path"`
	ReversePathStatus string                `json:"reverse_path_status"`
	Stream            jsonStream            `json:"stream"`
	ReverseStream     *ReverseStreamReport  `json:"reverse_stream,omitempty"`
	TCPCorroborate    *TCPCorroborateReport `json:"tcp_corroborate,omitempty"`
	Correlation       jsonCorrelation       `json:"correlation"`
	Timeline          []jsonTimelineRow     `json:"timeline"`
}

// jsonReverseHop mirrors jsonHop's shape — same `rtt_ms` struct, same
// `sent` / `recv` counters — modulo two intentional differences: Addr
// in place of IP, no Suspect flag (correlator runs against forward-path
// stream loss only).
type jsonReverseHop struct {
	TTL      int     `json:"ttl"`
	Addr     string  `json:"addr"`
	Sent     int64   `json:"sent"`
	Recv     int64   `json:"recv"`
	LossPct  float64 `json:"loss_pct"`
	RTTMS    jsonRTT `json:"rtt_ms"`
	Terminus string  `json:"terminus,omitempty"`
}

// jsonHop matches the spec § Output formats hop shape. rtt_ms is a
// nested object so the schema is human-readable. suspect stays false
// until the correlator lands.
type jsonHop struct {
	TTL     int     `json:"ttl"`
	IP      string  `json:"ip"`
	Sent    int64   `json:"sent"`
	Recv    int64   `json:"recv"`
	LossPct float64 `json:"loss_pct"`
	RTTMS   jsonRTT `json:"rtt_ms"`
	Suspect bool    `json:"suspect"`
}

type jsonRTT struct {
	Last   float64 `json:"last"`
	Best   float64 `json:"best"`
	Worst  float64 `json:"worst"`
	Avg    float64 `json:"avg"`
	StdDev float64 `json:"stddev"`
}

type jsonStream struct {
	Sent        int64      `json:"sent"`
	Recv        int64      `json:"recv"`
	LossPct     float64    `json:"loss_pct"`
	OutOfOrder  int64      `json:"out_of_order"`
	Duplicates  int64      `json:"duplicates"`
	JitterMS    float64    `json:"jitter_ms"`
	KernelDrops int64      `json:"kernel_drops"`
	// LocalDrops surfaces receiver ring-overflow drops. Omitempty so the
	// JSON shape stays byte-identical to the v1 snapshot when zero (the
	// common case); non-zero values flag the reported LossPct as suspect.
	LocalDrops  int64      `json:"local_drops,omitempty"`
	Bursts      jsonBursts `json:"bursts"`
	RateTxBPS   int64      `json:"rate_tx_bps"`
	RateRxBPS   int64      `json:"rate_rx_bps"`
}

// jsonBursts matches the spec example exactly: it folds the 10-99 and
// 100+ buckets into one "10+" key. control.BurstBuckets keeps them
// separate for finer reporting; the JSON output collapses for brevity
// per the spec sample.
type jsonBursts struct {
	One    int64 `json:"1"`
	Two    int64 `json:"2"`
	Three9 int64 `json:"3-9"`
	TenUp  int64 `json:"10+"`
	Max    int64 `json:"max"`
}

type jsonCorrelation struct {
	SuspectHops []jsonSuspectHop `json:"suspect_hops"`
}

type jsonSuspectHop struct {
	TTL        int     `json:"ttl"`
	Confidence float64 `json:"confidence"`
	Evidence   string  `json:"evidence"`
}

type jsonTimelineRow struct {
	T          int       `json:"t"`
	StreamLoss int64     `json:"stream_loss"`
	Hops       []jsonHop `json:"hops"`
}

// RenderJSON writes the JSON form of d to w with two-space indentation
// and a trailing newline.
func RenderJSON(w io.Writer, d Data) error {
	status := d.ReversePathStatus
	if status == "" {
		status = control.ReverseStatusDisabledByServer
	}
	doc := jsonDoc{
		Version:           1,
		Target:            d.Target,
		StartedAt:         d.StartedAt.UTC().Format(time.RFC3339),
		DurationS:         d.DurationS,
		Config:            d.Config,
		Hops:              hopsToJSON(d.Hops),
		ReversePath:       reverseHopsToJSON(d.ReversePath),
		ReversePathStatus: status,
		Stream: jsonStream{
			Sent:        d.Stream.Sent,
			Recv:        d.Stream.Recv,
			LossPct:     d.lossPct(),
			OutOfOrder:  d.Stream.OutOfOrder,
			Duplicates:  d.Stream.Duplicates,
			JitterMS:    d.Stream.JitterMS,
			KernelDrops: d.Stream.KernelDrops,
			LocalDrops:  d.Stream.LocalDrops,
			Bursts: jsonBursts{
				One:    d.Stream.Bursts.One,
				Two:    d.Stream.Bursts.Two,
				Three9: d.Stream.Bursts.Three9,
				TenUp:  d.Stream.Bursts.Ten99 + d.Stream.Bursts.HundredUp,
				Max:    d.Stream.Bursts.Max,
			},
			RateTxBPS: d.Stream.RateTxBPS,
			RateRxBPS: d.Stream.RateRxBPS,
		},
		ReverseStream:  d.ReverseStream,
		TCPCorroborate: d.TCPCorroborate,
		Correlation:    jsonCorrelation{SuspectHops: suspectHopsToJSON(d.Correlation)},
		Timeline:       make([]jsonTimelineRow, 0, len(d.Timeline)),
	}
	for _, b := range d.Timeline {
		doc.Timeline = append(doc.Timeline, jsonTimelineRow{
			T:          b.T,
			StreamLoss: b.StreamLost,
			Hops:       hopViewsToJSON(b.Hops),
		})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(doc)
}

// suspectHopsToJSON converts the renderer's SuspectHopReport list to the
// JSON wire shape. Returns an empty (non-nil) slice when src is empty so
// the JSON output stays `"suspect_hops": []` rather than `"suspect_hops":
// null` — consumers like jq treat the two differently.
func suspectHopsToJSON(src []SuspectHopReport) []jsonSuspectHop {
	out := make([]jsonSuspectHop, len(src))
	for i, h := range src {
		out[i] = jsonSuspectHop{
			TTL:        h.TTL,
			Confidence: h.Confidence,
			Evidence:   h.Evidence,
		}
	}
	return out
}

// hopViewsToJSON converts an agg.HopView slice (from a per-second
// timeline bucket) into the JSON hop shape. Mirrors hopsToJSON's
// empty-slice contract for a stable JSON shape.
func hopViewsToJSON(src []agg.HopView) []jsonHop {
	out := make([]jsonHop, len(src))
	for i, h := range src {
		out[i] = jsonHop{
			TTL:     h.TTL,
			IP:      h.IP,
			Sent:    h.Sent,
			Recv:    h.Recv,
			LossPct: h.LossPct,
			RTTMS: jsonRTT{
				Last:   h.LastRTTMS,
				Best:   h.BestRTTMS,
				Worst:  h.WorstRTTMS,
				Avg:    h.AvgRTTMS,
				StdDev: h.StdDevRTTMS,
			},
			// Suspect on per-bucket hops is left false — the suspect
			// flag attaches to the overall hop in d.Hops, not the
			// per-bucket snapshot rows. This keeps the timeline a pure
			// observation log rather than a re-rendering of the verdict.
			Suspect: false,
		}
	}
	return out
}

// reverseHopsToJSON converts the renderer's ReverseHopReport list to the
// JSON wire shape. Returns an empty (non-nil) slice when src is empty so
// the JSON output stays `"reverse_path": []` rather than `"reverse_path":
// null`.
func reverseHopsToJSON(src []ReverseHopReport) []jsonReverseHop {
	out := make([]jsonReverseHop, len(src))
	for i, h := range src {
		out[i] = jsonReverseHop{
			TTL:     h.TTL,
			Addr:    h.Addr,
			Sent:    h.Sent,
			Recv:    h.Recv,
			LossPct: h.LossPct,
			RTTMS: jsonRTT{
				Last:   h.RTTMS,
				Best:   h.BestRTTMS,
				Worst:  h.WorstRTTMS,
				Avg:    h.AvgRTTMS,
				StdDev: h.StdDevRTTMS,
			},
			Terminus: h.Terminus,
		}
	}
	return out
}

// hopsToJSON converts the renderer's HopReport list to the spec wire
// shape. Returns an empty (non-nil) slice when src is empty so the
// JSON output stays `"hops": []` rather than `"hops": null`.
func hopsToJSON(src []HopReport) []jsonHop {
	out := make([]jsonHop, len(src))
	for i, h := range src {
		out[i] = jsonHop{
			TTL:     h.TTL,
			IP:      h.IP,
			Sent:    h.Sent,
			Recv:    h.Recv,
			LossPct: h.LossPct,
			RTTMS: jsonRTT{
				Last:   h.LastRTTMS,
				Best:   h.BestRTTMS,
				Worst:  h.WorstRTTMS,
				Avg:    h.AvgRTTMS,
				StdDev: h.StdDevRTTMS,
			},
			Suspect: h.Suspect,
		}
	}
	return out
}
