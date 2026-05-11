package control

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   Message
	}{
		{"hello", &Hello{Type: TypeHello, Version: 1, Mode: "loss", RateBPS: 10_000_000, DurationMS: 60_000, PacketSize: 1200, FlowID: 2847391057, MTRIntervalMS: 2500}},
		{"hello_reverse_stream", &Hello{
			Type: TypeHello, Version: 1, Mode: "loss", RateBPS: 5_000_000, DurationMS: 60_000,
			PacketSize: 1200, FlowID: 11, ReverseStream: "auto", ReverseFlowID: 22,
			TCPCorroborate: "auto", TCPCorroborateRateBPS: 100_000,
		}},
		{"ready", &Ready{Type: TypeReady, SessionID: "abc-123"}},
		{"ready_reverse_stream", &Ready{
			Type: TypeReady, SessionID: "xyz-987",
			ReverseStream: "on", ReverseFlowID: 22, TCPCorroborate: "on",
		}},
		{"error", &Error{Type: TypeError, Reason: "server busy"}},
		{"stats", &Stats{Type: TypeStats, T: 12.0, Recv: 12450, Loss: 3, JitterMS: 0.41, KernelDrops: 0}},
		{"final", &Final{Type: TypeFinal, Stats: FinalStats{Recv: 600000, Loss: 12, JitterMS: 0.5, KernelDrops: 0, DurationS: 60.0}}},
		{"final_with_reverse_and_tcp", &Final{Type: TypeFinal, Stats: FinalStats{
			Recv: 60000, Loss: 12, DurationS: 60.0,
			ReverseSent: 60000, ReverseDurationS: 60.0,
			TCPBytesRetrans: 12345, TCPBytesOut: 7_500_000,
		}}},
		{"pause_true", &Pause{Type: TypePause, Paused: true}},
		{"pause_false", &Pause{Type: TypePause, Paused: false}},
		{"tcp_probe", &TCPProbe{Type: TypeTCPProbe, SessionID: "abc-123", RateBPS: 100_000}},
		{"reverse_hop_update", &ReverseHopUpdate{
			Type: TypeReverseHopUpdate, TTL: 7, Addr: "203.0.113.4",
			RTTMS: 14.2, LossPct: 0.0, Terminus: "host",
			Sent: 12, Recv: 12, BestRTTMS: 12.1, WorstRTTMS: 18.4,
			AvgRTTMS: 14.0, StdDevRTTMS: 1.5, BaselineRTTMS: 13.8,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteMessage(&buf, tc.in); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			got, err := ReadMessage(&buf)
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			if !reflect.DeepEqual(tc.in, got) {
				t.Fatalf("round-trip mismatch:\n want %#v\n  got %#v", tc.in, got)
			}
		})
	}
}

func TestReadMessageUnknownType(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"type": "bogus"})
	var buf bytes.Buffer
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	_, err := ReadMessage(&buf)
	if !errors.Is(err, ErrUnknownType) {
		t.Fatalf("want ErrUnknownType, got %v", err)
	}
}

func TestReadMessageMalformedJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, []byte("{not json")); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	_, err := ReadMessage(&buf)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	var syn *json.SyntaxError
	if !errors.As(err, &syn) {
		t.Fatalf("want *json.SyntaxError in chain, got %v", err)
	}
}

// Hello.MTRIntervalMS is omitempty: an old client that never set it must
// not produce a `mtr_interval_ms` key on the wire, so existing servers
// don't have to special-case the absence of the field.
func TestHelloMTRIntervalOmitemptyZero(t *testing.T) {
	h := &Hello{Type: TypeHello, Version: 1, Mode: "loss", RateBPS: 1, DurationMS: 1, PacketSize: 64, FlowID: 1}
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if bytes.Contains(raw, []byte("mtr_interval_ms")) {
		t.Errorf("zero MTRIntervalMS should omit; got %s", raw)
	}
}

// And a non-zero MTRIntervalMS round-trips cleanly through the JSON form.
func TestHelloMTRIntervalRoundTrip(t *testing.T) {
	h := &Hello{Type: TypeHello, Version: 1, Mode: "loss", RateBPS: 1, DurationMS: 1, PacketSize: 64, FlowID: 1, MTRIntervalMS: 5000}
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"mtr_interval_ms":5000`)) {
		t.Errorf("missing mtr_interval_ms in %s", raw)
	}
	var got Hello
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.MTRIntervalMS != 5000 {
		t.Errorf("MTRIntervalMS round-trip = %d, want 5000", got.MTRIntervalMS)
	}
}

// ReverseHopUpdate's rolling-stat fields are omitempty: an old (slim)
// server's wire bytes must stay identical when none of the new fields
// are set, so existing clients see no change.
func TestReverseHopUpdateOmitemptyZero(t *testing.T) {
	u := &ReverseHopUpdate{
		Type: TypeReverseHopUpdate, TTL: 1, Addr: "10.0.0.1",
		RTTMS: 0.4, LossPct: 0,
	}
	raw, err := json.Marshal(u)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, key := range []string{
		`"sent"`, `"recv"`, `"rtt_ms_best"`, `"rtt_ms_worst"`,
		`"rtt_ms_avg"`, `"rtt_ms_stddev"`, `"rtt_ms_baseline"`,
	} {
		if bytes.Contains(raw, []byte(key)) {
			t.Errorf("zero rolling field %s should be omitted; got %s", key, raw)
		}
	}
}

// And the rich fields round-trip cleanly when populated.
func TestReverseHopUpdateRichRoundTrip(t *testing.T) {
	in := &ReverseHopUpdate{
		Type: TypeReverseHopUpdate, TTL: 5, Addr: "10.0.0.5",
		RTTMS: 5.0, LossPct: 0,
		Sent: 4, Recv: 4, BestRTTMS: 4.8, WorstRTTMS: 5.4,
		AvgRTTMS: 5.0, StdDevRTTMS: 0.2, BaselineRTTMS: 5.0,
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got ReverseHopUpdate
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, &got) {
		t.Fatalf("round-trip mismatch:\n want %#v\n  got %#v", in, &got)
	}
}

// Hello's new reverse-stream / tcp-corroborate fields are omitempty so an
// old client's wire bytes stay byte-identical (no surprise keys for
// pre-feature servers to special-case).
func TestHelloReverseStreamAndTCPOmitemptyZero(t *testing.T) {
	h := &Hello{Type: TypeHello, Version: 1, Mode: "loss", RateBPS: 1, DurationMS: 1, PacketSize: 64, FlowID: 1}
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, key := range []string{
		`"reverse_stream"`, `"reverse_flow_id"`,
		`"tcp_corroborate"`, `"tcp_corroborate_rate_bps"`,
	} {
		if bytes.Contains(raw, []byte(key)) {
			t.Errorf("zero %s should be omitted; got %s", key, raw)
		}
	}
}

// Ready's new fields are omitempty so an old server's wire bytes stay
// byte-identical to a new client reading them.
func TestReadyReverseStreamAndTCPOmitemptyZero(t *testing.T) {
	r := &Ready{Type: TypeReady, SessionID: "x"}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, key := range []string{`"reverse_stream"`, `"reverse_flow_id"`, `"tcp_corroborate"`} {
		if bytes.Contains(raw, []byte(key)) {
			t.Errorf("zero %s should be omitted; got %s", key, raw)
		}
	}
}

// FinalStats' new reverse / tcp-corroborate fields are omitempty so an
// old server's Final keeps byte-for-byte parity with the v1 schema.
func TestFinalStatsReverseAndTCPOmitemptyZero(t *testing.T) {
	s := &FinalStats{Sent: 1, Recv: 1, DurationS: 1.0}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, key := range []string{
		`"reverse_sent"`, `"reverse_duration_s"`,
		`"tcp_bytes_retrans"`, `"tcp_bytes_out"`,
	} {
		if bytes.Contains(raw, []byte(key)) {
			t.Errorf("zero %s should be omitted; got %s", key, raw)
		}
	}
}

// TCPProbe.RateBPS is omitempty: a client that doesn't advertise a rate
// produces no key, so future servers that ignore the field can stay
// agnostic to its presence.
func TestTCPProbeRateOmitemptyZero(t *testing.T) {
	p := &TCPProbe{Type: TypeTCPProbe, SessionID: "x"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if bytes.Contains(raw, []byte(`"rate_bps"`)) {
		t.Errorf("zero rate_bps should be omitted; got %s", raw)
	}
}

// sanity-check the wire format itself: 4-byte BE length + UTF-8 JSON.
func TestWireFormatBytes(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, &Ready{Type: TypeReady, SessionID: "x"}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	raw := buf.Bytes()
	if len(raw) < 4 {
		t.Fatalf("frame too short: %d", len(raw))
	}
	n := binary.BigEndian.Uint32(raw[:4])
	if int(n) != len(raw)-4 {
		t.Fatalf("length prefix %d does not match payload %d", n, len(raw)-4)
	}
	if !json.Valid(raw[4:]) {
		t.Fatalf("payload is not valid JSON: %q", raw[4:])
	}
}
