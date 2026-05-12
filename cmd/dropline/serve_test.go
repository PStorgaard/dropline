package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PStorgaard/dropline/internal/control"
	"github.com/PStorgaard/dropline/internal/probe"
	"github.com/PStorgaard/dropline/internal/stream"
)

func parseServe(t *testing.T, args []string) serveConfig {
	t.Helper()
	cfg, err := parseServeArgs(args, io.Discard)
	if err != nil {
		t.Fatalf("parseServeArgs(%v): %v", args, err)
	}
	return cfg
}

func parseServeErr(t *testing.T, args []string) error {
	t.Helper()
	_, err := parseServeArgs(args, io.Discard)
	if err == nil {
		t.Fatalf("parseServeArgs(%v): expected error, got nil", args)
	}
	return err
}

func TestParseServeArgsDefaults(t *testing.T) {
	cfg := parseServe(t, nil)
	if cfg.listen != ":5301" {
		t.Errorf("listen = %q, want :5301", cfg.listen)
	}
	if cfg.maxRateBPS != 1_000_000_000 {
		t.Errorf("maxRateBPS = %d, want 1_000_000_000 (1G default)", cfg.maxRateBPS)
	}
	if !cfg.allowReverseStream {
		t.Errorf("allowReverseStream = false, want true (default)")
	}
}

func TestParseServeArgsMaxRateBPS(t *testing.T) {
	cases := []struct {
		raw  string
		want int64
	}{
		{"50M", 50_000_000},
		{"1G", 1_000_000_000},
		{"0", 0},
		{"unlimited", 0},
		{"UNLIMITED", 0},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			cfg := parseServe(t, []string{"--max-rate-bps", tc.raw})
			if cfg.maxRateBPS != tc.want {
				t.Errorf("maxRateBPS = %d, want %d", cfg.maxRateBPS, tc.want)
			}
		})
	}
}

func TestParseServeArgsAllowReverseStream(t *testing.T) {
	cfg := parseServe(t, []string{"--allow-reverse-stream=false"})
	if cfg.allowReverseStream {
		t.Errorf("--allow-reverse-stream=false: got true, want false")
	}
	cfg = parseServe(t, []string{"--allow-reverse-stream=true"})
	if !cfg.allowReverseStream {
		t.Errorf("--allow-reverse-stream=true: got false, want true")
	}
}

func TestParseServeArgsListenOverride(t *testing.T) {
	cfg := parseServe(t, []string{"--listen", "127.0.0.1:0"})
	if cfg.listen != "127.0.0.1:0" {
		t.Errorf("listen = %q, want 127.0.0.1:0", cfg.listen)
	}
}

func TestParseServeArgsErrors(t *testing.T) {
	cases := [][]string{
		{"--listen", ""},
		{"--unknown"},
		{"positional"},
		{"--listen", ":5301", "extra"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			_ = parseServeErr(t, args)
		})
	}
}

func TestValidateHello(t *testing.T) {
	good := &control.Hello{Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 1000, PacketSize: 1200, FlowID: 1, Token: 0xABCDEF12}
	if reason := validateHello(good, 0); reason != "" {
		t.Fatalf("good hello rejected: %s", reason)
	}
	cases := []struct {
		name   string
		mutate func(*control.Hello)
		want   string
	}{
		{"version", func(h *control.Hello) { h.Version = 2 }, "version"},
		{"mode", func(h *control.Hello) { h.Mode = "throughput" }, "mode"},
		{"packet_size_low", func(h *control.Hello) { h.PacketSize = 16 }, "packet_size"},
		{"packet_size_high", func(h *control.Hello) { h.PacketSize = 70_000 }, "packet_size"},
		{"duration_zero", func(h *control.Hello) { h.DurationMS = 0 }, "duration_ms"},
		{"duration_over_cap", func(h *control.Hello) { h.DurationMS = maxSessionDurationMS + 1 }, "exceeds server cap"},
		{"rate_zero", func(h *control.Hello) { h.RateBPS = 0 }, "rate_bps"},
		{"mtr_below_floor", func(h *control.Hello) { h.MTRIntervalMS = minMTRIntervalMS - 1 }, "mtr_interval_ms"},
		{"mtr_above_ceiling", func(h *control.Hello) { h.MTRIntervalMS = maxMTRIntervalMS + 1 }, "mtr_interval_ms"},
		{"tcp_corroborate_negative", func(h *control.Hello) { h.TCPCorroborateRateBPS = -1 }, "tcp_corroborate_rate_bps"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := *good
			tc.mutate(&h)
			reason := validateHello(&h, 0)
			if !strings.Contains(reason, tc.want) {
				t.Errorf("reason=%q, want substring %q", reason, tc.want)
			}
		})
	}
}

// TestValidateHelloRateCap covers the server-side --max-rate-bps gate.
// maxRateBPS=0 must accept any positive rate (cap disabled); a positive
// cap rejects rates above it with the documented error substring.
func TestValidateHelloRateCap(t *testing.T) {
	good := &control.Hello{Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 10_000_000, DurationMS: 1000, PacketSize: 1200, FlowID: 1, Token: 0xABCDEF12}

	if reason := validateHello(good, 0); reason != "" {
		t.Errorf("cap=0 should disable the limit; got %q", reason)
	}
	if reason := validateHello(good, 100_000_000); reason != "" {
		t.Errorf("rate under cap rejected: %q", reason)
	}
	over := *good
	over.RateBPS = 200_000_000
	reason := validateHello(&over, 100_000_000)
	if !strings.Contains(reason, "exceeds server cap") {
		t.Errorf("over-cap rate: reason=%q, want substring \"exceeds server cap\"", reason)
	}

	// TCPCorroborateRateBPS must obey the same per-session cap as RateBPS.
	tcpOver := *good
	tcpOver.TCPCorroborateRateBPS = 200_000_000
	reason = validateHello(&tcpOver, 100_000_000)
	if !strings.Contains(reason, "tcp_corroborate_rate_bps") || !strings.Contains(reason, "exceeds server cap") {
		t.Errorf("tcp-corroborate over-cap: reason=%q, want substring \"tcp_corroborate_rate_bps … exceeds server cap\"", reason)
	}
	// Zero is "disabled" and must pass even with a tight cap.
	zero := *good
	zero.TCPCorroborateRateBPS = 0
	if reason := validateHello(&zero, 100_000_000); reason != "" {
		t.Errorf("tcp-corroborate zero (disabled) rejected with cap: %q", reason)
	}

	// When the client opted out via TCPCorroborate="off", the rate
	// field is meaningless and must not gate admission even if it
	// exceeds the cap (legacy clients populated this field
	// unconditionally; a low --max-rate-bps shouldn't reject them).
	offOverCap := *good
	offOverCap.TCPCorroborate = "off"
	offOverCap.TCPCorroborateRateBPS = 200_000_000
	if reason := validateHello(&offOverCap, 100_000_000); reason != "" {
		t.Errorf("tcp-corroborate=off with over-cap rate rejected: %q", reason)
	}
	// Regression guard: when corroboration is requested, the cap
	// still applies even if the explicit "on" string is present.
	onOverCap := *good
	onOverCap.TCPCorroborate = "on"
	onOverCap.TCPCorroborateRateBPS = 200_000_000
	if reason := validateHello(&onOverCap, 100_000_000); !strings.Contains(reason, "tcp_corroborate_rate_bps") {
		t.Errorf("tcp-corroborate=on with over-cap rate accepted: reason=%q", reason)
	}
}

// TestValidateHelloMTRBounds covers the MTR-interval bounds. Zero is the
// legacy "no field" signal and must pass; the floor and ceiling are
// inclusive, anything outside is rejected.
func TestValidateHelloMTRBounds(t *testing.T) {
	base := &control.Hello{Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 1000, PacketSize: 1200, FlowID: 1, Token: 0xABCDEF12}

	cases := []struct {
		name   string
		ms     int64
		reject bool
	}{
		{"zero_legacy", 0, false},
		{"below_floor", minMTRIntervalMS - 1, true},
		{"at_floor", minMTRIntervalMS, false},
		{"at_ceiling", maxMTRIntervalMS, false},
		{"above_ceiling", maxMTRIntervalMS + 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := *base
			h.MTRIntervalMS = tc.ms
			reason := validateHello(&h, 0)
			if tc.reject && reason == "" {
				t.Errorf("MTRIntervalMS=%d: expected rejection, got pass", tc.ms)
			}
			if !tc.reject && reason != "" {
				t.Errorf("MTRIntervalMS=%d: unexpected rejection: %q", tc.ms, reason)
			}
		})
	}
}

// TestBuildFinalCopiesLocalDrops verifies the receiver's LocalDrops counter
// reaches the wire. Receiver overload creates seq gaps that look like
// network loss in the aggregator; LocalDrops on the wire is the only signal
// the renderer has to mark the verdict suspect.
func TestBuildFinalCopiesLocalDrops(t *testing.T) {
	hello := &control.Hello{Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 1000, PacketSize: 1200, FlowID: 1, Token: 0xABCDEF12}
	snap := stream.Snapshot{
		Recv:        42,
		Lost:        7,
		LocalDrops:  3,
		KernelDrops: 1,
		T:           1.0,
		MaxSeq:      48,
	}
	fs := buildFinal(hello, snap, nil, nil)
	if fs.Stats.LocalDrops != 3 {
		t.Errorf("LocalDrops not propagated: got %d, want 3", fs.Stats.LocalDrops)
	}
	if fs.Stats.KernelDrops != 1 {
		t.Errorf("KernelDrops regression: got %d, want 1", fs.Stats.KernelDrops)
	}
}

func TestMintSessionIDIsHex(t *testing.T) {
	a, err := mintSessionID()
	if err != nil {
		t.Fatalf("mintSessionID: %v", err)
	}
	if len(a) != 16 {
		t.Errorf("len = %d, want 16", len(a))
	}
	for _, r := range a {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			t.Errorf("non-hex rune in id: %q", a)
			break
		}
	}
	b, _ := mintSessionID()
	if a == b {
		t.Errorf("two ids collided: %q == %q", a, b)
	}
}

// startServer wires up a server on loopback and returns the bound TCP
// address, the bound UDP address, and a teardown func. maxSessions of 0
// uses the default of 4. The server keeps running until ctx is canceled.
// maxRateBPS defaults to 0 (unlimited) and reverse stream is allowed.
func startServer(t *testing.T, ctx context.Context, maxSessions int) (tcpAddr string, udpAddr *net.UDPAddr, teardown func()) {
	t.Helper()
	return startServerWith(t, ctx, maxSessions, 0, true)
}

// startServerWith is the configurable form used by tests that exercise
// --max-rate-bps and --allow-reverse-stream.
func startServerWith(t *testing.T, ctx context.Context, maxSessions int, maxRateBPS int64, allowReverseStream bool) (tcpAddr string, udpAddr *net.UDPAddr, teardown func()) {
	t.Helper()
	if maxSessions == 0 {
		maxSessions = 4
	}
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = udpConn.Close()
		t.Fatalf("Listen tcp: %v", err)
	}
	hub := stream.NewHub(udpConn)
	hubDone := make(chan struct{})
	go func() {
		defer close(hubDone)
		_ = hub.Run(ctx)
	}()
	// Tests run unprivileged, so reverseCapable is false — exercises the
	// "Ready.ReverseTrace=off" path and avoids opening a raw ICMP socket
	// that the test environment can't grant.
	reg := newSessionRegistry()
	srv := &control.Server{
		Handler:      newServeHandler(hub, false, maxSessions, maxRateBPS, allowReverseStream, reg),
		ProbeHandler: newProbeHandler(reg, maxSessions),
	}
	go func() { _ = srv.Serve(ctx, tcpLn) }()

	teardown = func() {
		_ = tcpLn.Close()
		_ = udpConn.Close()
		<-hubDone
	}
	return tcpLn.Addr().String(), udpConn.LocalAddr().(*net.UDPAddr), teardown
}

func TestServeRejectsBadHello(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServer(t, ctx, 0)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Send(&control.Hello{
		Type: control.TypeHello, Version: 99, Mode: "loss",
		RateBPS: 1, DurationMS: 1, PacketSize: 64, FlowID: 1, Token: 0xABCDEF12,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	e, ok := msg.(*control.Error)
	if !ok {
		t.Fatalf("expected Error, got %#v", msg)
	}
	if !strings.Contains(e.Reason, "version") {
		t.Errorf("reason=%q, want substring \"version\"", e.Reason)
	}
}

func TestServeHappyPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, udpAddr, teardown := startServer(t, ctx, 0)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	const flowID uint32 = 0x5A5A1234
	const packetSize = 64
	hello := &control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS:    int64(packetSize) * 8 * 100, // 100 pps
		DurationMS: 250,
		PacketSize: packetSize,
		FlowID:     flowID,
		Token:      0xABCDEF12,
	}
	if err := c.Send(hello); err != nil {
		t.Fatalf("Send hello: %v", err)
	}

	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv ready: %v", err)
	}
	ready, ok := msg.(*control.Ready)
	if !ok {
		t.Fatalf("expected Ready, got %#v", msg)
	}
	if ready.SessionID == "" {
		t.Errorf("empty session_id")
	}

	// Push a handful of UDP packets at the receiver while the test runs.
	sender, err := net.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sender.Close() }()
	buf := make([]byte, packetSize)
	for i := uint64(0); i < 5; i++ {
		stream.EncodeHeader(buf, stream.Header{
			Magic: stream.Magic, FlowID: flowID, Seq: i, TxUnixNS: time.Now().UnixNano(), Token: 0xABCDEF12,
		})
		if _, err := sender.Write(buf); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Read messages until Final arrives or we exhaust a budget.
	if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	var final *control.Final
	for final == nil {
		m, err := c.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		switch v := m.(type) {
		case *control.Stats:
			// optional; depends on whether we crossed a 1s tick
		case *control.Final:
			final = v
		default:
			t.Fatalf("unexpected message %#v", m)
		}
	}

	if final.Stats.Recv != 5 {
		t.Errorf("recv: want 5, got %d", final.Stats.Recv)
	}
	if final.Stats.RateTxBPS != hello.RateBPS {
		t.Errorf("rate_tx_bps: want echoed %d, got %d", hello.RateBPS, final.Stats.RateTxBPS)
	}
	if final.Stats.DurationS <= 0 {
		t.Errorf("duration_s: want > 0, got %f", final.Stats.DurationS)
	}
}

// runOneSession is a small helper for the multi-session tests: it dials
// the server with the given Hello, asserts a Ready, then drives a few
// UDP packets through the bound socket and reads until Final arrives.
// Returns the recv count from the Final message.
func runOneSession(t *testing.T, ctx context.Context, tcpAddr string, udpAddr *net.UDPAddr, flowID uint32, packets int) int64 {
	t.Helper()
	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	const packetSize = 64
	hello := &control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS:    int64(packetSize) * 8 * 100,
		DurationMS: 500,
		PacketSize: packetSize,
		FlowID:     flowID,
		Token:      0xABCDEF12,
	}
	if err := c.Send(hello); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv ready: %v", err)
	}
	if _, ok := msg.(*control.Ready); !ok {
		t.Fatalf("expected Ready, got %#v", msg)
	}

	sender, err := net.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sender.Close() }()
	buf := make([]byte, packetSize)
	for i := uint64(0); i < uint64(packets); i++ {
		stream.EncodeHeader(buf, stream.Header{
			Magic: stream.Magic, FlowID: flowID, Seq: i, TxUnixNS: time.Now().UnixNano(), Token: 0xABCDEF12,
		})
		if _, err := sender.Write(buf); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	for {
		m, err := c.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if f, ok := m.(*control.Final); ok {
			return f.Stats.Recv
		}
	}
}

// Two clients run concurrently with distinct flow_ids; each must see only
// its own packets through the hub demuxer.
func TestServeRunsConcurrentSessions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, udpAddr, teardown := startServer(t, ctx, 4)
	defer teardown()

	type result struct{ flow uint32; recv int64 }
	results := make(chan result, 2)
	for _, fid := range []uint32{0xAAAA0001, 0xBBBB0002} {
		fid := fid
		go func() {
			recv := runOneSession(t, ctx, tcpAddr, udpAddr, fid, 7)
			results <- result{flow: fid, recv: recv}
		}()
	}
	got := map[uint32]int64{}
	for i := 0; i < 2; i++ {
		select {
		case r := <-results:
			got[r.flow] = r.recv
		case <-time.After(5 * time.Second):
			t.Fatalf("session %d did not complete within 5s; got=%+v", i, got)
		}
	}
	for fid, recv := range got {
		if recv != 7 {
			t.Errorf("flow %x: recv = %d, want 7 (no cross-flow bleed)", fid, recv)
		}
	}
}

// max-sessions=1 → second concurrent client gets the over-cap rejection.
func TestServeRejectsOverMaxSessions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServer(t, ctx, 1)
	defer teardown()

	// First session: long-running so it occupies the only slot.
	c1, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial 1: %v", err)
	}
	defer func() { _ = c1.Close() }()
	if err := c1.Send(&control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 5_000, PacketSize: 64, FlowID: 1, Token: 0xABCDEF12,
	}); err != nil {
		t.Fatalf("Send 1: %v", err)
	}
	if _, err := c1.Recv(); err != nil {
		t.Fatalf("Recv 1 ready: %v", err)
	}

	// Second session: must be rejected with "max sessions reached".
	c2, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial 2: %v", err)
	}
	defer func() { _ = c2.Close() }()
	if err := c2.Send(&control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 100, PacketSize: 64, FlowID: 2, Token: 0xABCDEF12,
	}); err != nil {
		t.Fatalf("Send 2: %v", err)
	}
	if err := c2.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	msg, err := c2.Recv()
	if err != nil {
		t.Fatalf("Recv 2: %v", err)
	}
	e, ok := msg.(*control.Error)
	if !ok {
		t.Fatalf("session 2 expected Error, got %#v", msg)
	}
	if !strings.Contains(e.Reason, "max sessions") {
		t.Errorf("reason=%q, want substring \"max sessions\"", e.Reason)
	}
}

func TestClassifyTerminus(t *testing.T) {
	clientIP := net.ParseIP("203.0.113.7").To4()
	cases := []struct {
		name string
		hop  probe.HopStat
		want string
	}{
		{"intermediate", probe.HopStat{TTL: 5, Addr: "10.0.0.1", Terminus: false}, ""},
		{"intermediate_no_addr", probe.HopStat{TTL: 5, Addr: "", Terminus: false}, ""},
		{"terminus_no_addr", probe.HopStat{TTL: 9, Addr: "", Terminus: true}, ""},
		{"host_match", probe.HopStat{TTL: 9, Addr: "203.0.113.7", Terminus: true}, "host"},
		{"nat_mismatch", probe.HopStat{TTL: 9, Addr: "198.51.100.1", Terminus: true}, "nat"},
		{"unparseable_addr", probe.HopStat{TTL: 9, Addr: "not-an-ip", Terminus: true}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyTerminus(tc.hop, clientIP)
			if got != tc.want {
				t.Errorf("classifyTerminus(%+v) = %q, want %q", tc.hop, got, tc.want)
			}
		})
	}
}

func TestReverseInterval(t *testing.T) {
	cases := []struct {
		ms   int64
		want time.Duration
	}{
		{0, time.Second},
		{-1, time.Second},
		{500, 500 * time.Millisecond},
		{2500, 2500 * time.Millisecond},
	}
	for _, tc := range cases {
		if got := reverseInterval(tc.ms); got != tc.want {
			t.Errorf("reverseInterval(%d) = %s, want %s", tc.ms, got, tc.want)
		}
	}
}


// Any client→server message after Hello must trigger the "fatal
// disconnect" path — no post-Hello traffic is expected.
func TestServeUnexpectedSecondMessageDisconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServer(t, ctx, 0)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	hello := &control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 3000, PacketSize: 64, FlowID: 1, Token: 0xABCDEF12,
	}
	if err := c.Send(hello); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	if _, err := c.Recv(); err != nil {
		t.Fatalf("Recv ready: %v", err)
	}

	// A second Hello is non-sensical mid-session; should disconnect.
	if err := c.Send(hello); err != nil {
		t.Fatalf("Send second hello: %v", err)
	}
	if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	for {
		msg, err := c.Recv()
		if err != nil {
			return // expected: connection closed
		}
		// A Final from cancellation may slip through; that's also
		// acceptable evidence of disconnect.
		if _, ok := msg.(*control.Final); ok {
			return
		}
	}
}

// Quick guard: returning early from validateHello when conditions are met
// avoids any subsequent panic on zero-valued fields.
func TestValidateHelloDoesNotPanicOnEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic: %v", r)
		}
	}()
	if reason := validateHello(&control.Hello{}, 0); reason == "" {
		t.Fatal("empty hello accepted; expected rejection")
	}
}

// A second client sending the same flow_id while the first is mid-test
// must get a clean Error frame back instead of crashing the server. Pre-
// fix Hub.Register panicked on collision, killing the whole srv.handle
// goroutine.
func TestServeRejectsDuplicateFlowID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServer(t, ctx, 4)
	defer teardown()

	const sharedFlow uint32 = 0xDEADBEEF

	// First session occupies sharedFlow for ~3s.
	c1, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial 1: %v", err)
	}
	defer func() { _ = c1.Close() }()
	if err := c1.Send(&control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 3_000, PacketSize: 64, FlowID: sharedFlow, Token: 0xABCDEF12,
	}); err != nil {
		t.Fatalf("Send 1: %v", err)
	}
	if _, err := c1.Recv(); err != nil {
		t.Fatalf("Recv 1 ready: %v", err)
	}

	// Second session tries the same flow_id — must get Error, not Ready.
	c2, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial 2: %v", err)
	}
	defer func() { _ = c2.Close() }()
	if err := c2.Send(&control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 100, PacketSize: 64, FlowID: sharedFlow, Token: 0xABCDEF12,
	}); err != nil {
		t.Fatalf("Send 2: %v", err)
	}
	if err := c2.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	msg, err := c2.Recv()
	if err != nil {
		t.Fatalf("Recv 2: %v", err)
	}
	e, ok := msg.(*control.Error)
	if !ok {
		t.Fatalf("session 2 expected Error, got %#v", msg)
	}
	if !strings.Contains(e.Reason, "flow_id collision") {
		t.Errorf("reason=%q, want substring \"flow_id collision\"", e.Reason)
	}

	// Server must still accept fresh sessions with a different flow_id.
	c3, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial 3: %v", err)
	}
	defer func() { _ = c3.Close() }()
	if err := c3.Send(&control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 100, PacketSize: 64, FlowID: sharedFlow + 1, Token: 0xABCDEF12,
	}); err != nil {
		t.Fatalf("Send 3: %v", err)
	}
	if err := c3.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetDeadline 3: %v", err)
	}
	m3, err := c3.Recv()
	if err != nil {
		t.Fatalf("Recv 3: %v", err)
	}
	if _, ok := m3.(*control.Ready); !ok {
		t.Fatalf("session 3 expected Ready, got %#v", m3)
	}
}

// A peer that aborts its read side after Ready should not cause the
// server's session to drag out for the full DurationMS. The stats
// forwarder must exit on the first Send error and call cancel() so
// sessionCtx tears the rest down and the maxSessions slot frees
// promptly. Verified directly via the slot-release probe: a fresh
// session must be admitted well before DurationMS expires.
func TestServeStatsForwarderBailsOnSendError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// maxSessions=1 — the only direct way to observe slot release is a
	// fresh Hello succeeding immediately after the prior session ends.
	tcpAddr, udpAddr, teardown := startServer(t, ctx, 1)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	// Not deferring Close — we close it explicitly below to trigger
	// the send-error / recv-EOF path while the session is mid-test.

	const flowID uint32 = 0xC0FFEE42
	const packetSize = 64
	// DurationMS large enough that, if the slot weren't released, the
	// fresh Hello below would block for the full duration; small enough
	// to bound test runtime when the fix regresses.
	const sessionDurationMS = 15_000
	hello := &control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS:    int64(packetSize) * 8 * 100,
		DurationMS: sessionDurationMS,
		PacketSize: packetSize,
		FlowID:     flowID,
		Token:      0xABCDEF12,
	}
	if err := c.Send(hello); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	if _, err := c.Recv(); err != nil {
		t.Fatalf("Recv ready: %v", err)
	}

	// Push a few packets so the stats forwarder has something to ship.
	sender, err := net.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sender.Close() }()
	buf := make([]byte, packetSize)
	for i := uint64(0); i < 5; i++ {
		stream.EncodeHeader(buf, stream.Header{
			Magic: stream.Magic, FlowID: flowID, Seq: i, TxUnixNS: time.Now().UnixNano(), Token: 0xABCDEF12,
		})
		if _, err := sender.Write(buf); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	start := time.Now()
	_ = c.Close()

	// Slot-release probe: a fresh control session must be admitted
	// promptly. If the prior session held its slot until DurationMS
	// (the regression we're guarding against), every attempt within the
	// 3s budget would come back as "server busy". Retry instead of
	// fatal-fail on the first Error — there is an inherent race between
	// the client-side close, the server detecting EOF / firing cancel,
	// handleSession unwinding through receiver+forwarder waits, and the
	// next session's accept+Hello round-trip. The bound this test
	// actually guards against is "the slot ever frees within 3s," not
	// "first c2 attempt is in the lead."
	deadline := time.Now().Add(3 * time.Second)
	var lastReason string
	for time.Now().Before(deadline) {
		dialCtx, dialCancel := context.WithDeadline(ctx, deadline)
		c2, err := control.Dial(dialCtx, tcpAddr)
		dialCancel()
		if err != nil {
			t.Fatalf("fresh Dial after peer close: %v (elapsed=%s)", err, time.Since(start))
		}
		hello2 := *hello
		hello2.FlowID = 0xC0FFEE43 // distinct from the prior session's flow
		if err := c2.Send(&hello2); err != nil {
			_ = c2.Close()
			t.Fatalf("Send hello2: %v", err)
		}
		if err := c2.SetDeadline(deadline); err != nil {
			_ = c2.Close()
			t.Fatalf("SetDeadline: %v", err)
		}
		msg, err := c2.Recv()
		if err != nil {
			_ = c2.Close()
			t.Fatalf("Recv ready2 after %s: %v", time.Since(start), err)
		}
		if _, ok := msg.(*control.Ready); ok {
			_ = c2.Close()
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Errorf("slot released too slowly: %s", elapsed)
			}
			return
		}
		if e, ok := msg.(*control.Error); ok {
			lastReason = e.Reason
			_ = c2.Close()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		_ = c2.Close()
		t.Fatalf("fresh session: expected Ready, got %#v", msg)
	}
	t.Fatalf("slot did not release within 3s after peer close (last reason: %q)", lastReason)
}

// TestServeRejectsRateOverCap drives the --max-rate-bps gate end-to-end:
// a client whose Hello.RateBPS exceeds the server's cap must receive an
// Error frame with the documented substring.
func TestServeRejectsRateOverCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServerWith(t, ctx, 0, 1_000_000, true)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Send(&control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 10_000_000, DurationMS: 1000, PacketSize: 64, FlowID: 1, Token: 0xABCDEF12,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	e, ok := msg.(*control.Error)
	if !ok {
		t.Fatalf("expected Error, got %#v", msg)
	}
	if !strings.Contains(e.Reason, "exceeds server cap") {
		t.Errorf("reason=%q, want substring \"exceeds server cap\"", e.Reason)
	}
}

// TestServeReverseStreamDisabledByFlag covers the --allow-reverse-stream
// kill switch: a server started with allowReverseStream=false must
// resolve every client's reverse stream to "off" regardless of what the
// client asks for, and the resulting Final must carry ReverseSent=0.
func TestServeReverseStreamDisabledByFlag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, udpAddr, teardown := startServerWith(t, ctx, 0, 0, false)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	const flowID uint32 = 0x7E5700A1
	const reverseFlowID uint32 = 0x7E5700A2
	const packetSize = 64
	hello := &control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS:       int64(packetSize) * 8 * 100,
		DurationMS:    300,
		PacketSize:    packetSize,
		FlowID:        flowID,
		ReverseStream: "on",
		ReverseFlowID: reverseFlowID,
		Token:         0xABCDEF12,
	}
	if err := c.Send(hello); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv ready: %v", err)
	}
	ready, ok := msg.(*control.Ready)
	if !ok {
		t.Fatalf("expected Ready, got %#v", msg)
	}
	if ready.ReverseStream != "off" {
		t.Errorf("Ready.ReverseStream = %q, want \"off\"", ready.ReverseStream)
	}
	if ready.ReverseFlowID != 0 {
		t.Errorf("Ready.ReverseFlowID = %d, want 0", ready.ReverseFlowID)
	}

	// Drive a couple of forward packets so the session runs naturally.
	sender, err := net.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = sender.Close() }()
	buf := make([]byte, packetSize)
	for i := uint64(0); i < 3; i++ {
		stream.EncodeHeader(buf, stream.Header{Magic: stream.Magic, FlowID: flowID, Seq: i, TxUnixNS: time.Now().UnixNano(), Token: 0xABCDEF12})
		if _, err := sender.Write(buf); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	for {
		m, err := c.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if f, ok := m.(*control.Final); ok {
			if f.Stats.ReverseSent != 0 {
				t.Errorf("Final.ReverseSent = %d, want 0 (reverse stream disabled)", f.Stats.ReverseSent)
			}
			return
		}
	}
}

// Sanity that net.Conn errors do not leak in unexpected ways.
func TestServeUnexpectedFirstMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServer(t, ctx, 0)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// Send a Stats first instead of Hello — control.Server replies with
	// Error{"expected hello"} (see internal/control/server.go).
	if err := c.Send(&control.Stats{Type: control.TypeStats, T: 0.5}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	msg, err := c.Recv()
	if err != nil {
		// Some platforms close the conn before the Error frame lands; either
		// outcome (Error or EOF/closed) demonstrates rejection.
		var ne net.Error
		if errors.As(err, &ne) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
			return
		}
		t.Fatalf("Recv: %v", err)
	}
	if _, ok := msg.(*control.Error); !ok {
		t.Fatalf("expected Error, got %#v", msg)
	}
}

// runServerLoops must couple the hub and TCP loops as one failure
// domain. The three cases below check the three ways that coupling can
// matter: (1) context cancellation tears down both, (2) the hub side
// failing cancels the TCP side and surfaces its error, (3) the TCP side
// failing cancels the hub side and surfaces its error. Each subcase
// must return promptly — the original bug was an indefinite block when
// only one loop exited.
func TestRunServerLoopsCoordinatedShutdown(t *testing.T) {
	const settle = 2 * time.Second

	t.Run("ctx_cancel_stops_both", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		hubExited, tcpExited := make(chan struct{}), make(chan struct{})
		hubRun := func(c context.Context) error {
			defer close(hubExited)
			<-c.Done()
			return nil
		}
		tcpServe := func(c context.Context) error {
			defer close(tcpExited)
			<-c.Done()
			return nil
		}
		done := make(chan error, 1)
		go func() { done <- runServerLoops(ctx, hubRun, tcpServe) }()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ctx cancel: want nil err, got %v", err)
			}
		case <-time.After(settle):
			t.Fatal("runServerLoops did not return after ctx cancel")
		}
		// Both loops must have observed cancellation.
		<-hubExited
		<-tcpExited
	})

	t.Run("hub_failure_cancels_tcp", func(t *testing.T) {
		want := errors.New("hub boom")
		tcpExited := make(chan struct{})
		hubRun := func(c context.Context) error {
			return want
		}
		tcpServe := func(c context.Context) error {
			defer close(tcpExited)
			<-c.Done()
			return nil
		}
		done := make(chan error, 1)
		go func() { done <- runServerLoops(context.Background(), hubRun, tcpServe) }()
		select {
		case err := <-done:
			if !errors.Is(err, want) {
				t.Fatalf("want %v, got %v", want, err)
			}
		case <-time.After(settle):
			t.Fatal("runServerLoops did not return after hub failure")
		}
		select {
		case <-tcpExited:
		default:
			t.Fatal("tcp loop was not cancelled by hub failure")
		}
	})

	t.Run("tcp_failure_cancels_hub", func(t *testing.T) {
		want := errors.New("accept boom")
		hubExited := make(chan struct{})
		hubRun := func(c context.Context) error {
			defer close(hubExited)
			<-c.Done()
			return nil
		}
		tcpServe := func(c context.Context) error {
			return want
		}
		done := make(chan error, 1)
		go func() { done <- runServerLoops(context.Background(), hubRun, tcpServe) }()
		select {
		case err := <-done:
			if !errors.Is(err, want) {
				t.Fatalf("want %v, got %v", want, err)
			}
		case <-time.After(settle):
			t.Fatal("runServerLoops did not return after tcp failure")
		}
		select {
		case <-hubExited:
		default:
			t.Fatal("hub loop was not cancelled by tcp failure")
		}
	})
}

// A TCP probe connection targeting a live session that negotiated
// tcp_corroborate="off" must be rejected with a "not negotiated" Error,
// independent of whether the SessionID matches a live session. This
// guards the fix for defect 10b: per-session policy is the source of
// truth, not just the existence of an active session_id.
func TestServeRejectsProbeWhenNotNegotiated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServer(t, ctx, 0)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Send(&control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 5_000, PacketSize: 64, FlowID: 0xDEADBEEF, Token: 0xABCDEF12,
		TCPCorroborate: "off",
	}); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv ready: %v", err)
	}
	ready, ok := msg.(*control.Ready)
	if !ok {
		t.Fatalf("expected Ready, got %#v", msg)
	}
	if ready.TCPCorroborate != "off" {
		t.Fatalf("server resolved TCPCorroborate=%q, want \"off\"", ready.TCPCorroborate)
	}
	sid := ready.SessionID

	// Open the second TCP — the would-be probe — directly via net.Dial,
	// since this is exercising the server's probe-dispatch path, not the
	// client wrapper.
	probeNC, err := net.Dial("tcp4", tcpAddr)
	if err != nil {
		t.Fatalf("probe Dial: %v", err)
	}
	defer func() { _ = probeNC.Close() }()
	if err := control.WriteMessage(probeNC, &control.TCPProbe{
		Type: control.TypeTCPProbe, SessionID: sid, RateBPS: 1_000,
	}); err != nil {
		t.Fatalf("WriteMessage TCPProbe: %v", err)
	}
	_ = probeNC.SetReadDeadline(time.Now().Add(2 * time.Second))
	reply, err := control.ReadMessage(probeNC)
	if err != nil {
		t.Fatalf("ReadMessage on probe: %v", err)
	}
	e, ok := reply.(*control.Error)
	if !ok {
		t.Fatalf("expected Error reply, got %#v", reply)
	}
	if !strings.Contains(e.Reason, "not negotiated") {
		t.Errorf("reason=%q, want substring \"not negotiated\"", e.Reason)
	}
}

// A TCP probe must terminate promptly when its session ends, rather
// than idling on the flat probeMaxLifetime ceiling (5 min). With
// DurationMS short, the probe socket must be closed by the server
// well before probeMaxLifetime expires.
func TestServeProbeTerminatesWithSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServer(t, ctx, 0)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	const durationMS = 500
	if err := c.Send(&control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: durationMS, PacketSize: 64, FlowID: 0xCAFEBABE, Token: 0xABCDEF12,
		TCPCorroborate: "on",
	}); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv ready: %v", err)
	}
	ready, ok := msg.(*control.Ready)
	if !ok {
		t.Fatalf("expected Ready, got %#v", msg)
	}
	if ready.TCPCorroborate != "on" {
		t.Fatalf("server resolved TCPCorroborate=%q, want \"on\"", ready.TCPCorroborate)
	}

	probeNC, err := net.Dial("tcp4", tcpAddr)
	if err != nil {
		t.Fatalf("probe Dial: %v", err)
	}
	defer func() { _ = probeNC.Close() }()
	if err := control.WriteMessage(probeNC, &control.TCPProbe{
		Type: control.TypeTCPProbe, SessionID: ready.SessionID, RateBPS: 1_000,
	}); err != nil {
		t.Fatalf("WriteMessage TCPProbe: %v", err)
	}

	start := time.Now()
	// Block on Read; the server must close the probe socket when the
	// session's ctx cancels (DurationMS expiry). 2s budget = 4× the
	// session duration; the old probeMaxLifetime-only behavior would
	// have taken 5 minutes.
	_ = probeNC.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := probeNC.Read(buf); err == nil {
		t.Fatalf("probe Read returned no error; server should have closed the conn")
	}
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("probe close took %s, expected within ~1s of session end (DurationMS=%dms)", elapsed, durationMS)
	}
}

// TestValidateHelloRequiresToken pins finding-8 hardening at the
// admission boundary: a Hello with Token==0 (pre-feature client, or
// a token-stripping middlebox) is rejected up front with the documented
// reason, rather than silently degrading to v1-era integrity.
func TestValidateHelloRequiresToken(t *testing.T) {
	h := &control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 1000, PacketSize: 1200, FlowID: 1,
		// Token deliberately omitted (zero value).
	}
	reason := validateHello(h, 0)
	if !strings.Contains(reason, "token") {
		t.Fatalf("reason=%q, want substring \"token\"", reason)
	}
	// Sanity: same Hello with a non-zero Token passes.
	h.Token = 0x1
	if reason := validateHello(h, 0); reason != "" {
		t.Fatalf("Token=1 rejected: %q", reason)
	}
}

// TestServeRejectsProbeFromDifferentIP exercises finding-7's IP-binding
// gate: a probe dial from a source IP that differs from the control
// conn's source must be rejected with the documented reason.
//
// The test dials the control channel from 127.0.0.1 (default loopback)
// and the probe from 127.0.0.2. On most platforms 127.0.0.0/8 is wholly
// loopback so the LocalAddr bind succeeds; if it doesn't, the test
// skips rather than fail spuriously.
func TestServeRejectsProbeFromDifferentIP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpAddr, _, teardown := startServer(t, ctx, 0)
	defer teardown()

	c, err := control.Dial(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("Dial control: %v", err)
	}
	defer func() { _ = c.Close() }()

	hello := &control.Hello{
		Type: control.TypeHello, Version: 1, Mode: "loss",
		RateBPS: 1_000_000, DurationMS: 3_000, PacketSize: 64, FlowID: 0xAA,
		TCPCorroborate: "on", TCPCorroborateRateBPS: 1_000_000,
		Token: 0xABCDEF12,
	}
	if err := c.Send(hello); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	ready, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv ready: %v", err)
	}
	r, ok := ready.(*control.Ready)
	if !ok {
		t.Fatalf("expected Ready, got %#v", ready)
	}

	// Dial probe from 127.0.0.2 — different source IP than control (127.0.0.1).
	dialer := net.Dialer{
		LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 2)},
		Timeout:   2 * time.Second,
	}
	probeNC, err := dialer.DialContext(ctx, "tcp4", tcpAddr)
	if err != nil {
		t.Skipf("cannot bind local 127.0.0.2 on this host: %v", err)
	}
	defer func() { _ = probeNC.Close() }()

	if err := control.WriteMessage(probeNC, &control.TCPProbe{
		Type: control.TypeTCPProbe, SessionID: r.SessionID, RateBPS: 1_000_000,
	}); err != nil {
		t.Fatalf("Write probe: %v", err)
	}

	if err := probeNC.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	msg, err := control.ReadMessage(probeNC)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	e, ok := msg.(*control.Error)
	if !ok {
		t.Fatalf("expected Error, got %#v", msg)
	}
	if !strings.Contains(e.Reason, "source IP") {
		t.Errorf("reason=%q, want substring \"source IP\"", e.Reason)
	}
}

// TestRunServerLoopsCapturesFirstErrorOnRace is the regression guard for
// finding 6: the pre-fix atomic.Value would panic when two goroutines
// raced to store errors of different concrete types. The post-fix
// sync.Once path captures the first one and serializes; the test fires
// both arms with distinct concrete error types and expects no panic and
// a returned error from {err1, err2}. Run with -race for cross-coverage.
func TestRunServerLoopsCapturesFirstErrorOnRace(t *testing.T) {
	err1 := &net.OpError{Op: "read", Err: errors.New("hub-side")}
	err2 := errors.New("tcp-serve-side") // *errors.errorString — distinct type
	for i := 0; i < 200; i++ {
		hubRun := func(context.Context) error { return err1 }
		tcpServe := func(context.Context) error { return err2 }
		got := runServerLoops(context.Background(), hubRun, tcpServe)
		if got == nil {
			t.Fatalf("iter %d: got nil, want one of {err1, err2}", i)
		}
		if got != err1 && got != err2 {
			t.Fatalf("iter %d: got %v, want one of {err1, err2}", i, got)
		}
	}
}

// TestRunServerLoopsIgnoresContextCanceled pins that a clean shutdown
// path (both arms returning context.Canceled) yields nil — the
// non-context-error filter must not promote a Canceled into firstErr.
func TestRunServerLoopsIgnoresContextCanceled(t *testing.T) {
	hubRun := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	tcpServe := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	if err := runServerLoops(ctx, hubRun, tcpServe); err != nil {
		t.Fatalf("clean cancel returned err: %v", err)
	}
}
