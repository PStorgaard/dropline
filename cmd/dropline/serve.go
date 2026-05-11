package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/PStorgaard/dropline/internal/control"
	"github.com/PStorgaard/dropline/internal/privcheck"
	"github.com/PStorgaard/dropline/internal/probe"
	"github.com/PStorgaard/dropline/internal/stream"
)

type serveConfig struct {
	listen      string
	maxSessions int
}

// maxSessionDurationMS caps Hello.DurationMS at admission time. The cap
// is server admission policy bounding the duration an idle client can
// hold a session slot; the default trace duration (60s) is 60× below.
const maxSessionDurationMS int64 = 60 * 60 * 1000

func parseServeArgs(args []string, errOut io.Writer) (serveConfig, error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(errOut)
	listen := fs.String("listen", ":5301", "address to bind for both TCP control and UDP stream")
	maxSessions := fs.Int("max-sessions", 4, "maximum concurrent client sessions")
	if err := fs.Parse(args); err != nil {
		return serveConfig{}, err
	}
	if fs.NArg() != 0 {
		return serveConfig{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *listen == "" {
		return serveConfig{}, errors.New("--listen must not be empty")
	}
	if *maxSessions < 1 {
		return serveConfig{}, fmt.Errorf("--max-sessions must be >= 1, got %d", *maxSessions)
	}
	return serveConfig{
		listen:      *listen,
		maxSessions: *maxSessions,
	}, nil
}

func runServe(args []string) {
	cfg, err := parseServeArgs(args, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dropline serve: %s\n", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serveRun(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "dropline serve: %s\n", err)
		os.Exit(1)
	}
}

// serveRun binds both sockets, prints a banner to stderr, and runs the
// control accept loop until ctx is canceled. Returns nil on graceful
// shutdown via ctx.
func serveRun(ctx context.Context, cfg serveConfig) error {
	udpAddr, err := net.ResolveUDPAddr("udp4", cfg.listen)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", cfg.listen, err)
	}
	udpConn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return fmt.Errorf("listen udp %s: %w", cfg.listen, err)
	}
	defer func() { _ = udpConn.Close() }()

	tcpLn, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen tcp %s: %w", cfg.listen, err)
	}
	defer func() { _ = tcpLn.Close() }()

	// Detect raw-ICMP capability once at startup. The server still starts
	// fine when capability is missing; the only effect is that reverse
	// trace will be advertised as "off" in every Ready, so clients that
	// asked for it know to surface the disabled_by_server status.
	rawICMP := privcheck.RawICMP()
	if rawICMP.OK {
		fmt.Fprintln(os.Stderr, "dropline serve: raw-ICMP available; reverse trace enabled")
	} else {
		fmt.Fprintf(os.Stderr, "dropline serve: reverse trace disabled (%s)\n", rawICMP.Reason)
	}

	// Best-effort warning when the kernel's rmem_max would cap the
	// receiver's SO_RCVBUF target — silent on non-Linux and on
	// /proc-read failures (see internal/stream/rmemmax_*).
	stream.WarnIfRmemMaxBelow(os.Stderr, stream.DefaultSocketBuf)

	fmt.Fprintf(os.Stderr, "dropline serve: listening on %s (TCP control + UDP stream, max-sessions=%d)\n", cfg.listen, cfg.maxSessions)

	// One Hub demultiplexes UDP packets by flow_id across all concurrent
	// sessions. Its Run goroutine is the single ReadFromUDP caller for
	// the lifetime of the server; cancelling ctx unblocks Read via the
	// drainer-shutdown deadline trick.
	hub := stream.NewHub(udpConn)
	hubDone := make(chan struct{})
	go func() {
		defer close(hubDone)
		_ = hub.Run(ctx)
	}()

	srv := &control.Server{
		Handler:      newServeHandler(hub, rawICMP.OK, cfg.maxSessions),
		ProbeHandler: handleTCPProbe,
	}
	err = srv.Serve(ctx, tcpLn)
	<-hubDone
	return err
}

// newServeHandler returns a control.Handler that admits up to
// maxSessions concurrent sessions and rejects further attempts with
// "server busy". reverseCapable threads server-side raw-ICMP capability
// into each session's reverse-trace decision.
func newServeHandler(hub *stream.Hub, reverseCapable bool, maxSessions int) control.Handler {
	if maxSessions < 1 {
		maxSessions = 1
	}
	sem := make(chan struct{}, maxSessions)
	return func(ctx context.Context, c *control.Conn, hello *control.Hello) error {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		default:
			return c.Send(&control.Error{Type: control.TypeError, Reason: "server busy: max sessions reached"})
		}
		return handleSession(ctx, c, hello, hub, reverseCapable)
	}
}

func handleSession(ctx context.Context, c *control.Conn, hello *control.Hello, hub *stream.Hub, reverseCapable bool) error {
	if reason := validateHello(hello); reason != "" {
		return c.Send(&control.Error{Type: control.TypeError, Reason: reason})
	}
	sid, err := mintSessionID()
	if err != nil {
		return c.Send(&control.Error{Type: control.TypeError, Reason: "internal error: session id"})
	}

	reverseDecision := resolveReverseTrace(hello.ReverseTrace, reverseCapable)
	reverseStreamDecision, reverseStreamFlowID := resolveReverseStream(hello.ReverseStream, hello.ReverseFlowID, hello.FlowID)
	tcpCorroborateDecision := resolveTCPCorroborate(hello.TCPCorroborate)

	// Register the flow_id BEFORE sending Ready so a collision produces a
	// terminal Error rather than a Ready-then-Error sequence. The Hub's
	// panicking Register is reserved for invariants the server controls;
	// flow_id comes from an untrusted Hello, so use TryRegister.
	flowHandle, ok := hub.TryRegister(hello.FlowID, stream.DefaultRingSize)
	if !ok {
		return c.Send(&control.Error{Type: control.TypeError, Reason: "flow_id collision; retry with a fresh flow_id"})
	}
	defer flowHandle.Release()

	if err := c.Send(&control.Ready{
		Type:           control.TypeReady,
		SessionID:      sid,
		ReverseTrace:   reverseDecision,
		ReverseStream:  reverseStreamDecision,
		ReverseFlowID:  reverseStreamFlowID,
		TCPCorroborate: tcpCorroborateDecision,
	}); err != nil {
		return err
	}

	snaps := make(chan stream.Snapshot, 4)
	rcv, err := stream.NewReceiverFromHub(flowHandle, stream.ReceiverConfig{
		Tick:        time.Second,
		Snapshots:   snaps,
		KernelDrops: stream.NewKernelDropSampler(),
	})
	if err != nil {
		return c.Send(&control.Error{Type: control.TypeError, Reason: "internal error: " + err.Error()})
	}

	duration := time.Duration(hello.DurationMS) * time.Millisecond
	sessionCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	// paused gates the stats forwarder — when set, the per-second
	// Stats snapshots are dropped on the floor so the client's
	// sparkline freezes cleanly. Receiver and prober keep running
	// underneath; their counters don't move because no packets arrive
	// during a paused window.
	var paused atomic.Bool

	// Recv loop demuxes client→server messages. Pause toggles the
	// forwarder gate; any other message or any error cancels the session.
	go func() {
		for {
			msg, err := c.Recv()
			if err != nil {
				cancel()
				return
			}
			if p, ok := msg.(*control.Pause); ok {
				paused.Store(p.Paused)
				continue
			}
			cancel()
			return
		}
	}()

	// Stats forwarder: pump receiver Snapshots onto the control
	// channel. When paused the snapshot is consumed but not forwarded.
	// Exits on the first Send error so a dead peer doesn't drag the
	// session out for the full DurationMS at WriteTimeout per tick.
	forwarderDone := make(chan struct{})
	go func() {
		defer close(forwarderDone)
		for {
			select {
			case <-sessionCtx.Done():
				return
			case s := <-snaps:
				if paused.Load() {
					continue
				}
				if err := c.Send(&control.Stats{
					Type:        control.TypeStats,
					T:           s.T,
					Recv:        s.Recv,
					Loss:        s.Lost,
					JitterMS:    s.JitterMS,
					KernelDrops: s.KernelDrops,
				}); err != nil {
					fmt.Fprintf(os.Stderr, "dropline serve: stats forwarder exiting: %s\n", err)
					return
				}
			}
		}
	}()

	// Reverse trace: when the resolved decision is "on", run an ICMP TTL
	// walk back toward the client's TCP RemoteAddr concurrently with the
	// stream test, and forward each prober Snapshot's hops as one
	// ReverseHopUpdate per hop. The probe.Prober opens its own socket; if
	// reverseCapable was true at startup but the per-session open fails,
	// log and downgrade silently — the client already saw "on" in Ready.
	var reverseWG sync.WaitGroup
	if reverseDecision == "on" {
		clientIP := clientIPv4(c.RemoteAddr())
		if clientIP == nil {
			fmt.Fprintln(os.Stderr, "dropline serve: reverse trace skipped (no IPv4 RemoteAddr)")
		} else {
			startReverseTrace(sessionCtx, &reverseWG, c, clientIP, reverseInterval(hello.MTRIntervalMS))
		}
	}

	// Reverse UDP stream: when the resolved decision is "on", run a
	// Sender writing back to the client's first-observed UDP source
	// address via the Hub's shared listening socket (WriteToUDP), stamped
	// with the negotiated reverse flow id. Rate / duration / packet-size
	// mirror the forward stream.
	var reverseStreamWG sync.WaitGroup
	var reverseStreamSent atomic.Int64
	var reverseStreamDurationS atomic.Uint64 // float64 bits
	if reverseStreamDecision == "on" {
		reverseStreamWG.Add(1)
		go runReverseStream(sessionCtx, &reverseStreamWG, hub, flowHandle, hello, reverseStreamFlowID,
			&reverseStreamSent, &reverseStreamDurationS)
	}

	finalSnap, _ := rcv.Run(sessionCtx)
	cancel()
	<-forwarderDone
	reverseWG.Wait()
	reverseStreamWG.Wait()

	final := buildFinal(hello, finalSnap, &reverseStreamSent, &reverseStreamDurationS)
	return c.Send(&final)
}

// resolveTCPCorroborate is the server's accept/reject decision for the
// client's --tcp-corroborate preference. Today the server accepts probe
// connections unconditionally (the handler is wired in serveRun), so
// the resolution is trivial — anything other than the explicit "off"
// resolves to "on". Kept as a function so future server-side gating
// (e.g. a flag to disable the probe handler) lands here.
func resolveTCPCorroborate(clientPref string) string {
	if clientPref == "off" {
		return "off"
	}
	return "on"
}

// resolveReverseStream picks the server's reverse-UDP-stream decision.
// Returns ("off", 0) when the client opted out, sent no/colliding
// ReverseFlowID, or asked for something unparseable. Returns ("on",
// clientReverseFlowID) otherwise — the server doesn't pick its own flow
// id; it echoes the client's choice. Mirrors resolveReverseTrace's
// auto-vs-off semantics.
func resolveReverseStream(clientPref string, clientReverseFlowID, forwardFlowID uint32) (string, uint32) {
	switch clientPref {
	case "off":
		return "off", 0
	case "on", "auto":
		// fallthrough
	default:
		return "off", 0
	}
	if clientReverseFlowID == 0 || clientReverseFlowID == forwardFlowID {
		return "off", 0
	}
	return "on", clientReverseFlowID
}

// runReverseStream blocks on the forward stream's first packet to learn
// the client's UDP source addr, then runs a Sender against that addr
// using the Hub's listening socket (WriteToUDP). On exit it records the
// total packets sent and the duration the sender actually ran for so
// buildFinal can carry both values to the client.
//
// math.Float64bits is used to atomically publish the float64 duration
// through an atomic.Uint64 — there's no atomic.Float64 in the std lib.
func runReverseStream(ctx context.Context, wg *sync.WaitGroup, hub *stream.Hub, flowHandle *stream.HubFlow,
	hello *control.Hello, reverseFlowID uint32, sentOut *atomic.Int64, durationBitsOut *atomic.Uint64) {
	defer wg.Done()
	clientAddr, err := flowHandle.RemoteAddr(ctx)
	if err != nil {
		// Either ctx cancelled before the client sent its first packet,
		// or the addr capture itself failed. Either way silently skip
		// reverse — the client will see ReverseSent=0 in Final and
		// renderers degrade naturally.
		return
	}
	duration := time.Duration(hello.DurationMS) * time.Millisecond
	sender, err := stream.NewSender(hub.Conn(), stream.SenderConfig{
		RateBPS:     hello.RateBPS,
		PacketSize:  hello.PacketSize,
		Duration:    duration,
		FlowID:      reverseFlowID,
		WriteTarget: clientAddr,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dropline serve: reverse-stream sender init failed: %s\n", err)
		return
	}
	start := time.Now()
	_ = sender.Run(ctx)
	sentOut.Store(sender.Sent())
	durationBitsOut.Store(math.Float64bits(time.Since(start).Seconds()))
}

// resolveReverseTrace picks the server's reverse-trace decision from the
// client's preference plus the server's raw-ICMP capability. Empty
// clientPref (old client) is treated as "auto". See plan resolution table.
func resolveReverseTrace(clientPref string, capable bool) string {
	if clientPref == "off" || !capable {
		return "off"
	}
	return "on"
}

// clientIPv4 extracts the IPv4 form of the control-channel peer's address.
// Returns nil if the peer isn't a TCP address or carries no IPv4 form
// (v6-only client). Loopback / RFC1918 addresses are returned as-is — LAN
// reverse-trace is supported per progress.md.
func clientIPv4(addr net.Addr) net.IP {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return nil
	}
	return tcp.IP.To4()
}

// startReverseTrace spawns the reverse prober and a per-hop forwarder
// goroutine, both gated by sessionCtx. The wg is incremented for both
// goroutines so handleSession can wait for them before sending Final.
// interval is the TTL-sweep cadence — derived from the client's
// --mtr-interval (Hello.MTRIntervalMS) with a 1s fallback.
func startReverseTrace(sessionCtx context.Context, wg *sync.WaitGroup, c *control.Conn, target net.IP, interval time.Duration) {
	revSnaps := make(chan probe.Snapshot, 4)
	prober, err := probe.New(probe.Config{
		Target:      target,
		MTRInterval: interval,
		MaxHops:     30,
	}, revSnaps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dropline serve: reverse prober init failed: %s\n", err)
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = prober.Run(sessionCtx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-sessionCtx.Done():
				return
			case snap := <-revSnaps:
				// The prober trims its sweep at the detected terminus, so
				// the highest TTL in this batch IS the active cap. Stamp
				// every update with it so the client can prune any cached
				// hop past the cap — otherwise the first sweep's no-reply
				// TTLs stay forever (the prober stops re-probing them once
				// terminus is set, and the wire is merge-only).
				maxTTL := 0
				if n := len(snap.Hops); n > 0 {
					maxTTL = snap.Hops[n-1].TTL
				}
				for _, h := range snap.Hops {
					if err := c.Send(&control.ReverseHopUpdate{
						Type:          control.TypeReverseHopUpdate,
						TTL:           h.TTL,
						Addr:          h.Addr,
						RTTMS:         h.LastRTTMS,
						LossPct:       h.LossPct,
						Terminus:      classifyTerminus(h, target),
						Sent:          h.Sent,
						Recv:          h.Recv,
						BestRTTMS:     h.BestRTTMS,
						WorstRTTMS:    h.WorstRTTMS,
						AvgRTTMS:      h.AvgRTTMS,
						StdDevRTTMS:   h.StdDevRTTMS,
						BaselineRTTMS: h.BaselineRTTMS,
						MaxTTL:        maxTTL,
					}); err != nil {
						fmt.Fprintf(os.Stderr, "dropline serve: reverse-hop forwarder exiting: %s\n", err)
						return
					}
				}
			}
		}
	}()
}

// reverseInterval picks the reverse prober's TTL-walk cadence from the
// client's Hello.MTRIntervalMS, with a 1s fallback when the field is
// missing (old client) or non-positive.
func reverseInterval(ms int64) time.Duration {
	if ms <= 0 {
		return time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

// classifyTerminus maps a reverse-prober HopStat into the wire's
// Terminus tag. Empty for intermediate hops (no EchoReply seen yet),
// "host" when the EchoReply came from the client we walked toward,
// "nat" when the EchoReply came from a different address (the path
// was translated by a NAT before reaching the client host).
func classifyTerminus(h probe.HopStat, clientIP net.IP) string {
	if !h.Terminus {
		return ""
	}
	if h.Addr == "" {
		return ""
	}
	replyIP := net.ParseIP(h.Addr)
	if replyIP == nil {
		return ""
	}
	if replyIP.Equal(clientIP) {
		return "host"
	}
	return "nat"
}

// buildFinal converts a stream.Snapshot to a control.Final. The server
// has no sender-side counter for the forward direction, so `sent` is
// approximated as MaxSeq + 1 when any traffic was observed; this
// under-counts by however many of the sender's *last* packets were lost
// (and never reached us to bump MaxSeq). Documented as a known v1
// limitation. For the reverse stream the server IS the sender so it
// reports an exact Sent count.
//
// reverseSent and reverseDurationBits may be nil when reverse stream is
// off for this session, in which case the corresponding FinalStats
// fields stay zero (omitempty drops them from the wire).
func buildFinal(hello *control.Hello, s stream.Snapshot,
	reverseSent *atomic.Int64, reverseDurationBits *atomic.Uint64) control.Final {
	var sent int64
	if s.Recv > 0 || s.Lost > 0 {
		sent = int64(s.MaxSeq) + 1 // #nosec G115 -- bounded by sender duration
	}
	var rateRx int64
	if s.T > 0 {
		rateRx = int64(float64(s.Recv*int64(hello.PacketSize)*8) / s.T)
	}
	var revSent int64
	var revDur float64
	if reverseSent != nil {
		revSent = reverseSent.Load()
	}
	if reverseDurationBits != nil {
		revDur = math.Float64frombits(reverseDurationBits.Load())
	}
	return control.Final{
		Type: control.TypeFinal,
		Stats: control.FinalStats{
			Sent:        sent,
			Recv:        s.Recv,
			Loss:        s.Lost,
			OutOfOrder:  s.OutOfOrder,
			Duplicates:  s.Duplicates,
			JitterMS:    s.JitterMS,
			KernelDrops: s.KernelDrops,
			DurationS:   s.T,
			Bursts: control.BurstBuckets{
				One:       s.Bursts.One,
				Two:       s.Bursts.Two,
				Three9:    s.Bursts.Three9,
				Ten99:     s.Bursts.Ten99,
				HundredUp: s.Bursts.HundredUp,
				Max:       s.Bursts.Max,
			},
			RateTxBPS:        hello.RateBPS,
			RateRxBPS:        rateRx,
			ReverseSent:      revSent,
			ReverseDurationS: revDur,
		},
	}
}

// validateHello returns an empty string if the Hello is acceptable, or a
// human-readable reason for rejection. The reason is sent verbatim in the
// Error message back to the client.
func validateHello(h *control.Hello) string {
	if h.Version != 1 {
		return fmt.Sprintf("unsupported version %d (server speaks v1)", h.Version)
	}
	if h.Mode != "loss" {
		return fmt.Sprintf("unsupported mode %q (server supports \"loss\")", h.Mode)
	}
	if h.PacketSize < stream.HeaderSize || h.PacketSize > stream.MaxPacketSize {
		return fmt.Sprintf("packet_size %d out of range [%d,%d]", h.PacketSize, stream.HeaderSize, stream.MaxPacketSize)
	}
	if h.DurationMS <= 0 {
		return fmt.Sprintf("duration_ms must be > 0, got %d", h.DurationMS)
	}
	if h.DurationMS > maxSessionDurationMS {
		return fmt.Sprintf("duration_ms %d exceeds server cap %d", h.DurationMS, maxSessionDurationMS)
	}
	if h.RateBPS <= 0 {
		return fmt.Sprintf("rate_bps must be > 0, got %d", h.RateBPS)
	}
	return ""
}

// handleTCPProbe is the server-side discard sink for TCP retransmit
// corroboration. After the client's TCPProbe first message has been
// parsed by control.Server, this handler reads bytes off the raw conn
// in a tight loop and discards them. The client's TCP_INFO sampling
// happens on the client side; the server has no diagnostic role here
// beyond keeping the connection open so the kernel keeps transmitting.
//
// The function returns when the client closes the connection, when ctx
// is canceled (server shutdown), or when the read returns a non-EOF
// error. We don't bound the lifetime with hello.DurationMS — the
// client controls its end of the conn and closes when done.
func handleTCPProbe(ctx context.Context, nc net.Conn, probe *control.TCPProbe) {
	// Close the conn on ctx done so a long-lived peer doesn't keep the
	// goroutine alive past server shutdown.
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		select {
		case <-ctx.Done():
			_ = nc.Close()
		case <-doneCh:
		}
	}()
	buf := make([]byte, 32<<10)
	for {
		if _, err := nc.Read(buf); err != nil {
			return
		}
	}
}

func mintSessionID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
