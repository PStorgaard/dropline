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
	"strings"
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
	listen             string
	maxSessions        int
	maxRateBPS         int64 // 0 == unlimited
	allowReverseStream bool
}

// maxSessionDurationMS caps Hello.DurationMS at admission time. The cap
// is server admission policy bounding the duration an idle client can
// hold a session slot; the default trace duration (60s) is 60× below.
const maxSessionDurationMS int64 = 60 * 60 * 1000

// MTR interval bounds enforced in validateHello. Mirror the client-side
// time.Duration bounds in trace.go (minMTRInterval / maxMTRInterval).
// MTRIntervalMS == 0 is the legacy pre-MTR-field signal from old clients
// and is accepted as-is — reverseInterval() substitutes a 1s default.
const (
	minMTRIntervalMS int64 = 100
	maxMTRIntervalMS int64 = 60_000
)

func parseServeArgs(args []string, errOut io.Writer) (serveConfig, error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(errOut)
	listen := fs.String("listen", ":5301", "address to bind for both TCP control and UDP stream")
	maxSessions := fs.Int("max-sessions", 4, "maximum concurrent client sessions")
	maxRateRaw := fs.String("max-rate-bps", "1G", "per-session send-rate cap (bps; suffixes K/M/G accepted; 0 or \"unlimited\" disables the cap)")
	allowReverseStream := fs.Bool("allow-reverse-stream", true, "permit server-side reverse-direction UDP loss test")
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
	maxRateBPS, err := parseMaxRate(*maxRateRaw)
	if err != nil {
		return serveConfig{}, fmt.Errorf("--max-rate-bps: %w", err)
	}
	return serveConfig{
		listen:             *listen,
		maxSessions:        *maxSessions,
		maxRateBPS:         maxRateBPS,
		allowReverseStream: *allowReverseStream,
	}, nil
}

// parseMaxRate is a thin wrapper over parseRate that accepts "0" and
// "unlimited" as "no cap". parseRate itself rejects zero because
// --rate / --tcp-corroborate-rate legitimately require non-zero send
// rates; loosening it there would silently break those.
func parseMaxRate(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "0" || strings.EqualFold(s, "unlimited") {
		return 0, nil
	}
	return parseRate(raw)
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

	// "tcp4" mirrors the udp4 listener above; v1 is IPv4-only and we
	// want both sockets bound to the same family so a hostname-style
	// --listen can't bind TCP on IPv6 while UDP sits on IPv4.
	tcpLn, err := net.Listen("tcp4", cfg.listen)
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

	// Match the direct receiver's SO_RCVBUF target so multi-session
	// hub-driven serving doesn't run on the OS default buffer. The
	// kernel silently caps at rmem_max; the warning below surfaces
	// that case.
	_ = udpConn.SetReadBuffer(stream.DefaultSocketBuf)

	// Best-effort warning when the kernel's rmem_max would cap the
	// receiver's SO_RCVBUF target — silent on non-Linux and on
	// /proc-read failures (see internal/stream/rmemmax_*).
	stream.WarnIfRmemMaxBelow(os.Stderr, stream.DefaultSocketBuf)

	rateBanner := "unlimited"
	if cfg.maxRateBPS > 0 {
		rateBanner = fmt.Sprintf("%d bps", cfg.maxRateBPS)
	}
	fmt.Fprintf(os.Stderr, "dropline serve: listening on %s (TCP control + UDP stream, max-sessions=%d, max-rate-bps=%s, allow-reverse-stream=%t)\n",
		cfg.listen, cfg.maxSessions, rateBanner, cfg.allowReverseStream)

	// One Hub demultiplexes UDP packets by flow_id across all concurrent
	// sessions. Its Run goroutine is the single ReadFromUDP caller for
	// the lifetime of the server; cancelling serveCtx unblocks Read via
	// the drainer-shutdown deadline trick.
	hub := stream.NewHub(udpConn)

	reg := newSessionRegistry()
	srv := &control.Server{
		Handler:      newServeHandler(hub, rawICMP.OK, cfg.maxSessions, cfg.maxRateBPS, cfg.allowReverseStream, reg),
		ProbeHandler: newProbeHandler(reg, cfg.maxSessions),
		// Cap pre-Hello accepted connections so a TCP flood can't pin
		// goroutines + fds for HelloReadTimeout each. 8× headroom over
		// maxSessions tolerates reconnect bursts; --max-sessions defaults
		// to 4, so the default cap is 32.
		MaxPendingConns: 8 * cfg.maxSessions,
	}

	return runServerLoops(ctx,
		func(c context.Context) error { return hub.Run(c) },
		func(c context.Context) error { return srv.Serve(c, tcpLn) },
	)
}

// runServerLoops drives the UDP hub and TCP accept loop as a single
// failure domain. Without this coupling, a fatal accept error from the
// TCP loop would leave hub.Run running on the parent ctx forever, and a
// hub read failure would let TCP keep admitting sessions that can never
// receive UDP. Either loop's exit cancels the other; the first
// non-context error wins and propagates back to the caller.
func runServerLoops(ctx context.Context, hubRun, tcpServe func(context.Context) error) error {
	serveCtx, serveCancel := context.WithCancel(ctx)
	defer serveCancel()

	// sync.Once + plain error replaces an earlier atomic.Value: the latter
	// panics on the second CompareAndSwap if hubRun and tcpServe race to
	// store errors with different concrete types (e.g. *net.OpError vs
	// *fmt.wrapError). wg.Wait below establishes the happens-before for
	// the firstErr read.
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	fail := func(err error) {
		if err != nil && !errors.Is(err, context.Canceled) {
			errOnce.Do(func() { firstErr = err })
		}
		serveCancel()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fail(hubRun(serveCtx))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		fail(tcpServe(serveCtx))
	}()

	wg.Wait()
	return firstErr
}

// newServeHandler returns a control.Handler that admits up to
// maxSessions concurrent sessions and rejects further attempts with
// "server busy". reverseCapable threads server-side raw-ICMP capability
// into each session's reverse-trace decision. maxRateBPS caps the
// client-requested send rate (0 = unlimited); allowReverseStream is the
// global kill switch for the reverse-direction UDP loss test. The
// registry threads session liveness to newProbeHandler so tcp_probe
// connections can be gated by an active session.
func newServeHandler(hub *stream.Hub, reverseCapable bool, maxSessions int, maxRateBPS int64, allowReverseStream bool, reg *sessionRegistry) control.Handler {
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
		return handleSession(ctx, c, hello, hub, reverseCapable, maxRateBPS, allowReverseStream, reg)
	}
}

// sessionState is the per-session policy + lifetime handle stored in
// sessionRegistry. ctx is the session's WithTimeout context — probe
// handlers parent on it so a probe dies when its session does, without
// waiting for the flat probeMaxLifetime ceiling. tcpProbeOK records
// whether this session negotiated tcp_corroborate="on"; probes against
// sessions where it's false are rejected.
//
// clientIP pins the TCP control conn's source address so the probe
// handler can reject any TCPProbe dial that arrives from a different IP
// — a peer who has the session_id but not the control-plane vantage
// point can't open a probe. nil clientIP (non-TCPv4 control conn) is
// treated as "no probes allowed" at admission time.
//
// probeRate is the negotiated Hello.TCPCorroborateRateBPS the probe
// handler enforces via read-side backpressure. Zero means "no cap"
// (legacy/test path); production Hello always carries a positive rate
// when tcp_corroborate is on.
type sessionState struct {
	ctx        context.Context
	tcpProbeOK bool
	clientIP   net.IP
	probeRate  int64
}

// sessionRegistry tracks the set of session IDs currently held by a
// live handleSession goroutine plus the per-session policy a probe
// handler needs. It exists so newProbeHandler can reject tcp_probe
// connections that don't reference a real session (otherwise any peer
// with TCP reachability could hold a probe fd open forever) and so
// probes inherit the session's lifetime.
type sessionRegistry struct {
	mu       sync.Mutex
	sessions map[string]sessionState
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{sessions: make(map[string]sessionState)}
}

func (r *sessionRegistry) add(sid string, st sessionState) {
	r.mu.Lock()
	r.sessions[sid] = st
	r.mu.Unlock()
}

func (r *sessionRegistry) remove(sid string) {
	r.mu.Lock()
	delete(r.sessions, sid)
	r.mu.Unlock()
}

func (r *sessionRegistry) lookup(sid string) (sessionState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.sessions[sid]
	return st, ok
}

func handleSession(ctx context.Context, c *control.Conn, hello *control.Hello, hub *stream.Hub, reverseCapable bool, maxRateBPS int64, allowReverseStream bool, reg *sessionRegistry) error {
	if reason := validateHello(hello, maxRateBPS); reason != "" {
		return c.Send(&control.Error{Type: control.TypeError, Reason: reason})
	}
	sid, err := mintSessionID()
	if err != nil {
		return c.Send(&control.Error{Type: control.TypeError, Reason: "internal error: session id"})
	}

	// Pin the TCP control peer's IPv4 address up front; the hub uses it
	// to drop spoofed-source UDP, and the reverse-trace prober uses it
	// as the TTL-walk target. nil means non-TCPv4 (IPv6 client, etc.),
	// in which case both reverse paths are forced off — reverse stream
	// can't work without a known peer IP, and reverse trace was already
	// gated this way.
	clientIP := clientIPv4(c.RemoteAddr())

	reverseDecision := resolveReverseTrace(hello.ReverseTrace, reverseCapable)
	reverseStreamDecision, reverseStreamFlowID := resolveReverseStream(hello.ReverseStream, hello.ReverseFlowID, hello.FlowID)
	tcpCorroborateDecision := resolveTCPCorroborate(hello.TCPCorroborate)
	if clientIP == nil {
		reverseDecision = "off"
		reverseStreamDecision, reverseStreamFlowID = "off", 0
	}
	if !allowReverseStream {
		reverseStreamDecision, reverseStreamFlowID = "off", 0
	}

	// Register the flow_id BEFORE sending Ready so a collision produces a
	// terminal Error rather than a Ready-then-Error sequence. The Hub's
	// panicking Register is reserved for invariants the server controls;
	// flow_id comes from an untrusted Hello, so use TryRegister.
	flowHandle, ok := hub.TryRegister(hello.FlowID, stream.DefaultRingSize, clientIP, hello.Token)
	if !ok {
		return c.Send(&control.Error{Type: control.TypeError, Reason: "flow_id collision; retry with a fresh flow_id"})
	}
	defer flowHandle.Release()

	duration := time.Duration(hello.DurationMS) * time.Millisecond
	sessionCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	// Publish the session BEFORE sending Ready. The client cannot
	// learn session_id until Ready reaches it, but once it does it
	// may dial the TCP probe socket immediately — if reg.add hadn't
	// happened yet, the probe handler would reject as "unknown or
	// expired session_id". tcpProbeOK gates probe admission to
	// sessions that actually negotiated corroboration. clientIP and
	// probeRate thread the control-conn IP + negotiated rate into
	// newProbeHandler so probes from other IPs are rejected and the
	// reader is rate-limited.
	reg.add(sid, sessionState{
		ctx:        sessionCtx,
		tcpProbeOK: tcpCorroborateDecision == "on",
		clientIP:   clientIP,
		probeRate:  hello.TCPCorroborateRateBPS,
	})
	defer reg.remove(sid)

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
					// Cancel the session so rcv.Run unwinds and releases the
					// maxSessions slot now, rather than holding it until
					// DurationMS expires while the peer ignores us.
					cancel()
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
		// clientIP was already extracted up top; if it were nil
		// reverseDecision would have been forced to "off".
		startReverseTrace(sessionCtx, cancel, &reverseWG, c, clientIP, reverseInterval(hello.MTRIntervalMS))
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
// client's --tcp-corroborate preference. Empty/absent on the wire is the
// pre-feature old-client signal and resolves to "off" to match Hello's
// docstring contract; explicit "off" likewise resolves to "off".
// Anything else ("on", "auto", future values) resolves to "on" so the
// server is prepared to accept a TCP corroboration probe connection for
// this session. Per-probe authorization (whether THIS session
// negotiated "on") is enforced separately in newProbeHandler.
func resolveTCPCorroborate(clientPref string) string {
	if clientPref == "" || clientPref == "off" {
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
		Token:       hello.Token,
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
// --mtr-interval (Hello.MTRIntervalMS) with a 1s fallback. cancel is
// the session's CancelFunc; the per-hop forwarder calls it on a
// terminal c.Send failure so a peer that stops reading TCP releases its
// session slot promptly rather than waiting for DurationMS.
func startReverseTrace(sessionCtx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, c *control.Conn, target net.IP, interval time.Duration) {
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
		if err := prober.Run(sessionCtx); err != nil {
			fmt.Fprintf(os.Stderr, "dropline serve: reverse prober: %s\n", err)
		}
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
						// Cancel the session so rcv.Run unwinds and the slot
						// frees rather than waiting for DurationMS.
						cancel()
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
			LocalDrops:  s.LocalDrops,
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
// Error message back to the client. maxRateBPS=0 disables the rate cap.
func validateHello(h *control.Hello, maxRateBPS int64) string {
	if h.Version != 1 {
		return fmt.Sprintf("unsupported version %d (server speaks v1)", h.Version)
	}
	if h.Mode != "loss" {
		return fmt.Sprintf("unsupported mode %q (server supports \"loss\")", h.Mode)
	}
	// Token is the per-session secret stamped into every UDP packet's
	// Header.Token. The server uses it to defeat same-NAT / on-path
	// injection that knows only flow_id + source IP. A zero token means
	// the client either omitted the field (pre-feature build) or chose
	// not to mint one; either way no integrity gate is possible, so
	// reject up front rather than silently degrade.
	if h.Token == 0 {
		return "token required (zero is reserved as \"no token\")"
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
	if maxRateBPS > 0 && h.RateBPS > maxRateBPS {
		return fmt.Sprintf("rate_bps %d exceeds server cap %d", h.RateBPS, maxRateBPS)
	}
	// MTRIntervalMS == 0 is the old-client signal for "no MTR field"; the
	// server falls back to 1s in reverseInterval(). Reject any non-zero
	// value outside the documented bounds so a malicious client can't
	// drive the reverse prober at flood rates.
	if h.MTRIntervalMS != 0 && (h.MTRIntervalMS < minMTRIntervalMS || h.MTRIntervalMS > maxMTRIntervalMS) {
		return fmt.Sprintf("mtr_interval_ms %d out of range [%d,%d]",
			h.MTRIntervalMS, minMTRIntervalMS, maxMTRIntervalMS)
	}
	// TCPCorroborateRateBPS == 0 means "TCP corroboration disabled" (the
	// field is omitempty on the wire). When the client opted out via
	// TCPCorroborate=="off", the rate is meaningless even if non-zero
	// — don't reject the session for a feature it isn't using. When
	// the client wants corroboration, apply the same per-session cap
	// as the forward UDP rate.
	if h.TCPCorroborateRateBPS < 0 {
		return fmt.Sprintf("tcp_corroborate_rate_bps must be >= 0, got %d", h.TCPCorroborateRateBPS)
	}
	if h.TCPCorroborate != "off" && maxRateBPS > 0 && h.TCPCorroborateRateBPS > maxRateBPS {
		return fmt.Sprintf("tcp_corroborate_rate_bps %d exceeds server cap %d",
			h.TCPCorroborateRateBPS, maxRateBPS)
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
// Probe connections are gated three ways:
//   - They must reference an active session_id (registered by
//     handleSession). Bare reachability to the TCP port is not enough.
//   - They take a slot on a per-server probe semaphore sized to
//     maxProbes, so an attacker can't pin one goroutine + fd per probe
//     beyond the configured concurrency.
//   - Each read carries an idle deadline, and the connection has a
//     hard lifetime ceiling, so a peer that opens a probe and stops
//     sending bytes still gets evicted.
const (
	probeIdleTimeout = 30 * time.Second
	probeMaxLifetime = 5 * time.Minute
)

func newProbeHandler(reg *sessionRegistry, maxProbes int) control.ProbeHandler {
	if maxProbes < 1 {
		maxProbes = 1
	}
	sem := make(chan struct{}, maxProbes)
	return func(ctx context.Context, nc net.Conn, probe *control.TCPProbe) {
		// Reject probes that don't match a live session before
		// taking any slot or starting any goroutines.
		st, ok := reg.lookup(probe.SessionID)
		if probe.SessionID == "" || !ok {
			_ = control.WriteMessage(nc, &control.Error{Type: control.TypeError, Reason: "tcp_probe: unknown or expired session_id"})
			return
		}
		// Probes are admitted only for sessions whose Hello negotiated
		// tcp_corroborate="on". Any other state — including the
		// "on"-by-accident from the old resolveTCPCorroborate behavior —
		// is rejected explicitly so an attacker can't open a probe
		// against an unrelated session.
		if !st.tcpProbeOK {
			_ = control.WriteMessage(nc, &control.Error{Type: control.TypeError, Reason: "tcp_probe: not negotiated for this session"})
			return
		}
		// Bind the probe to the control-conn's source IP. A peer with
		// the session_id but a different vantage point (e.g. it leaked
		// via logs / sidecars) cannot open a probe — the only IP that
		// can is the one that holds the control channel.
		peer, peerOK := nc.RemoteAddr().(*net.TCPAddr)
		if !peerOK || st.clientIP == nil || !peer.IP.To4().Equal(st.clientIP) {
			_ = control.WriteMessage(nc, &control.Error{Type: control.TypeError, Reason: "tcp_probe: source IP does not match control connection"})
			return
		}
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		default:
			_ = control.WriteMessage(nc, &control.Error{Type: control.TypeError, Reason: "tcp_probe: server busy"})
			return
		}

		// Parent the probe ctx on the session ctx so the probe dies
		// with its session (normal teardown via DurationMS expiry or
		// any handleSession exit path). probeMaxLifetime remains as
		// a defense-in-depth ceiling against a session that somehow
		// outlives expectations.
		probeCtx, cancel := context.WithTimeout(st.ctx, probeMaxLifetime)
		defer cancel()
		doneCh := make(chan struct{})
		defer close(doneCh)
		go func() {
			select {
			case <-probeCtx.Done():
				_ = nc.Close()
			case <-doneCh:
			}
		}()

		// Backpressure-based rate enforcement: when the session
		// negotiated a non-zero TCPCorroborateRateBPS, slow our reads
		// once the peer outpaces that rate. TCP receive-window flow
		// control then throttles the sender. No conn-close on transient
		// bursts. probeRate==0 preserves today's unmetered behavior for
		// loopback/test paths that don't advertise a rate.
		buf := make([]byte, 32<<10)
		var total int64
		start := time.Now()
		for {
			_ = nc.SetReadDeadline(time.Now().Add(probeIdleTimeout))
			n, err := nc.Read(buf)
			if err != nil {
				return
			}
			total += int64(n)
			if st.probeRate > 0 {
				expected := time.Duration(total*8*int64(time.Second) / st.probeRate)
				elapsed := time.Since(start)
				if extra := expected - elapsed; extra > 10*time.Millisecond {
					select {
					case <-time.After(extra):
					case <-probeCtx.Done():
						return
					}
				}
			}
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
