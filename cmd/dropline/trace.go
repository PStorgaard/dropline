package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/PStorgaard/dropline/internal/agg"
	"github.com/PStorgaard/dropline/internal/control"
	"github.com/PStorgaard/dropline/internal/privcheck"
	"github.com/PStorgaard/dropline/internal/probe"
	"github.com/PStorgaard/dropline/internal/report"
	"github.com/PStorgaard/dropline/internal/stream"
	"github.com/PStorgaard/dropline/internal/tcpinfo"
	"github.com/PStorgaard/dropline/internal/tui"
)

// defaultPort is the dropline server's default TCP control + UDP stream
// port. Chosen to sit one increment above iperf3's 5201 so a host can
// run both servers side-by-side without a port collision.
const defaultPort = "5301"

// MTR interval bounds. The floor keeps the forward prober's 16-bit ICMP
// seq from wrapping inside the still-live lossTimeout window: at 100ms ×
// MaxHops=30 = 300 probes/s, the 2s lossTimeout (internal/probe/probe.go)
// holds at most ~600 inflight entries, three orders of magnitude below
// 65536. Below the floor the receive loop and per-hop bookkeeping start
// drifting (a wrap collision orphans a hop's inflight counter when the
// late reply decrements the new owner's slot instead). Ceiling is a
// usability gate: above 60s the sweep emits too rarely to be useful in
// a typical session. validateHello on the server mirrors these in ms.
const (
	minMTRInterval = 100 * time.Millisecond
	maxMTRInterval = 60 * time.Second
)

type outputMode int

const (
	outputTUI outputMode = iota
	outputReport
	outputJSON
)

func (m outputMode) String() string {
	switch m {
	case outputTUI:
		return "tui"
	case outputReport:
		return "report"
	case outputJSON:
		return "json"
	default:
		return "?"
	}
}

type reverseTraceMode int

const (
	reverseAuto reverseTraceMode = iota
	reverseOn
	reverseOff
)

func (m reverseTraceMode) String() string {
	switch m {
	case reverseAuto:
		return "auto"
	case reverseOn:
		return "on"
	case reverseOff:
		return "off"
	default:
		return "?"
	}
}

type traceConfig struct {
	target             string
	rateBPS            int64
	duration           time.Duration
	packetSize         int
	mtrInterval        time.Duration
	maxHops            int
	output             outputMode
	savePath           string
	reverseTrace       reverseTraceMode
	reverseStream      reverseTraceMode
	tcpCorroborate     reverseTraceMode
	tcpCorroborateRate int64 // bps
}

func parseTraceArgs(args []string, stdoutIsTTY bool, errOut io.Writer) (traceConfig, error) {
	fs := flag.NewFlagSet("trace", flag.ContinueOnError)
	fs.SetOutput(errOut)

	rate := fs.String("rate", "10M", "send rate (bps; suffixes K/M/G accepted)")
	duration := fs.Duration("duration", 60*time.Second, "test duration")
	packetSize := fs.Int("packet-size", 1200, "UDP payload size in bytes")
	mtrInterval := fs.Duration("mtr-interval", time.Second, "interval between TTL sweeps")
	maxHops := fs.Int("max-hops", 30, "maximum TTL")
	tui := fs.Bool("tui", false, "force interactive TUI output")
	rep := fs.Bool("report", false, "force plain-text summary output")
	jsonOut := fs.Bool("json", false, "force JSON document output")
	save := fs.String("save", "", "write final JSON report to FILE")
	reverse := fs.String("reverse-trace", "auto", "reverse trace mode: on, off, or auto")
	reverseStream := fs.String("reverse-stream", "auto", "reverse-direction UDP loss test: on, off, or auto")
	tcpCorroborate := fs.String("tcp-corroborate", "auto", "TCP retransmit corroboration probe: on, off, or auto")
	tcpCorroborateRate := fs.String("tcp-corroborate-rate", "100K", "TCP corroboration probe rate (bps; suffixes K/M/G accepted)")

	// Stdlib flag.Parse stops at the first non-flag token, but the spec
	// puts TARGET before flags (`dropline trace TARGET --rate ...`).
	// Loop-parse to fold positionals out and keep parsing flags after them.
	var positionals []string
	remaining := args
	for {
		if err := fs.Parse(remaining); err != nil {
			return traceConfig{}, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		positionals = append(positionals, rest[0])
		remaining = rest[1:]
	}

	if len(positionals) == 0 {
		return traceConfig{}, errors.New("missing TARGET")
	}
	if len(positionals) > 1 {
		return traceConfig{}, fmt.Errorf("expected one TARGET, got %d", len(positionals))
	}

	target, err := normalizeTarget(positionals[0])
	if err != nil {
		return traceConfig{}, err
	}

	rateBPS, err := parseRate(*rate)
	if err != nil {
		return traceConfig{}, fmt.Errorf("--rate: %w", err)
	}

	mode, err := pickOutputMode(*tui, *rep, *jsonOut, stdoutIsTTY)
	if err != nil {
		return traceConfig{}, err
	}

	rev, err := parseReverseTrace(*reverse)
	if err != nil {
		return traceConfig{}, err
	}
	revStream, err := parseReverseStream(*reverseStream)
	if err != nil {
		return traceConfig{}, err
	}
	tcpCorr, err := parseTCPCorroborate(*tcpCorroborate)
	if err != nil {
		return traceConfig{}, err
	}
	tcpCorrRate, err := parseRate(*tcpCorroborateRate)
	if err != nil {
		return traceConfig{}, fmt.Errorf("--tcp-corroborate-rate: %w", err)
	}

	if *packetSize < 32 {
		return traceConfig{}, fmt.Errorf("--packet-size must be >= 32 (stream header is 32 bytes), got %d", *packetSize)
	}
	if *packetSize > 65507 {
		return traceConfig{}, fmt.Errorf("--packet-size must be <= 65507 (UDP max payload), got %d", *packetSize)
	}
	if *maxHops < 1 || *maxHops > 255 {
		return traceConfig{}, fmt.Errorf("--max-hops must be in [1,255], got %d", *maxHops)
	}
	if *duration <= 0 {
		return traceConfig{}, fmt.Errorf("--duration must be > 0, got %s", *duration)
	}
	if *mtrInterval < minMTRInterval || *mtrInterval > maxMTRInterval {
		return traceConfig{}, fmt.Errorf("--mtr-interval must be in [%s, %s], got %s",
			minMTRInterval, maxMTRInterval, *mtrInterval)
	}

	return traceConfig{
		target:             target,
		rateBPS:            rateBPS,
		duration:           *duration,
		packetSize:         *packetSize,
		mtrInterval:        *mtrInterval,
		maxHops:            *maxHops,
		output:             mode,
		savePath:           *save,
		reverseTrace:       rev,
		reverseStream:      revStream,
		tcpCorroborate:     tcpCorr,
		tcpCorroborateRate: tcpCorrRate,
	}, nil
}

func normalizeTarget(raw string) (string, error) {
	if raw == "" {
		return "", errors.New("TARGET is empty")
	}
	if _, _, err := net.SplitHostPort(raw); err == nil {
		return raw, nil
	}
	// No port — but bracketed-IPv6-without-port still fails SplitHostPort,
	// and so does a bare IPv6. Disambiguate by counting colons.
	if strings.Count(raw, ":") >= 2 && !strings.HasPrefix(raw, "[") {
		return "[" + raw + "]:" + defaultPort, nil
	}
	return raw + ":" + defaultPort, nil
}

// resolveTargetIPv4 takes a normalized host:port and returns ip:port with
// the host resolved to a single IPv4 address. We resolve once at the top
// of traceRun so the TCP control channel, the UDP stream, and the ICMP
// prober all see the same peer — without this, dual-stack DNS or
// round-robin A records can split the three paths across different hosts
// and the session looks healthy while data goes nowhere.
//
// Returns a clean error when the name has no IPv4 form (v6-only host) or
// fails to resolve; the caller surfaces it to the operator.
func resolveTargetIPv4(target string) (string, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return "", fmt.Errorf("split host:port %q: %w", target, err)
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("resolve %q: %w", host, err)
	}
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			return net.JoinHostPort(v4.String(), port), nil
		}
	}
	return "", fmt.Errorf("resolve %q: no IPv4 address", host)
}

func parseRate(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("empty rate")
	}
	mult := uint64(1)
	last := s[len(s)-1]
	switch last {
	case 'k', 'K':
		mult = 1_000
		s = s[:len(s)-1]
	case 'm', 'M':
		mult = 1_000_000
		s = s[:len(s)-1]
	case 'g', 'G':
		mult = 1_000_000_000
		s = s[:len(s)-1]
	}
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid rate %q", raw)
	}
	if n == 0 {
		return 0, errors.New("rate must be > 0")
	}
	// Bound the product inside int64 so downstream RateBPS fields
	// (int64) don't silently wrap to a negative value.
	if n > uint64(math.MaxInt64)/mult {
		return 0, fmt.Errorf("rate %q exceeds max supported rate", raw)
	}
	return int64(n * mult), nil
}

func pickOutputMode(tui, report, jsonOut, isTTY bool) (outputMode, error) {
	count := 0
	if tui {
		count++
	}
	if report {
		count++
	}
	if jsonOut {
		count++
	}
	if count > 1 {
		return 0, errors.New("only one of --tui/--report/--json may be set")
	}
	switch {
	case tui:
		return outputTUI, nil
	case report:
		return outputReport, nil
	case jsonOut:
		return outputJSON, nil
	}
	if isTTY {
		return outputTUI, nil
	}
	return outputReport, nil
}

func parseReverseTrace(raw string) (reverseTraceMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "auto":
		return reverseAuto, nil
	case "on":
		return reverseOn, nil
	case "off":
		return reverseOff, nil
	default:
		return 0, fmt.Errorf("--reverse-trace must be on, off, or auto; got %q", raw)
	}
}

// parseReverseStream mirrors parseReverseTrace for the --reverse-stream
// flag. Same three-mode semantics (auto/on/off) — kept distinct so the
// error message points at the right flag name.
func parseReverseStream(raw string) (reverseTraceMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "auto":
		return reverseAuto, nil
	case "on":
		return reverseOn, nil
	case "off":
		return reverseOff, nil
	default:
		return 0, fmt.Errorf("--reverse-stream must be on, off, or auto; got %q", raw)
	}
}

// parseTCPCorroborate mirrors parseReverseTrace for the --tcp-corroborate
// flag. Same three-mode semantics (auto/on/off) — distinct function for a
// distinct error message.
func parseTCPCorroborate(raw string) (reverseTraceMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "auto":
		return reverseAuto, nil
	case "on":
		return reverseOn, nil
	case "off":
		return reverseOff, nil
	default:
		return 0, fmt.Errorf("--tcp-corroborate must be on, off, or auto; got %q", raw)
	}
}

func stdoutIsTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func runTrace(args []string) {
	cfg, err := parseTraceArgs(args, stdoutIsTTY(), os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dropline trace: %s\n", err)
		os.Exit(2)
	}
	// Raw ICMP is required for the forward path-trace. Spec § Probe
	// layer requires a clear-error fail (no fallback to IcmpSendEcho2).
	if st := privcheck.RawICMP(); !st.OK {
		fmt.Fprintf(os.Stderr, "dropline trace: %s\n", st.Reason)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := traceRun(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "dropline trace: %s\n", err)
		os.Exit(1)
	}
}

// traceRun drives one full client session: open control TCP, exchange
// Hello/Ready, send UDP at the configured rate for cfg.duration,
// consume Stats, and render the report on Final. Returns nil on a
// successful test (Final received) or an error otherwise.
func traceRun(ctx context.Context, cfg traceConfig) error {
	startedAt := time.Now()

	// Pin the target to a concrete IPv4 once, so the TCP control dial,
	// the UDP stream resolve, and the ICMP probe all hit the same host.
	// Generic DNS + dual-stack would otherwise let TCP land on IPv6 or
	// a different A record than UDP/probe — a silent split-path bug.
	pinned, err := resolveTargetIPv4(cfg.target)
	if err != nil {
		return fmt.Errorf("resolve target: %w", err)
	}
	cfg.target = pinned

	cc, err := control.Dial(ctx, cfg.target)
	if err != nil {
		return fmt.Errorf("dial control %s: %w", cfg.target, err)
	}
	defer func() { _ = cc.Close() }()

	flowID, err := mintFlowID()
	if err != nil {
		return fmt.Errorf("mint flow id: %w", err)
	}
	// token is the per-session secret stamped into every UDP packet
	// header (forward AND reverse). The server validates it before
	// captureRemote so a same-NAT spoofer who knows only flow_id +
	// source IP can't latch the reverse-direction Sender at a forged
	// port. 64 bits of crypto/rand is overwhelmingly secure against
	// blind injection over a 1-hour session ceiling.
	token, err := mintToken()
	if err != nil {
		return fmt.Errorf("mint token: %w", err)
	}
	// reverseFlowID is the distinct flow id the server stamps on its
	// reverse-direction UDP packets. Minted separately and re-minted on
	// the (astronomically unlikely) collision with the forward flow id
	// so the client's receiver can filter cleanly.
	var reverseFlowID uint32
	if cfg.reverseStream != reverseOff {
		reverseFlowID, err = mintFlowID()
		if err != nil {
			return fmt.Errorf("mint reverse flow id: %w", err)
		}
		for reverseFlowID == flowID {
			if reverseFlowID, err = mintFlowID(); err != nil {
				return fmt.Errorf("mint reverse flow id: %w", err)
			}
		}
	}
	// When the user opted out of TCP corroboration the rate field is
	// meaningless; zero it on the wire so the server's per-session
	// rate cap doesn't reject a session for a feature we aren't
	// using. omitempty on the field makes 0 wire-equivalent to absent.
	tcpCorrRateOnWire := cfg.tcpCorroborateRate
	if cfg.tcpCorroborate == reverseOff {
		tcpCorrRateOnWire = 0
	}
	hello := &control.Hello{
		Type:                  control.TypeHello,
		Version:               1,
		Mode:                  "loss",
		RateBPS:               cfg.rateBPS,
		DurationMS:            cfg.duration.Milliseconds(),
		PacketSize:            cfg.packetSize,
		FlowID:                flowID,
		ReverseTrace:          cfg.reverseTrace.String(),
		MTRIntervalMS:         cfg.mtrInterval.Milliseconds(),
		ReverseStream:         cfg.reverseStream.String(),
		ReverseFlowID:         reverseFlowID,
		TCPCorroborate:        cfg.tcpCorroborate.String(),
		TCPCorroborateRateBPS: tcpCorrRateOnWire,
		Token:                 token,
	}
	if err := cc.Send(hello); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	first, err := cc.Recv()
	if err != nil {
		return fmt.Errorf("recv ready: %w", err)
	}
	var reversePathStatus string
	var reverseStreamOn bool
	var tcpCorroborateOn bool
	var sessionID string
	switch m := first.(type) {
	case *control.Ready:
		reversePathStatus = resolveReversePathStatus(cfg.reverseTrace.String(), m.ReverseTrace)
		// Reverse stream is on only if BOTH the server agreed AND its
		// echoed flow id matches what we sent. Mismatched echo means the
		// server is on a slightly different version that doesn't honor
		// the client's flow id — treat as off rather than risk filtering
		// against the wrong id.
		if m.ReverseStream == "on" && m.ReverseFlowID == reverseFlowID && reverseFlowID != 0 {
			reverseStreamOn = true
		}
		tcpCorroborateOn = (m.TCPCorroborate == "on")
		sessionID = m.SessionID
	case *control.Error:
		return fmt.Errorf("server rejected session: %s", m.Reason)
	default:
		return fmt.Errorf("unexpected first message %T (want ready)", first)
	}

	udpAddr, err := net.ResolveUDPAddr("udp4", cfg.target)
	if err != nil {
		return fmt.Errorf("resolve udp %s: %w", cfg.target, err)
	}
	udpConn, err := net.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		return fmt.Errorf("dial udp %s: %w", cfg.target, err)
	}
	defer func() { _ = udpConn.Close() }()

	// The ICMP prober walks the same target as the UDP stream. udpAddr
	// already carries the resolved IPv4; reuse it to avoid a second
	// DNS lookup that could land on a different A record. probeSnaps
	// is buffered so a slow drainer drops oldest snapshots rather than
	// stalling the prober.
	probeSnaps := make(chan probe.Snapshot, 4)
	prober, err := probe.New(probe.Config{
		Target:      udpAddr.IP,
		MTRInterval: cfg.mtrInterval,
		MaxHops:     cfg.maxHops,
	}, probeSnaps)
	if err != nil {
		return fmt.Errorf("init prober: %w", err)
	}
	defer func() { _ = prober.Close() }()

	// Pause gate is shared between the sender (which blocks on it) and
	// the TUI's [p] keybinding callback (which toggles it). Always
	// constructed; the TUI flow wires PauseFn while --report / --json
	// flows leave PauseFn nil so the gate is never flipped.
	pauseGate := stream.NewPauseGate(nil)

	sender, err := stream.NewSender(udpConn, stream.SenderConfig{
		RateBPS:    cfg.rateBPS,
		PacketSize: cfg.packetSize,
		Duration:   cfg.duration,
		FlowID:     flowID,
		Gate:       pauseGate,
		Token:      token,
	})
	if err != nil {
		return fmt.Errorf("init sender: %w", err)
	}

	// The aggregator owns the merged stream + hop history. In TUI mode it
	// emits live StateSnapshots onto a buffered channel that the bubbletea
	// model consumes; in --report / --json mode there's no live consumer
	// and we pull final state via aggregator.Snapshot() at end-of-test.
	var snaps chan agg.StateSnapshot
	if cfg.output == outputTUI {
		snaps = make(chan agg.StateSnapshot, 16)
	}
	aggregator := agg.New(startedAt, snaps)
	// Push the resolved reverse-path status into the aggregator before the
	// workers start so the TUI's first StateSnapshot already carries the
	// banner — even if no ReverseHopUpdate ever arrives.
	aggregator.SetReversePathStatus(reversePathStatus)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var workersWG sync.WaitGroup
	var senderErr error
	var proberErr error
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		senderErr = sender.Run(workerCtx)
	}()

	// Prober + snapshot drainer share workerCtx so a single cancel
	// stops both alongside the sender. Both Prober.Run implementations
	// return nil on ctx cancellation; a non-nil proberErr is always the
	// "consecutive send blackout" escalation from probe_unix.go.
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		proberErr = prober.Run(workerCtx)
	}()
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		for {
			select {
			case <-workerCtx.Done():
				return
			case s := <-probeSnaps:
				aggregator.IngestForwardHop(s)
			}
		}
	}()

	// Reverse-direction UDP stream: when the server agreed to send back
	// (Ready.ReverseStream == "on"), open a Receiver on the SAME dialed
	// UDP socket the sender is using. The socket is full-duplex; the
	// sender's Write doesn't conflict with the receiver's ReadFromUDP
	// (SetReadDeadline on the drainer affects reads only). The receiver
	// filters by reverseFlowID so any stray traffic on this socket is
	// silently dropped. Snapshots flow into a per-tick drainer that
	// forwards them to aggregator.IngestReverseStream.
	if reverseStreamOn {
		reverseSnaps := make(chan stream.Snapshot, 4)
		reverseRcv, err := stream.NewReceiver(udpConn, stream.ReceiverConfig{
			FlowID:    reverseFlowID,
			Token:     token,
			Tick:      time.Second,
			Snapshots: reverseSnaps,
			// No kernel-drop sampler on the client side. The server-side
			// receiver already reports kernel_drops for the forward path
			// (delta-baselined inside Receiver.Run); the client's reverse
			// stream isn't the diagnostic surface a user expects to see
			// kernel drops on, and leaving it nil keeps the reverse path
			// from conflating host-wide UDP noise into the test report.
		})
		if err != nil {
			return fmt.Errorf("init reverse receiver: %w", err)
		}
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			_, _ = reverseRcv.Run(workerCtx)
		}()
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case s := <-reverseSnaps:
					aggregator.IngestReverseStream(s)
				}
			}
		}()
	}

	// TCP retransmit corroboration: when the server agreed, open a
	// second TCP connection to the dropline port, send the TCPProbe
	// first message, then push dummy bytes at a low rate while
	// sampling TCP_INFO on the local end. The server-side handler just
	// reads and discards; the diagnostic signal lives entirely on the
	// client side. On dial / send failure we log and skip — the rest
	// of the test keeps running.
	if tcpCorroborateOn && cfg.tcpCorroborateRate > 0 {
		startTCPCorroborateProbe(workerCtx, &workersWG, cfg, sessionID, aggregator)
	}

	var (
		final    *control.Final
		serverEr string
		recvErr  error
	)

	// buildReportData runs Correlate + MarkSuspects against the latest
	// aggregator state for f, then constructs the canonical report.Data.
	// Idempotent: re-running from already-marked aggregator state is
	// cheap (Correlate is pure) and yields the same Data.
	buildReportData := func(f *control.Final) report.Data {
		finalState := aggregator.Snapshot()
		suspects := agg.Correlate(finalState.Buckets, finalState.LatestForwardHops)
		ttls := make([]int, len(suspects))
		for i, s := range suspects {
			ttls[i] = s.TTL
		}
		// MarkSuspects mutates the aggregator's stored hops and emits a
		// fresh StateSnapshot so a live TUI sees suspect highlights on
		// its test-complete view (the snaps channel is still open at
		// this call site for the TUI flow).
		aggregator.MarkSuspects(ttls)
		finalState = aggregator.Snapshot()
		return report.Data{
			Version:   1,
			Target:    cfg.target,
			StartedAt: startedAt,
			DurationS: f.Stats.DurationS,
			Config: report.Config{
				RateBPS:       cfg.rateBPS,
				PacketSize:    cfg.packetSize,
				MTRIntervalMS: cfg.mtrInterval.Milliseconds(),
				MaxHops:       cfg.maxHops,
				ReverseTrace:  cfg.reverseTrace.String(),
			},
			Stream:            f.Stats,
			Timeline:          finalState.Buckets,
			Hops:              hopsFromAgg(finalState.LatestForwardHops),
			ReversePath:       reverseHopsFromAgg(finalState.LatestReverseHops),
			ReversePathStatus: finalState.ReversePathStatus,
			Correlation:       suspectsToReports(suspects),
			ReverseStream:     reverseStreamReport(finalState.ReverseStream, &f.Stats),
			TCPCorroborate:    tcpCorroborateReport(finalState.TCPCorroborate),
		}
	}

	// builtData is populated from the recvLoop goroutine in TUI mode
	// once Final arrives, so an in-TUI [s] keypress can write the report
	// without re-snapping the aggregator from the bubbletea goroutine.
	var builtData atomic.Pointer[report.Data]

	if cfg.output == outputTUI {
		// TUI flow: bubbletea owns the main goroutine, so the control
		// recv loop runs in the background and feeds the aggregator. A
		// ctx-watcher closes cc when the user quits early so cc.Recv
		// unblocks; another goroutine closes the snaps channel once all
		// senders have exited so the model can transition to "test
		// complete" state.
		recvDone := make(chan struct{})
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			defer close(recvDone)
			final, serverEr, recvErr = runRecvLoop(cc, aggregator)
			// See the non-TUI flow below for why this grace exists:
			// reverse-direction UDP packets may still be arriving when
			// Final has already returned over TCP. Holding before
			// buildReportData (which Snapshot()s the aggregator) keeps
			// the reverse-stream recv count honest in the rendered
			// report and on the TUI's test-complete view.
			if reverseStreamOn {
				time.Sleep(200 * time.Millisecond)
			}
			// Run the correlator + MarkSuspects inside this goroutine
			// so the StateSnapshot it emits reaches the TUI before the
			// snaps watcher closes the channel and flips m.done.
			if final != nil {
				d := buildReportData(final)
				builtData.Store(&d)
			}
			workerCancel()
		}()
		go func() {
			<-workerCtx.Done()
			_ = cc.Close()
		}()
		go func() {
			workersWG.Wait()
			close(snaps)
		}()

		if err := tui.Run(ctx, snaps, tui.Options{
			Target:    cfg.target,
			RateBPS:   cfg.rateBPS,
			Duration:  cfg.duration,
			StartedAt: startedAt,
			ResetFn:   aggregator.Reset,
			SaveFn:    makeSaveFn(cfg, &builtData),
			CopyFn:    makeCopyFn(&builtData, os.Stdout),
			PauseFn:   makePauseFn(pauseGate, cc),
		}); err != nil {
			workerCancel()
			workersWG.Wait()
			return fmt.Errorf("tui: %w", err)
		}
		// User may have quit before the test finished. Cancel and wait
		// so final / serverEr / recvErr are stable below.
		workerCancel()
		<-recvDone
		workersWG.Wait()
	} else {
		// Mirror the TUI branch's closer: when workerCtx is cancelled
		// (SIGINT/SIGTERM via signal.NotifyContext, or any early exit
		// path that calls workerCancel), close cc so the blocked
		// cc.Recv() inside runRecvLoop returns instead of waiting for
		// the server's Final.
		go func() {
			<-workerCtx.Done()
			_ = cc.Close()
		}()
		final, serverEr, recvErr = runRecvLoop(cc, aggregator)
		// When the reverse-direction stream is active, the server emits
		// packets right up until its session timer fires; some of those
		// arrive at the client AFTER Final has already returned over TCP
		// because UDP and TCP are independent transports. Without a grace
		// window the reverse Receiver's drainer exits as soon as ctx is
		// cancelled, leaving tail packets unread in the kernel buffer
		// and the recv count under-reporting. 200ms covers loopback +
		// real internet RTTs comfortably.
		if reverseStreamOn {
			time.Sleep(200 * time.Millisecond)
		}
		workerCancel()
		workersWG.Wait()
	}

	if recvErr != nil && final == nil && serverEr == "" {
		return fmt.Errorf("control recv: %w", recvErr)
	}
	if serverEr != "" {
		return fmt.Errorf("server error: %s", serverEr)
	}
	if final == nil {
		return errors.New("control channel closed before final")
	}
	if senderErr != nil && !errors.Is(senderErr, context.Canceled) {
		fmt.Fprintf(os.Stderr, "dropline trace: sender: %s\n", senderErr)
	}
	if proberErr != nil {
		fmt.Fprintf(os.Stderr, "dropline trace: prober: %s\n", proberErr)
	}

	// Reuse the report built inside the TUI goroutine when available;
	// otherwise build it now (non-TUI flow, or TUI flow where the
	// goroutine somehow failed to publish).
	var d report.Data
	if p := builtData.Load(); p != nil {
		d = *p
	} else {
		d = buildReportData(final)
	}

	// In TUI mode the dashboard *was* the output; don't dump a second
	// summary on top of whatever the alt-screen restored. --save still
	// writes JSON if requested.
	switch cfg.output {
	case outputTUI:
		// no-op
	case outputJSON:
		if err := report.RenderJSON(os.Stdout, d); err != nil {
			return fmt.Errorf("render json: %w", err)
		}
	default:
		if err := report.RenderText(os.Stdout, d); err != nil {
			return fmt.Errorf("render text: %w", err)
		}
	}

	if cfg.savePath != "" {
		if err := writeJSONFile(cfg.savePath, d); err != nil {
			return fmt.Errorf("save %s: %w", cfg.savePath, err)
		}
	}
	return nil
}

// runRecvLoop drains the control channel into the aggregator until the
// server sends Final / Error or the connection closes. Returns the
// terminal state via three out-of-band values rather than a result struct
// so the TUI flow's goroutine can assign directly to outer-scope vars.
func runRecvLoop(cc *control.Conn, aggregator *agg.Aggregator) (*control.Final, string, error) {
	for {
		msg, err := cc.Recv()
		if err != nil {
			return nil, "", err
		}
		switch m := msg.(type) {
		case *control.Stats:
			aggregator.IngestStream(stream.Snapshot{
				T:           m.T,
				Recv:        m.Recv,
				Lost:        m.Loss,
				JitterMS:    m.JitterMS,
				KernelDrops: m.KernelDrops,
			})
		case *control.Final:
			return m, "", nil
		case *control.Error:
			return nil, m.Reason, nil
		case *control.ReverseHopUpdate:
			aggregator.IngestReverseHop(*m)
		default:
			// Future message types: ignore so we stay forward-compatible.
		}
	}
}

func writeJSONFile(path string, d report.Data) error {
	// Write to a sibling temp file and rename on success so a render
	// failure mid-stream cannot leave a truncated JSON artifact at the
	// operator-supplied path.
	tmp := path + ".tmp"
	f, err := os.Create(tmp) // #nosec G304 -- path supplied by operator
	if err != nil {
		return err
	}
	if err := report.RenderJSON(f, d); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// resolveReversePathStatus maps the client's --reverse-trace preference and
// the server's Ready.ReverseTrace echo into the canonical status string
// used in the JSON report. See plan resolution table.
func resolveReversePathStatus(clientPref, serverDecision string) string {
	if clientPref == "off" {
		return control.ReverseStatusDisabledByClient
	}
	if serverDecision == "on" {
		return control.ReverseStatusOK
	}
	return control.ReverseStatusDisabledByServer
}

// reverseHopsFromAgg adapts the aggregator's ReverseHopView into the
// renderer's ReverseHopReport. Returns nil for empty input so the JSON
// renderer can substitute a non-nil empty slice in one place.
func reverseHopsFromAgg(in []agg.ReverseHopView) []report.ReverseHopReport {
	if len(in) == 0 {
		return nil
	}
	out := make([]report.ReverseHopReport, len(in))
	for i, h := range in {
		out[i] = report.ReverseHopReport{
			TTL:           h.TTL,
			Addr:          h.Addr,
			RTTMS:         h.RTTMS,
			LossPct:       h.LossPct,
			Sent:          h.Sent,
			Recv:          h.Recv,
			BestRTTMS:     h.BestRTTMS,
			WorstRTTMS:    h.WorstRTTMS,
			AvgRTTMS:      h.AvgRTTMS,
			StdDevRTTMS:   h.StdDevRTTMS,
			BaselineRTTMS: h.BaselineRTTMS,
			Terminus:      h.Terminus,
		}
	}
	return out
}

// hopsFromAgg translates the aggregator's HopView slice into the
// renderer's HopReport shape. The two types intentionally differ — the
// renderer doesn't depend on internal/agg's wire surface — so a small
// adapter lives here next to the final-state pull. HopView.Suspect is
// the single source of truth and flows through to HopReport.Suspect;
// callers must run aggregator.MarkSuspects before this adapter.
func hopsFromAgg(in []agg.HopView) []report.HopReport {
	if len(in) == 0 {
		return nil
	}
	out := make([]report.HopReport, len(in))
	for i, h := range in {
		out[i] = report.HopReport{
			TTL:         h.TTL,
			IP:          h.IP,
			Sent:        h.Sent,
			Recv:        h.Recv,
			LossPct:     h.LossPct,
			LastRTTMS:   h.LastRTTMS,
			BestRTTMS:   h.BestRTTMS,
			WorstRTTMS:  h.WorstRTTMS,
			AvgRTTMS:    h.AvgRTTMS,
			StdDevRTTMS: h.StdDevRTTMS,
			Suspect:     h.Suspect,
		}
	}
	return out
}

// startTCPCorroborateProbe opens the dedicated TCP probe connection to
// the dropline server port, sends the TCPProbe handshake, and spawns
// (a) a low-rate writer that keeps the kernel busy enough to retransmit
// when the path drops bytes, and (b) a 1Hz sampler that reads TCP_INFO
// and forwards it to the aggregator. Dial + handshake run inside an
// outer goroutine bound to workerCtx so a stalled second TCP dial
// cannot block trace startup; tcp-corroborate is a best-effort
// sub-feature and any setup failure is logged to stderr without
// affecting the rest of the test. All inner goroutines ride the
// supplied WaitGroup so the trace driver's shutdown path picks them
// up alongside the other workers.
func startTCPCorroborateProbe(workerCtx context.Context, wg *sync.WaitGroup, cfg traceConfig, sessionID string, aggregator *agg.Aggregator) {
	// wg.Add for the outer goroutine happens *before* `go` so a parent
	// Wait can't race past us.
	wg.Add(1)
	go func() {
		defer wg.Done()
		dialer := net.Dialer{Timeout: 3 * time.Second}
		conn, err := dialer.DialContext(workerCtx, "tcp4", cfg.target)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dropline trace: tcp-corroborate dial: %s\n", err)
			return
		}
		probeConn, ok := conn.(*net.TCPConn)
		if !ok {
			_ = conn.Close()
			fmt.Fprintf(os.Stderr, "dropline trace: tcp-corroborate: dial returned non-TCP conn\n")
			return
		}
		if err := control.WriteMessage(probeConn, &control.TCPProbe{
			Type:      control.TypeTCPProbe,
			SessionID: sessionID,
			RateBPS:   cfg.tcpCorroborateRate,
		}); err != nil {
			_ = probeConn.Close()
			fmt.Fprintf(os.Stderr, "dropline trace: tcp-corroborate send: %s\n", err)
			return
		}
		sampler, err := tcpinfo.New(probeConn)
		if err != nil {
			_ = probeConn.Close()
			fmt.Fprintf(os.Stderr, "dropline trace: tcp-corroborate sampler: %s\n", err)
			return
		}

		// Close the probe socket when workerCtx fires so the writer
		// goroutine's blocking Write returns and the sampler goroutine
		// exits cleanly via the ticker case.
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-workerCtx.Done()
			_ = probeConn.Close()
		}()

		// Writer: emit fixed-size chunks at a 10Hz cadence to hit the
		// configured rate. The chunk size is chosen so even a 100kbps probe
		// produces enough TCP segments (10 chunks/sec × ~1250 bytes ≈ a few
		// MSS-sized segments) to be a meaningful retransmit canary.
		wg.Add(1)
		go func() {
			defer wg.Done()
			const ticksPerSec = 10
			bytesPerTick := int(cfg.tcpCorroborateRate / 8 / ticksPerSec)
			if bytesPerTick < 64 {
				bytesPerTick = 64
			}
			buf := make([]byte, bytesPerTick)
			tk := time.NewTicker(time.Second / ticksPerSec)
			defer tk.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-tk.C:
					if _, err := probeConn.Write(buf); err != nil {
						return
					}
				}
			}
		}()

		// Sampler: poll TCP_INFO every second; push results to aggregator.
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = sampler.Close() }()
			tk := time.NewTicker(time.Second)
			defer tk.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-tk.C:
					st, err := sampler.Sample()
					if err != nil {
						// Stop polling on first error — typically the
						// socket has been closed mid-shutdown.
						return
					}
					aggregator.IngestTCPInfo(st.BytesRetrans, st.BytesOut, st.RttUs, st.MinRttUs)
				}
			}
		}()
	}()
}

// reverseStreamReport builds the renderer's reverse-stream view from the
// aggregator's last-known reverse StreamView plus the server's reported
// counters in Final.ReverseSent / Final.ReverseDurationS. Returns nil
// when reverse stream wasn't active for this session — both the
// aggregator view and Sent are zero.
//
// Loss is reported as the effective max of the receiver-observed gap
// count and the server-reported (Sent - Recv) delta. The receiver only
// detects loss when a later seq arrives, so tail-loss (the final
// packets of the test never arriving) leaves v.Lost at zero even when
// Sent > Recv. Falling back to Sent - Recv recovers the true count.
// The denominator prefers Sent (server-authoritative) when available.
func reverseStreamReport(v *agg.StreamView, fs *control.FinalStats) *report.ReverseStreamReport {
	if v == nil && (fs == nil || (fs.ReverseSent == 0 && fs.ReverseDurationS == 0)) {
		return nil
	}
	out := report.ReverseStreamReport{}
	if fs != nil {
		out.Sent = fs.ReverseSent
		out.DurationS = fs.ReverseDurationS
	}
	if v != nil {
		out.Recv = v.Recv
		out.OutOfOrder = v.OutOfOrder
		out.Duplicates = v.Duplicates
		out.JitterMS = v.JitterMS
		out.LocalDrops = v.LocalDrops
	}
	var effLoss int64
	if v != nil {
		effLoss = v.Lost
	}
	if gap := out.Sent - out.Recv; gap > effLoss {
		effLoss = gap
	}
	if effLoss < 0 {
		effLoss = 0
	}
	out.Lost = effLoss
	switch {
	case out.Sent > 0:
		out.LossPct = float64(effLoss) / float64(out.Sent) * 100
	case out.Recv+effLoss > 0:
		out.LossPct = float64(effLoss) / float64(out.Recv+effLoss) * 100
	}
	return &out
}

// tcpCorroborateReport builds the renderer's TCP-corroborate view from
// the aggregator's last-seen sample. Returns nil when the aggregator
// never saw a sample (probe disabled, or sampler nop on this platform).
func tcpCorroborateReport(v *agg.TCPCorroborateView) *report.TCPCorroborateReport {
	if v == nil {
		return nil
	}
	return &report.TCPCorroborateReport{
		BytesRetrans: v.BytesRetrans,
		BytesOut:     v.BytesOut,
		RetransPct:   v.RetransPct,
		RttUs:        v.RttUs,
		MinRttUs:     v.MinRttUs,
	}
}

// suspectsToReports adapts the agg correlator's output into the renderer's
// SuspectHopReport shape. Returns nil for empty input so the report's
// Correlation field stays empty (the JSON renderer turns nil into `[]`).
func suspectsToReports(in []agg.SuspectHop) []report.SuspectHopReport {
	if len(in) == 0 {
		return nil
	}
	out := make([]report.SuspectHopReport, len(in))
	for i, s := range in {
		out[i] = report.SuspectHopReport{
			TTL:        s.TTL,
			Confidence: s.Confidence,
			Evidence:   s.Evidence,
		}
	}
	return out
}

// makeSaveFn returns the closure passed to tui.Options.SaveFn. The TUI
// only invokes it once Final has arrived, but we still guard for the
// nil pointer so a race between the user pressing [s] and Final
// publishing surfaces a clean error rather than panicking. Path
// resolves to cfg.savePath when set (matches the --save flag), else a
// generated `dropline-<sanitized-target>-<unix>.json` in the cwd.
func makeSaveFn(cfg traceConfig, builtData *atomic.Pointer[report.Data]) func() (string, error) {
	return func() (string, error) {
		d := builtData.Load()
		if d == nil {
			return "", errors.New("no final report available")
		}
		path := cfg.savePath
		if path == "" {
			path = defaultSavePath(cfg.target)
		}
		if err := writeJSONFile(path, *d); err != nil {
			return "", err
		}
		return path, nil
	}
}

// makePauseFn returns the closure passed to tui.Options.PauseFn. The
// TUI invokes it on every [p] keypress; the closure flips the local
// sender's pause gate (so the sender stops emitting packets) and
// sends a Pause wire message so the server stops forwarding Stats —
// without that, the client's sparkline would fill with zero-loss
// buckets through the pause and look misleadingly healthy.
//
// Returns the new paused state and any wire-send error. If the wire
// send fails the local gate is rolled back so client and server stay
// in lockstep.
func makePauseFn(gate *stream.PauseGate, cc *control.Conn) func() (bool, error) {
	return func() (bool, error) {
		newPaused := !gate.IsPaused()
		if newPaused {
			gate.Pause()
		} else {
			gate.Resume()
		}
		if err := cc.Send(&control.Pause{Type: control.TypePause, Paused: newPaused}); err != nil {
			// Roll back so the local sender state matches what the
			// server believes.
			if newPaused {
				gate.Resume()
			} else {
				gate.Pause()
			}
			return !newPaused, err
		}
		return newPaused, nil
	}
}

// makeCopyFn returns the closure passed to tui.Options.CopyFn. The TUI
// only invokes it once Final has arrived, but we still guard for the
// nil pointer so a race surfaces a clean error rather than panicking.
// On success the JSON report is base64-encoded into an OSC52 escape
// sequence and written to the supplied writer (typically os.Stdout);
// modern terminals (Windows Terminal, iTerm2, Alacritty, Kitty,
// WezTerm, tmux with set-clipboard on) interpret it as "set system
// clipboard"; legacy consoles (cmd.exe, conhost) silently no-op. The
// returned int is the unencoded byte count, surfaced in the TUI footer.
func makeCopyFn(builtData *atomic.Pointer[report.Data], out io.Writer) func() (int, error) {
	return func() (int, error) {
		d := builtData.Load()
		if d == nil {
			return 0, errors.New("no final report available")
		}
		var buf bytes.Buffer
		if err := report.RenderJSON(&buf, *d); err != nil {
			return 0, err
		}
		payload := buf.Bytes()
		if _, err := out.Write(osc52Encode(payload)); err != nil {
			return 0, err
		}
		return len(payload), nil
	}
}

// osc52Encode wraps payload in the OSC 52 "set clipboard" escape
// sequence: ESC ] 52 ; c ; <base64> BEL. Selection is "c" (system
// clipboard); BEL terminator is preferred over ST for compatibility
// with the modern-terminal target set documented in the slice plan.
func osc52Encode(payload []byte) []byte {
	const prefix = "\x1b]52;c;"
	const terminator = "\a"
	encoded := base64.StdEncoding.EncodeToString(payload)
	out := make([]byte, 0, len(prefix)+len(encoded)+len(terminator))
	out = append(out, prefix...)
	out = append(out, encoded...)
	out = append(out, terminator...)
	return out
}

// defaultSavePath builds the in-cwd filename used by the TUI's [s]
// keybinding when --save was not passed. The host portion of TARGET is
// sanitized so a target like `[::1]:5301` doesn't try to create a file
// path with brackets / colons on Windows.
func defaultSavePath(target string) string {
	host := target
	if h, _, err := net.SplitHostPort(target); err == nil {
		host = h
	}
	host = sanitizePathSegment(host)
	if host == "" {
		host = "target"
	}
	return fmt.Sprintf("dropline-%s-%d.json", host, time.Now().Unix())
}

// sanitizePathSegment replaces filesystem-reserved characters with `_`
// so the segment is safe on Linux and Windows alike. Conservative: any
// rune in the reserved set or a control character becomes `_`.
func sanitizePathSegment(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r < 0x20:
			b.WriteByte('_')
		case r == '/' || r == '\\' || r == ':' || r == '?' || r == '*' ||
			r == '<' || r == '>' || r == '"' || r == '|':
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func mintFlowID() (uint32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	id := binary.BigEndian.Uint32(b[:])
	if id == 0 {
		// Zero is reserved by the receiver as "match any flow"; nudge.
		id = 1
	}
	return id, nil
}

// mintToken returns a 64-bit per-session secret stamped into every UDP
// packet header. The server rejects token==0 at admission time, so the
// (astronomically unlikely) zero draw is replaced with 1.
func mintToken() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	t := binary.BigEndian.Uint64(b[:])
	if t == 0 {
		t = 1
	}
	return t, nil
}
