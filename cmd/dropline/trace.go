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
	"github.com/PStorgaard/dropline/internal/tui"
)

// defaultPort is the dropline server's default TCP control + UDP stream
// port. Chosen to sit one increment above iperf3's 5201 so a host can
// run both servers side-by-side without a port collision.
const defaultPort = "5301"

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
	target       string
	rateBPS      uint64
	duration     time.Duration
	packetSize   int
	mtrInterval  time.Duration
	maxHops      int
	output       outputMode
	savePath     string
	reverseTrace reverseTraceMode
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
	if *mtrInterval <= 0 {
		return traceConfig{}, fmt.Errorf("--mtr-interval must be > 0, got %s", *mtrInterval)
	}

	return traceConfig{
		target:       target,
		rateBPS:      rateBPS,
		duration:     *duration,
		packetSize:   *packetSize,
		mtrInterval:  *mtrInterval,
		maxHops:      *maxHops,
		output:       mode,
		savePath:     *save,
		reverseTrace: rev,
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

func parseRate(raw string) (uint64, error) {
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
	return n * mult, nil
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

	cc, err := control.Dial(ctx, cfg.target)
	if err != nil {
		return fmt.Errorf("dial control %s: %w", cfg.target, err)
	}
	defer func() { _ = cc.Close() }()

	flowID, err := mintFlowID()
	if err != nil {
		return fmt.Errorf("mint flow id: %w", err)
	}
	hello := &control.Hello{
		Type:          control.TypeHello,
		Version:       1,
		Mode:          "loss",
		RateBPS:       int64(cfg.rateBPS), // #nosec G115 -- bounded by --rate parser
		DurationMS:    cfg.duration.Milliseconds(),
		PacketSize:    cfg.packetSize,
		FlowID:        flowID,
		ReverseTrace:  cfg.reverseTrace.String(),
		MTRIntervalMS: cfg.mtrInterval.Milliseconds(),
	}
	if err := cc.Send(hello); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	first, err := cc.Recv()
	if err != nil {
		return fmt.Errorf("recv ready: %w", err)
	}
	var reversePathStatus string
	switch m := first.(type) {
	case *control.Ready:
		reversePathStatus = resolveReversePathStatus(cfg.reverseTrace.String(), m.ReverseTrace)
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
		RateBPS:    int64(cfg.rateBPS), // #nosec G115 -- bounded by --rate parser
		PacketSize: cfg.packetSize,
		Duration:   cfg.duration,
		FlowID:     flowID,
		Gate:       pauseGate,
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
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		senderErr = sender.Run(workerCtx)
	}()

	// Prober + snapshot drainer share workerCtx so a single cancel
	// stops both alongside the sender.
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		_ = prober.Run(workerCtx)
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
				RateBPS:       int64(cfg.rateBPS), // #nosec G115 -- bounded by --rate parser
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
			RateBPS:   int64(cfg.rateBPS), // #nosec G115 -- bounded by --rate parser
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
		final, serverEr, recvErr = runRecvLoop(cc, aggregator)
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
