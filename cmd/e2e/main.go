// Command e2e is a black-box smoke test: it builds nothing on its own,
// expects the dropline binary at $DROPLINE_BIN (or a host-default path),
// spawns `dropline serve` on a free loopback port, runs `dropline trace
// --json` against it, and asserts a few invariants on the resulting
// JSON. Intended to be invoked via `make e2e`.
//
// On success, prints "e2e: ok" and exits 0. On failure, prints
// "e2e: <reason>" to stderr (with the captured server / trace stderr
// when relevant) and exits 1.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"
)

const (
	bannerWaitTimeout = 5 * time.Second
	traceTimeout      = 30 * time.Second
)

func main() {
	bin := os.Getenv("DROPLINE_BIN")
	if bin == "" {
		bin = defaultBin()
	}
	if err := run(bin); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %s\n", err)
		os.Exit(1)
	}
	fmt.Println("e2e: ok")
}

// defaultBin returns the host-default location of the dropline binary
// when DROPLINE_BIN isn't set. Mirrors the layout `make all` produces.
func defaultBin() string {
	if runtime.GOOS == "windows" {
		return filepath.Join("dist", "dropline-windows-amd64.exe")
	}
	return filepath.Join("dist", "dropline-linux-amd64")
}

func run(bin string) error {
	if _, err := os.Stat(bin); err != nil {
		return fmt.Errorf("dropline binary %q not found (set DROPLINE_BIN or run `make all` first): %w", bin, err)
	}
	port, err := pickFreePort()
	if err != nil {
		return fmt.Errorf("pick free port: %w", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := startServer(bin, addr)
	if err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	defer srv.kill()

	if err := srv.waitForBanner(bannerWaitTimeout); err != nil {
		return err
	}

	out, err := runTrace(bin, addr)
	if err != nil {
		return err
	}

	var doc traceJSON
	if err := json.Unmarshal(out, &doc); err != nil {
		preview := out
		if len(preview) > 200 {
			preview = preview[:200]
		}
		return fmt.Errorf("parse trace JSON: %w (first 200B: %q)", err, preview)
	}
	return assertHealthy(doc)
}

// pickFreePort asks the kernel for an ephemeral TCP port on loopback,
// closes the listener, and returns the port number. There's a small
// race between close and the server binding it, but for a one-shot e2e
// on a developer box or hosted runner that's acceptable.
func pickFreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

// serverProc owns a running `dropline serve` subprocess plus the
// stderr-banner detection state.
type serverProc struct {
	cmd        *exec.Cmd
	stderrBuf  *bytes.Buffer
	bannerC    chan struct{}
	bannerOnce sync.Once
}

// bannerRegex matches the line the server prints once it has bound both
// sockets. Keep this prefix in lockstep with cmd/dropline/serve.go.
var bannerRegex = regexp.MustCompile(`dropline serve: listening on `)

func startServer(bin, addr string) (*serverProc, error) {
	cmd := exec.Command(bin, "serve", "--listen", addr)
	sp := &serverProc{
		cmd:       cmd,
		stderrBuf: &bytes.Buffer{},
		bannerC:   make(chan struct{}),
	}
	pr, pw := io.Pipe()
	cmd.Stderr = io.MultiWriter(sp.stderrBuf, pw)
	cmd.Stdout = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go sp.scan(pr)
	return sp, nil
}

func (sp *serverProc) scan(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if bannerRegex.MatchString(scanner.Text()) {
			sp.bannerOnce.Do(func() { close(sp.bannerC) })
		}
	}
}

func (sp *serverProc) waitForBanner(timeout time.Duration) error {
	select {
	case <-sp.bannerC:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("server did not print listening banner within %s; stderr:\n%s",
			timeout, sp.stderrBuf.String())
	}
}

func (sp *serverProc) kill() {
	if sp.cmd.Process != nil {
		_ = sp.cmd.Process.Kill()
	}
	_ = sp.cmd.Wait()
}

// runTrace invokes `dropline trace addr --rate 1M --duration 2s --json`
// and returns the captured stdout on success. On non-zero exit it
// returns an error wrapping the captured stderr — that's where the
// trace command's privcheck banner / connection-refused errors land.
func runTrace(bin, addr string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), traceTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "trace", addr,
		"--rate", "1M",
		"--duration", "2s",
		"--json",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("trace exited with error: %w; stderr:\n%s", err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// traceJSON mirrors only the fields this harness asserts on. Adding
// fields here when the JSON schema grows is intentional — we want
// surprises (renames, removals) to surface as decode-time mismatches.
type traceJSON struct {
	Stream struct {
		Recv        int64   `json:"recv"`
		LossPct     float64 `json:"loss_pct"`
		KernelDrops int64   `json:"kernel_drops"`
	} `json:"stream"`
	Correlation *struct {
		SuspectHops []any `json:"suspect_hops"`
	} `json:"correlation"`
	ReversePathStatus string `json:"reverse_path_status"`
	ReversePath       []struct {
		TTL      int    `json:"ttl"`
		Addr     string `json:"addr"`
		Sent     int64  `json:"sent"`
		Recv     int64  `json:"recv"`
		RTTMS    struct {
			Last   float64 `json:"last"`
			Best   float64 `json:"best"`
			Worst  float64 `json:"worst"`
			Avg    float64 `json:"avg"`
			StdDev float64 `json:"stddev"`
		} `json:"rtt_ms"`
		Terminus string `json:"terminus,omitempty"`
	} `json:"reverse_path"`
	ReverseStream *struct {
		Sent      int64   `json:"sent"`
		Recv      int64   `json:"recv"`
		LossPct   float64 `json:"loss_pct"`
		DurationS float64 `json:"duration_s"`
	} `json:"reverse_stream,omitempty"`
}

func assertHealthy(d traceJSON) error {
	if d.Stream.Recv == 0 {
		return errors.New("stream.recv == 0; no packets reached the receiver")
	}
	if d.Stream.LossPct >= 1.0 {
		return fmt.Errorf("stream.loss_pct = %.3f, want < 1.0 on loopback", d.Stream.LossPct)
	}
	if d.Stream.KernelDrops != 0 {
		return fmt.Errorf("stream.kernel_drops = %d, want 0 on a quiet loopback test", d.Stream.KernelDrops)
	}
	if d.Correlation == nil {
		return errors.New("correlation field missing in JSON")
	}
	// Reverse-path status must be one of the four shipping wire forms.
	// On a privileged host the loopback reply lands on the client IP and
	// the run resolves to "ok" with at least one terminus="host" hop. On
	// an unprivileged host the server advertises "disabled_by_server" and
	// the hop list is empty — still a passing run for the e2e.
	switch d.ReversePathStatus {
	case "ok", "terminated_at_nat", "disabled_by_server", "disabled_by_client":
	default:
		return fmt.Errorf("reverse_path_status = %q, want one of ok/terminated_at_nat/disabled_by_*", d.ReversePathStatus)
	}
	if d.ReversePathStatus == "ok" {
		if len(d.ReversePath) == 0 {
			return errors.New("reverse_path_status=ok but reverse_path is empty")
		}
		seenTerminus := false
		seenSent := false
		for _, h := range d.ReversePath {
			if h.Terminus != "" {
				seenTerminus = true
			}
			if h.Sent > 0 {
				seenSent = true
			}
		}
		if !seenTerminus {
			return errors.New("reverse_path_status=ok but no hop carries a non-empty terminus")
		}
		// Rich-shape regression guard: at least one hop must report
		// sent>0. Anything zero-everything signals an old slim-shape
		// server slipped into the build.
		if !seenSent {
			return errors.New("reverse_path_status=ok but no hop carries sent>0 (rich-shape regression)")
		}
	}
	// reverse_stream is the server→client UDP loss sub-test. On the
	// default auto/auto/auto matrix it should be present and clean on
	// loopback: at least one packet sent AND received, low loss%.
	if d.ReverseStream == nil {
		return errors.New("reverse_stream section missing (auto resolution should enable it on loopback)")
	}
	if d.ReverseStream.Sent == 0 {
		return errors.New("reverse_stream.sent == 0; server reverse-Sender never emitted")
	}
	if d.ReverseStream.Recv == 0 {
		return errors.New("reverse_stream.recv == 0; reverse packets didn't reach the client")
	}
	if d.ReverseStream.LossPct >= 1.0 {
		return fmt.Errorf("reverse_stream.loss_pct = %.3f, want < 1.0 on loopback", d.ReverseStream.LossPct)
	}
	return nil
}
