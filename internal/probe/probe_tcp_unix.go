//go:build !windows

package probe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/sys/unix"
)

// tcpProbePortBase is the high-port floor for the TCP prober's
// source-port allocator. Each TCPProber instance reserves a contiguous
// slice of size tcpProbeRangeFactor*MaxHops, claimed via the
// process-global tcpPortCursor. Started at 33434 — the classic
// UDP-traceroute base — so the range stays clear of common ephemeral
// allocations and is obvious to an operator eyeballing tcpdump.
const (
	tcpProbePortBase    uint32 = 33434
	tcpProbeRangeFactor uint32 = 4 // range = 4*MaxHops; allows lossTimeout overlap without collision
)

// tcpPortCursor walks the process-wide allocator monotonically; each
// TCPProber claims one slice of size tcpProbeRangeFactor*MaxHops. Wraps
// modulo the high-port capacity so a long-running server with many
// sessions doesn't run off the end of the 16-bit port space.
var tcpPortCursor atomic.Uint32

type tcpInflight struct {
	ttl    int
	sentAt time.Time
}

// TCPProber walks TTL 1..MaxHops once per MTRInterval tick, sending one
// TCP SYN per TTL via the kernel TCP stack (Dialer + IP_TTL +
// SO_LINGER=0) and demuxing the ICMP TimeExceeded responses on a
// dedicated raw ICMP socket. Demux key is the bound TCP source port:
// each probe binds an explicit port from this prober's reserved range,
// and the ICMP TimeExceeded's embedded original packet carries that
// port (per RFC 792's "original IP header + first 8 bytes of transport
// header" guarantee), letting recvLoop attribute the reply to the
// originating TTL. Terminus is detected directly from the dial result:
// successful handshake or ECONNREFUSED/ECONNRESET both indicate the
// destination was reached, the equivalent of an ICMP EchoReply for the
// classic ICMP prober.
type TCPProber struct {
	cfg  TCPConfig
	conn *icmp.PacketConn
	out  chan<- TCPSnapshot

	srcPortBase  uint16
	srcPortRange uint16

	mu         sync.Mutex
	portCursor uint16
	inflight   map[uint16]tcpInflight
	hops       []*hopState
	terminus   int

	closeOnce sync.Once
}

// nextTCPSrcPortBase reserves a fresh source-port range for a newly
// constructed TCPProber. Concurrent probers (forward + reverse on a
// server) get disjoint ranges by virtue of the process-global cursor.
func nextTCPSrcPortBase(maxHops int) uint16 {
	rangeSize := uint32(maxHops) * tcpProbeRangeFactor
	if rangeSize == 0 {
		rangeSize = tcpProbeRangeFactor
	}
	// Keep 1024 ports clear at the top of the 16-bit space for the
	// kernel's own ephemeral allocations.
	const cap = 65535 - tcpProbePortBase - 1024
	cursor := tcpPortCursor.Add(1) - 1
	offset := (cursor * rangeSize) % cap
	return uint16(tcpProbePortBase + offset) // #nosec G115 -- value is bounded by cap < 65535
}

// NewTCP opens the dedicated raw ICMPv4 socket and prepares per-hop
// state. Requires the same raw-ICMP capability as the ICMP Prober —
// privcheck.RawICMP() is the appropriate gate; this constructor
// returns the open error verbatim if the privilege is missing.
//
// out may be nil; in that case Run runs but emits nothing.
func NewTCP(cfg TCPConfig, out chan<- TCPSnapshot) (*TCPProber, error) {
	if cfg.Target == nil {
		return nil, errors.New("probe: nil target")
	}
	if cfg.Target.To4() == nil {
		return nil, errors.New("probe: ipv6 target unsupported in v1")
	}
	if cfg.MaxHops < 1 || cfg.MaxHops > 255 {
		return nil, fmt.Errorf("probe: max hops out of range: %d", cfg.MaxHops)
	}
	if cfg.MTRInterval <= 0 {
		return nil, errors.New("probe: mtr interval must be > 0")
	}
	if cfg.Port == 0 {
		return nil, errors.New("probe: tcp hop probe port must be > 0")
	}

	conn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		return nil, fmt.Errorf("probe: open icmp socket: %w", err)
	}

	hops := make([]*hopState, cfg.MaxHops)
	for i := range hops {
		hops[i] = &hopState{}
	}

	rangeSize := uint32(cfg.MaxHops) * tcpProbeRangeFactor
	return &TCPProber{
		cfg:          cfg,
		conn:         conn,
		out:          out,
		srcPortBase:  nextTCPSrcPortBase(cfg.MaxHops),
		srcPortRange: uint16(rangeSize), // #nosec G115 -- rangeSize <= 255*4 = 1020
		inflight:     make(map[uint16]tcpInflight),
		hops:         hops,
	}, nil
}

// Close releases the underlying raw ICMP socket. Safe to call multiple
// times.
func (p *TCPProber) Close() error {
	var err error
	p.closeOnce.Do(func() {
		if p.conn != nil {
			err = p.conn.Close()
		}
	})
	return err
}

// Run drives the prober until ctx is cancelled. Returns nil on a clean
// cancellation; non-nil when the send path has been blacked out for
// more than sendErrBackoffThreshold consecutive sweeps (same escalation
// model as the ICMP prober in probe_unix.go). The raw ICMP socket is
// closed on exit regardless of how Run terminated.
func (p *TCPProber) Run(ctx context.Context) error {
	defer func() { _ = p.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.recvLoop(ctx)
	}()

	ticker := time.NewTicker(p.cfg.MTRInterval)
	defer ticker.Stop()

	consecBadSweeps := 0

	// First sweep fires immediately so the hop table starts filling
	// before the first ticker tick.
	at, allFailed := p.sweep(ctx)
	p.emit(at)
	if allFailed {
		consecBadSweeps++
	}

	for {
		select {
		case <-ctx.Done():
			_ = p.conn.Close() // unblock recvLoop
			wg.Wait()
			return nil
		case <-ticker.C:
			at, allFailed := p.sweep(ctx)
			p.emit(at)
			if allFailed {
				consecBadSweeps++
				if consecBadSweeps > sendErrBackoffThreshold {
					_ = p.conn.Close()
					wg.Wait()
					return fmt.Errorf("probe: %d consecutive tcp-hop sweeps fully failed", consecBadSweeps)
				}
			} else {
				consecBadSweeps = 0
			}
		}
	}
}

// sweep fires one TCP probe per TTL (1..maxTTL) concurrently and
// returns when all probes have terminated or timed out. Returns the
// sweep-start time (so emit stamps the resulting Snapshot with
// sweep-start rather than post-prune time) and whether every probe in
// the sweep failed at the bind / sockopt stage (the signal for Run to
// escalate persistent send blackout).
func (p *TCPProber) sweep(ctx context.Context) (time.Time, bool) {
	at := time.Now()
	p.mu.Lock()
	maxTTL := len(p.hops)
	if p.terminus > 0 && p.terminus < maxTTL {
		maxTTL = p.terminus
	}
	p.mu.Unlock()
	if maxTTL == 0 {
		return at, false
	}

	var failures atomic.Int32
	var wg sync.WaitGroup
	for ttl := 1; ttl <= maxTTL; ttl++ {
		wg.Add(1)
		go func(ttl int) {
			defer wg.Done()
			if err := p.probe(ctx, ttl); err != nil {
				failures.Add(1)
			}
		}(ttl)
	}
	wg.Wait()
	return at, int(failures.Load()) == maxTTL
}

// nextSrcPort allocates the next port slot from the prober's reserved
// range, wrapping monotonically. The range is sized to 4*MaxHops so a
// previously-used port's inflight entry has fully expired (via
// lossTimeout) before the slot is reused — see CLAUDE.md.
func (p *TCPProber) nextSrcPort() uint16 {
	p.mu.Lock()
	defer p.mu.Unlock()
	port := p.srcPortBase + (p.portCursor % p.srcPortRange)
	p.portCursor++
	return port
}

// probe sends one TCP SYN at the given TTL and folds the outcome into
// hop state. The kernel's TCP stack handles SYN framing, sequence
// numbers, and checksums; we only set IP_TTL and SO_LINGER on the
// socket before connect. SO_LINGER {Onoff:1, Linger:0} ensures a
// completed handshake closes via RST (no TIME_WAIT pressure across
// thousands of probes per test). The bound source port is the demux
// key for the ICMP TimeExceeded recvLoop. Returns a non-nil error only
// when the socket-option configuration itself failed (probe never went
// on the wire) — those bubble up so sweep can escalate persistent
// platform issues like IP_TTL being rejected.
func (p *TCPProber) probe(ctx context.Context, ttl int) error {
	srcPort := p.nextSrcPort()
	sentAt := time.Now()

	p.mu.Lock()
	p.inflight[srcPort] = tcpInflight{ttl: ttl, sentAt: sentAt}
	p.hops[ttl-1].sent++
	p.hops[ttl-1].inflight++
	p.mu.Unlock()

	ctrl := func(network, address string, c syscall.RawConn) error {
		var sockErr error
		err := c.Control(func(fd uintptr) {
			// SO_LINGER {Onoff:1, Linger:0} forces close to send RST
			// instead of FIN. Set before connect so it applies even when
			// the handshake completes immediately (loopback).
			if e := unix.SetsockoptLinger(int(fd), unix.SOL_SOCKET, unix.SO_LINGER, &unix.Linger{Onoff: 1, Linger: 0}); e != nil {
				sockErr = e
				return
			}
			if e := unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_TTL, ttl); e != nil {
				sockErr = e
				return
			}
		})
		if err != nil {
			return err
		}
		return sockErr
	}

	dialer := net.Dialer{
		Timeout:   lossTimeout(p.cfg.MTRInterval),
		LocalAddr: &net.TCPAddr{Port: int(srcPort)},
		Control:   ctrl,
	}
	dest := net.JoinHostPort(p.cfg.Target.String(), strconv.Itoa(int(p.cfg.Port)))
	conn, err := dialer.DialContext(ctx, "tcp4", dest)
	rttMS := float64(time.Since(sentAt)) / float64(time.Millisecond)

	if err == nil {
		_ = conn.Close()
		p.recordTerminus(srcPort, ttl, p.cfg.Target, rttMS)
		return nil
	}

	if isTCPTerminusError(err) {
		p.recordTerminus(srcPort, ttl, p.cfg.Target, rttMS)
		return nil
	}

	if isBindCollisionError(err) {
		// Port collision (another process grabbed our range slot before
		// we could bind). Roll back so this TTL's sent counter isn't
		// inflated; next sweep advances the cursor and gets a fresh slot.
		p.rollbackProbe(srcPort, ttl)
		return nil
	}

	if isControlSetupError(err) {
		// IP_TTL or SO_LINGER rejected by the kernel. The probe never
		// went on the wire; surface as sweep failure so persistent
		// platform issues escalate.
		p.rollbackProbe(srcPort, ttl)
		return err
	}

	// Expected unreachable / timeout / context-cancel: the dedicated
	// raw ICMP recvLoop owns hop attribution from here. Leave the
	// inflight entry in place; recvLoop will pop it when the ICMP
	// arrives, or emit() will prune it as loss after lossTimeout.
	return nil
}

func (p *TCPProber) rollbackProbe(srcPort uint16, ttl int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.inflight[srcPort]; !ok {
		return
	}
	delete(p.inflight, srcPort)
	if ttl < 1 || ttl > len(p.hops) {
		return
	}
	p.hops[ttl-1].sent--
	p.hops[ttl-1].inflight--
}

// recordTerminus folds a terminus reply (handshake success, RST, or
// ECONNRESET) into the hop's stats. Equivalent to the ICMP prober's
// EchoReply path.
func (p *TCPProber) recordTerminus(srcPort uint16, ttl int, src net.IP, rttMS float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.inflight[srcPort]; !ok {
		// Already pruned or recorded via the recvLoop (rare race when
		// an asymmetric path delivers TimeExceeded for a probe that
		// also reaches the destination).
		return
	}
	delete(p.inflight, srcPort)
	if ttl < 1 || ttl > len(p.hops) {
		return
	}
	p.hops[ttl-1].inflight--
	p.hops[ttl-1].recordReply(src, rttMS, true)
	if p.terminus == 0 || ttl < p.terminus {
		p.terminus = ttl
	}
}

// isTCPTerminusError tests whether a dial error means the destination
// host was reached but rejected the connection (RST). Matched against
// syscall.Errno values rather than string-compared so the test is
// stable across glibc/musl/macOS variations.
func isTCPTerminusError(err error) bool {
	var sysErr syscall.Errno
	if !errors.As(err, &sysErr) {
		return false
	}
	switch sysErr {
	case syscall.ECONNREFUSED, syscall.ECONNRESET:
		return true
	}
	return false
}

// isBindCollisionError catches EADDRINUSE / EADDRNOTAVAIL from the
// kernel when our reserved source port is currently held by another
// socket. Transient; the next sweep advances the port cursor and
// usually clears.
func isBindCollisionError(err error) bool {
	var sysErr syscall.Errno
	if !errors.As(err, &sysErr) {
		return false
	}
	switch sysErr {
	case syscall.EADDRINUSE, syscall.EADDRNOTAVAIL:
		return true
	}
	return false
}

// isControlSetupError catches socket-option failures from the Control
// hook (IP_TTL or SO_LINGER rejected). Distinct from runtime network
// failures (HOSTUNREACH, TIMEDOUT, etc.) — those mean the probe DID go
// on the wire and the recvLoop owns attribution.
func isControlSetupError(err error) bool {
	var sysErr syscall.Errno
	if !errors.As(err, &sysErr) {
		return false
	}
	switch sysErr {
	case syscall.EPERM, syscall.EACCES, syscall.ENOPROTOOPT:
		return true
	}
	return false
}

// recvLoop reads ICMP packets, classifies them as TCP-TimeExceeded via
// parseTCPReply, and folds the resulting hop attribution into hop
// state. Exits when the raw socket is closed or ctx is cancelled.
// Mirrors Prober.recvLoop in probe_unix.go including the
// recvErrBackoffThreshold throttling for persistent transient errors.
func (p *TCPProber) recvLoop(ctx context.Context) {
	buf := make([]byte, 1500)
	consecutiveErrs := 0
	backoff := time.NewTimer(0)
	if !backoff.Stop() {
		<-backoff.C
	}
	defer backoff.Stop()
	for {
		n, peer, err := p.conn.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return
			}
			consecutiveErrs++
			if consecutiveErrs > recvErrBackoffThreshold {
				backoff.Reset(recvErrBackoff)
				select {
				case <-ctx.Done():
					if !backoff.Stop() {
						<-backoff.C
					}
					return
				case <-backoff.C:
				}
			}
			continue
		}
		consecutiveErrs = 0
		var src net.IP
		if a, ok := peer.(*net.IPAddr); ok {
			src = a.IP
		}
		now := time.Now()
		r := parseTCPReply(buf[:n], src)
		if r.kind == tcpReplyIgnore {
			continue
		}

		p.mu.Lock()
		fp, ok := p.inflight[r.srcPort]
		if !ok {
			p.mu.Unlock()
			continue
		}
		delete(p.inflight, r.srcPort)
		ttlIdx := fp.ttl - 1
		if ttlIdx < 0 || ttlIdx >= len(p.hops) {
			p.mu.Unlock()
			continue
		}
		rttMS := float64(now.Sub(fp.sentAt)) / float64(time.Millisecond)
		p.hops[ttlIdx].inflight--
		p.hops[ttlIdx].recordReply(src, rttMS, false)
		p.mu.Unlock()
	}
}

// pruneTCPInflightLocked ages out probes older than cutoff. p.mu must
// be held. Mirrors Prober.pruneInflightLocked in probe_unix.go: the
// map delete is load-bearing — a late reply that arrives after its
// inflight entry was pruned hits recvLoop's `if !ok` branch and is
// dropped, preventing a double-decrement of the per-hop inflight
// counter.
func (p *TCPProber) pruneTCPInflightLocked(cutoff time.Time) {
	for port, fp := range p.inflight {
		if !fp.sentAt.Before(cutoff) {
			continue
		}
		delete(p.inflight, port)
		ttlIdx := fp.ttl - 1
		if ttlIdx < 0 || ttlIdx >= len(p.hops) {
			continue
		}
		p.hops[ttlIdx].inflight--
	}
}

// emit pushes a TCPSnapshot stamped with the sweep-start time to the
// out channel. Non-blocking — slow consumers drop snapshots rather
// than stall the prober. Pruning runs unconditionally even when out is
// nil so inflight doesn't grow unbounded for a no-consumer prober.
func (p *TCPProber) emit(at time.Time) {
	p.mu.Lock()
	p.pruneTCPInflightLocked(time.Now().Add(-lossTimeout(p.cfg.MTRInterval)))
	if p.out == nil {
		p.mu.Unlock()
		return
	}
	snap := buildSnapshot(p.hops, p.terminus, at)
	tsnap := TCPSnapshot{
		At:        at,
		Supported: true,
		Port:      p.cfg.Port,
		Hops:      snap.Hops,
	}
	p.mu.Unlock()
	select {
	case p.out <- tsnap:
	default:
	}
}
