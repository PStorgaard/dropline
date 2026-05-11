//go:build !windows

package probe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// probeIDCounter assigns a unique 16-bit ICMP echo identifier to each
// Prober in the process. Seeded from PID so two dropline processes on
// the same host start in different id windows; each Prober gets the
// next atomic value mod 65536. Multi-session servers (default
// --max-sessions=4) create concurrent Probers; raw ICMP sockets receive
// all matching traffic, so without unique ids one Prober would silently
// consume another's replies.
var probeIDCounter atomic.Uint32

func init() {
	probeIDCounter.Store(uint32(os.Getpid())) // #nosec G115 -- pid fits in uint32
}

func nextProbeID() uint16 {
	return uint16(probeIDCounter.Add(1) & 0xffff) // #nosec G115 -- masked
}

type inflightProbe struct {
	ttl    int
	sentAt time.Time
}

// Prober is the forward ICMP TTL-walker. Construct with New, drive with
// Run, and Close the underlying socket if Run is not used.
//
// Per spec § Probe layer the global seq is unique across the whole
// test (not per-hop) so demux is unambiguous.
type Prober struct {
	cfg  Config
	id   uint16
	conn *icmp.PacketConn
	pc4  *ipv4.PacketConn
	out  chan<- Snapshot

	mu       sync.Mutex
	nextSeq  uint16
	inflight map[uint16]inflightProbe
	hops     []*hopState
	// terminus is the smallest TTL that has ever returned EchoReply,
	// or 0 if none. Subsequent sweeps cap at this TTL.
	terminus int
}

// New opens the raw ICMPv4 socket and prepares hop state. Callers must
// either call Run (which closes the socket on exit) or Close. The
// socket open requires raw-ICMP capability — privcheck.RawICMP() is
// the right gate; this constructor returns the open error verbatim if
// the privilege is missing.
//
// out may be nil; in that case Run runs but emits nothing.
func New(cfg Config, out chan<- Snapshot) (*Prober, error) {
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

	conn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		return nil, fmt.Errorf("probe: open icmp socket: %w", err)
	}
	pc4 := conn.IPv4PacketConn()
	if pc4 == nil {
		_ = conn.Close()
		return nil, errors.New("probe: ipv4 packet conn unavailable")
	}

	hops := make([]*hopState, cfg.MaxHops)
	for i := range hops {
		hops[i] = &hopState{}
	}

	return &Prober{
		cfg:      cfg,
		id:       nextProbeID(),
		conn:     conn,
		pc4:      pc4,
		out:      out,
		nextSeq:  1,
		inflight: make(map[uint16]inflightProbe),
		hops:     hops,
	}, nil
}

// Close releases the underlying socket. Safe to call multiple times.
func (p *Prober) Close() error {
	if p.conn == nil {
		return nil
	}
	return p.conn.Close()
}

// Run drives the prober until ctx is cancelled. Returns nil on a clean
// cancellation. The socket is closed on exit regardless of how Run
// terminated.
func (p *Prober) Run(ctx context.Context) error {
	defer func() { _ = p.conn.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.recvLoop(ctx)
	}()

	ticker := time.NewTicker(p.cfg.MTRInterval)
	defer ticker.Stop()

	// Send the first sweep immediately so the hop table starts filling
	// before the first ticker fire.
	at := p.sweep()
	p.emit(at)

	for {
		select {
		case <-ctx.Done():
			_ = p.conn.Close() // unblock recvLoop
			wg.Wait()
			return nil
		case <-ticker.C:
			at := p.sweep()
			p.emit(at)
		}
	}
}

// sweep sends one ICMP echo per TTL from 1 up to MaxHops (or the
// current terminus, whichever is smaller). All probes go out
// back-to-back; v1 makes no attempt to throttle within a sweep.
// Returns the moment the sweep began so emit() can stamp the resulting
// Snapshot with the sweep-start time rather than the post-prune time.
func (p *Prober) sweep() time.Time {
	at := time.Now()
	target := &net.IPAddr{IP: p.cfg.Target}
	p.mu.Lock()
	maxTTL := len(p.hops)
	if p.terminus > 0 && p.terminus < maxTTL {
		maxTTL = p.terminus
	}
	p.mu.Unlock()
	for ttl := 1; ttl <= maxTTL; ttl++ {
		p.send(target, ttl)
	}
	return at
}

// send transmits one ICMP echo with the given TTL, allocates a fresh
// global seq, and registers the inflight entry. Marshal/SetTTL/WriteTo
// errors are swallowed — a single bad send shows up as a recv miss in
// the hop's loss stats and surfaces naturally.
func (p *Prober) send(target net.Addr, ttl int) {
	p.mu.Lock()
	seq := p.nextSeq
	p.nextSeq++
	if p.nextSeq == 0 {
		// Wrap to 1 so the zero seq isn't reused (defensive — at
		// MaxHops=30 sweeps/sec this takes ~36 minutes).
		p.nextSeq = 1
	}
	p.inflight[seq] = inflightProbe{ttl: ttl, sentAt: time.Now()}
	p.hops[ttl-1].sent++
	p.hops[ttl-1].inflight++
	p.mu.Unlock()

	msg := icmp.Message{
		Type: ipv4.ICMPTypeEcho,
		Code: 0,
		Body: &icmp.Echo{
			ID:  int(p.id),
			Seq: int(seq),
		},
	}
	wb, err := msg.Marshal(nil)
	if err != nil {
		return
	}
	if err := p.pc4.SetTTL(ttl); err != nil {
		return
	}
	_, _ = p.conn.WriteTo(wb, target)
}

// recvLoop reads ICMP packets, classifies them, and folds replies into
// hop state. Exits when the socket is closed or ctx is cancelled.
//
// A counter throttles persistent transient errors: the first few errors
// are retried immediately (one-off network blips shouldn't add latency),
// but after recvErrBackoffThreshold consecutive errors the loop sleeps
// recvErrBackoff between reads so a stuck socket doesn't burn a core.
func (p *Prober) recvLoop(ctx context.Context) {
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
		r := parseReply(buf[:n], src, p.id)
		if r.kind == replyIgnore {
			continue
		}

		p.mu.Lock()
		fp, ok := p.inflight[r.seq]
		if !ok {
			p.mu.Unlock()
			continue
		}
		delete(p.inflight, r.seq)
		ttlIdx := fp.ttl - 1
		if ttlIdx < 0 || ttlIdx >= len(p.hops) {
			p.mu.Unlock()
			continue
		}
		rttMS := float64(now.Sub(fp.sentAt)) / float64(time.Millisecond)
		isEcho := r.kind == replyEchoReply
		p.hops[ttlIdx].inflight--
		p.hops[ttlIdx].recordReply(src, rttMS, isEcho)
		if isEcho && (p.terminus == 0 || fp.ttl < p.terminus) {
			p.terminus = fp.ttl
		}
		p.mu.Unlock()
	}
}

// recvErrBackoffThreshold and recvErrBackoff govern recvLoop's
// degradation on persistent transient ReadFrom errors. The first few
// errors retry immediately; once the consecutive-error count crosses the
// threshold, the loop sleeps between reads so a stuck socket doesn't
// burn a core for the rest of the test.
const (
	recvErrBackoffThreshold = 5
	recvErrBackoff          = 50 * time.Millisecond
)

// pruneInflightLocked ages out probes older than cutoff. p.mu must be
// held. Removes the entry from p.inflight and decrements the matching
// hop's inflight counter so the next snapshot's "settled" denominator
// grows and the gap surfaces as loss.
//
// The map delete is load-bearing: a late reply that arrives after its
// inflight entry was pruned hits recvLoop's `if !ok` branch and is
// dropped, preventing a double-decrement of the per-hop inflight counter.
func (p *Prober) pruneInflightLocked(cutoff time.Time) {
	for seq, fp := range p.inflight {
		if !fp.sentAt.Before(cutoff) {
			continue
		}
		delete(p.inflight, seq)
		ttlIdx := fp.ttl - 1
		if ttlIdx < 0 || ttlIdx >= len(p.hops) {
			continue
		}
		p.hops[ttlIdx].inflight--
	}
}

// emit pushes a Snapshot stamped with `at` (the sweep-start time) to the
// out channel. Non-blocking — slow consumers cause snapshot drops,
// matching the rule in agg/stream.
//
// Pruning runs unconditionally even when out is nil; otherwise inflight
// would grow unbounded and lossPct would stay 0 for a no-consumer Prober.
func (p *Prober) emit(at time.Time) {
	p.mu.Lock()
	p.pruneInflightLocked(time.Now().Add(-lossTimeout(p.cfg.MTRInterval)))
	if p.out == nil {
		p.mu.Unlock()
		return
	}
	snap := buildSnapshot(p.hops, p.terminus, at)
	p.mu.Unlock()
	select {
	case p.out <- snap:
	default:
	}
}
