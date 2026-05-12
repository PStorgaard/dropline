package stream

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultRingSize is the drainer→stats channel capacity. Sized to absorb
// short stalls in the stats goroutine without dropping packets at the
// canonical 1042 pps × 60s working point.
const DefaultRingSize = 4096

// DefaultSocketBuf is the SO_RCVBUF target per spec. Set best-effort; on
// Linux the effective cap is /proc/sys/net/core/rmem_max — surfacing a
// warning when capped lower is a follow-up (see progress.md).
const DefaultSocketBuf = 16 << 20

// DefaultTick is the periodic snapshot cadence (1 second).
const DefaultTick = time.Second

// drainerShutdownPoll bounds the latency between context cancellation and
// the drainer exiting its read loop.
const drainerShutdownPoll = 100 * time.Millisecond

// ReceiverConfig parameterizes a Receiver. Zero-value fields fall back to
// the Default* constants except FlowID (0 means "accept any flow id").
type ReceiverConfig struct {
	// FlowID, if non-zero, restricts accepted packets to this flow id.
	// Packets with a different flow id are silently dropped — they are
	// neither counted as recv nor as local drops.
	FlowID uint32
	// Tick is the periodic snapshot cadence pushed onto Snapshots.
	// Zero means use DefaultTick; negative means do not emit periodically.
	Tick time.Duration
	// StartTime is t0 for elapsed-second computation. Zero = time.Now() at Run.
	StartTime time.Time
	// Snapshots receives one Snapshot per Tick. Sends are non-blocking; if
	// the consumer is slower than Tick, snapshots are silently dropped —
	// the consumer should never gate the receiver path.
	Snapshots chan<- Snapshot
	// RingSize sizes the drainer→stats channel; zero = DefaultRingSize.
	RingSize int
	// SocketBufBytes is the SO_RCVBUF target; zero = DefaultSocketBuf.
	// Negative means do not call SetReadBuffer.
	SocketBufBytes int
	// KernelDrops samples cumulative kernel-side UDP drops. Default = nopSampler.
	KernelDrops KernelDropSampler
}

// Receiver runs a UDP loss-test server in one of two modes:
//   - direct-conn (NewReceiver): owns the drainer goroutine that
//     ReadFromUDPs the socket and decodes headers itself.
//   - hub-fed (NewReceiverFromHub): consumes pre-decoded observations
//     from a stream.Hub's per-flow channel, used by multi-session
//     `dropline serve` so one socket fans out to N receivers.
//
// In both modes the stat goroutine + snapshot ticker behave identically.
// See package doc for the concurrency contract.
type Receiver struct {
	conn *net.UDPConn // direct-conn mode only
	hub  *HubFlow     // hub-fed mode only; mutually exclusive with conn
	cfg  ReceiverConfig
	acc  *Accumulator
}

// NewReceiver wraps an already-bound *net.UDPConn. The Receiver does not
// manage the socket's lifecycle — the caller closes it.
func NewReceiver(conn *net.UDPConn, cfg ReceiverConfig) (*Receiver, error) {
	if conn == nil {
		return nil, errors.New("stream: nil conn")
	}
	cfg, err := applyReceiverDefaults(cfg)
	if err != nil {
		return nil, err
	}
	return &Receiver{conn: conn, cfg: cfg}, nil
}

// NewReceiverFromHub builds a Receiver that consumes observations from a
// Hub-registered flow. The caller is responsible for releasing the
// HubFlow when the session ends; the Receiver does not call Release.
//
// In this mode the receiver does not call ReadFromUDP — the hub's single
// drainer is the only socket reader. SocketBufBytes / FlowID config
// fields are ignored (the hub already owns those concerns).
func NewReceiverFromHub(hf *HubFlow, cfg ReceiverConfig) (*Receiver, error) {
	if hf == nil {
		return nil, errors.New("stream: nil hub flow")
	}
	cfg, err := applyReceiverDefaults(cfg)
	if err != nil {
		return nil, err
	}
	return &Receiver{hub: hf, cfg: cfg}, nil
}

// applyReceiverDefaults fills zero-valued ReceiverConfig fields with
// the package-level Default* constants. Mode-specific fields
// (SocketBufBytes, FlowID) still get defaults; the modes that don't use
// them simply ignore the value. Negative RingSize is an error to match
// the previous constructor contract.
func applyReceiverDefaults(cfg ReceiverConfig) (ReceiverConfig, error) {
	if cfg.RingSize == 0 {
		cfg.RingSize = DefaultRingSize
	}
	if cfg.RingSize < 1 {
		return cfg, errors.New("stream: ring size must be > 0")
	}
	if cfg.Tick == 0 {
		cfg.Tick = DefaultTick
	}
	if cfg.SocketBufBytes == 0 {
		cfg.SocketBufBytes = DefaultSocketBuf
	}
	if cfg.KernelDrops == nil {
		cfg.KernelDrops = nopSampler{}
	}
	return cfg, nil
}

// Accumulator returns the underlying counter store. Safe to call before
// Run (returns nil) or after (returns the populated accumulator).
func (r *Receiver) Accumulator() *Accumulator { return r.acc }

// Run starts the stats goroutine (and the drainer in direct-conn mode),
// emits periodic snapshots, and blocks until ctx is canceled. It returns
// a final Snapshot taken after the goroutine(s) have exited.
//
// In hub-fed mode the receiver does not own a drainer — it consumes
// observations directly from the HubFlow's channel and pulls
// ring-overflow drops from the hub on each snapshot tick.
func (r *Receiver) Run(ctx context.Context) (Snapshot, error) {
	t0 := r.cfg.StartTime
	if t0.IsZero() {
		t0 = time.Now()
	}
	r.acc = NewAccumulator(t0)

	// Wrap the configured sampler so kernel_drops reports the delta
	// over the test window. The underlying samplers read host-wide
	// protocol counters; without baselining they'd inherit pre-test
	// drops and drops from unrelated UDP sockets.
	kernelDrops := newBaselineSampler(r.cfg.KernelDrops)

	var (
		obsCh       <-chan observation
		drainerErr  atomic.Value
		drainerWG   sync.WaitGroup
		statsCh     chan observation
		statsWG     sync.WaitGroup
		ownsStatsCh bool
	)

	switch {
	case r.conn != nil:
		if r.cfg.SocketBufBytes > 0 {
			_ = r.conn.SetReadBuffer(r.cfg.SocketBufBytes) // best-effort
		}
		statsCh = make(chan observation, r.cfg.RingSize)
		obsCh = statsCh
		ownsStatsCh = true
		drainerWG.Add(1)
		go r.drain(ctx, statsCh, &drainerWG, &drainerErr)
	case r.hub != nil:
		obsCh = r.hub.Observations()
	default:
		return Snapshot{}, errors.New("stream: receiver has neither conn nor hub flow")
	}

	statsWG.Add(1)
	if r.conn != nil {
		// Direct-conn: stat consumes the drainer's channel via range; it
		// exits when Run closes the channel below, after the drainer has
		// finished flushing tail packets.
		go r.stat(obsCh, &statsWG)
	} else {
		// Hub-fed: the channel's lifecycle is owned by the HubFlow caller
		// (typically released after Run returns), so stat must also honor
		// ctx — best-effort drain on cancel and exit.
		go r.statHub(ctx, obsCh, &statsWG)
	}

	var tickC <-chan time.Time
	if r.cfg.Tick > 0 && r.cfg.Snapshots != nil {
		ticker := time.NewTicker(r.cfg.Tick)
		defer ticker.Stop()
		tickC = ticker.C
	}

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case t := <-tickC:
			r.applyHubLocalDrops()
			if n, err := kernelDrops.Sample(); err == nil {
				r.acc.SetKernelDrops(n)
			}
			select {
			case r.cfg.Snapshots <- r.acc.Snapshot(t):
			default: // consumer is slow — drop; never gate the receiver
			}
		}
	}

	if r.conn != nil {
		drainerWG.Wait()
	}
	if ownsStatsCh {
		close(statsCh)
	}
	statsWG.Wait()

	r.applyHubLocalDrops()
	if n, err := kernelDrops.Sample(); err == nil {
		r.acc.SetKernelDrops(n)
	}
	final := r.acc.Snapshot(time.Now())
	if e, _ := drainerErr.Load().(error); e != nil {
		return final, e
	}
	return final, nil
}

// applyHubLocalDrops pulls any per-flow ring-overflow drops the hub has
// counted since the last call and forwards them to the accumulator. No-op
// in direct-conn mode (the drainer accounts directly).
func (r *Receiver) applyHubLocalDrops() {
	if r.hub == nil {
		return
	}
	if n := r.hub.PullLocalDrops(); n > 0 {
		r.acc.AddLocalDrop(n)
	}
}

// observation is the drainer→stats payload: the decoded header plus the
// receiver's wall-clock at read time. Body bytes are discarded by the
// drainer — header decode is the only "parsing" allowed in that hot path.
type observation struct {
	h       Header
	arrival time.Time
}

func (r *Receiver) drain(ctx context.Context, ch chan<- observation, wg *sync.WaitGroup, errOut *atomic.Value) {
	defer wg.Done()
	buf := make([]byte, MaxPacketSize)
	for {
		if ctx.Err() != nil {
			return
		}
		if err := r.conn.SetReadDeadline(time.Now().Add(drainerShutdownPoll)); err != nil {
			errOut.Store(err)
			return
		}
		n, _, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			errOut.Store(err)
			return
		}
		if n < HeaderSize {
			continue
		}
		h, derr := DecodeHeader(buf[:HeaderSize])
		if derr != nil {
			continue
		}
		if r.cfg.FlowID != 0 && h.FlowID != r.cfg.FlowID {
			continue
		}
		obs := observation{h: h, arrival: time.Now()}
		select {
		case ch <- obs:
		default:
			r.acc.AddLocalDrop(1)
		}
	}
}

func (r *Receiver) stat(ch <-chan observation, wg *sync.WaitGroup) {
	defer wg.Done()
	for obs := range ch {
		r.acc.Observe(obs.h, obs.arrival)
	}
}

// statHub is the hub-fed counterpart of stat. The channel is owned by
// the HubFlow caller, so we exit on ctx cancel rather than waiting for
// the producer to close. Before exiting we drain whatever the hub has
// already buffered so tail packets aren't silently lost.
func (r *Receiver) statHub(ctx context.Context, ch <-chan observation, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case obs, ok := <-ch:
					if !ok {
						return
					}
					r.acc.Observe(obs.h, obs.arrival)
				default:
					return
				}
			}
		case obs, ok := <-ch:
			if !ok {
				return
			}
			r.acc.Observe(obs.h, obs.arrival)
		}
	}
}

func isTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return false
}
