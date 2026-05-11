package stream

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// SenderConfig parameterizes a Sender. PacketSize is the total UDP payload
// including the 32-byte header; the body (PacketSize - HeaderSize bytes) is
// generated once with crypto/rand and reused per spec § internal/stream to
// avoid NIC pattern-offload bias.
type SenderConfig struct {
	// RateBPS is the target send rate in bits per second. Must be > 0.
	RateBPS int64
	// PacketSize is total bytes per UDP datagram, in [HeaderSize, MaxPacketSize].
	PacketSize int
	// Duration bounds active send time (excludes time spent paused via
	// Gate). Zero means "run until ctx is done".
	Duration time.Duration
	// FlowID overrides the per-session random flow id when non-zero.
	FlowID uint32
	// Now is an injectable clock for tests; defaults to time.Now.
	Now func() time.Time
	// Gate, when non-nil, blocks the send loop while paused and shifts
	// the drift-free pacing schedule by the accumulated pause time.
	// Sessions without pause support pass nil.
	Gate *PauseGate
}

// Sender pushes loss-test UDP packets onto a connected *net.UDPConn at a
// configured bit rate, using busy-wait pacing for inter-packet intervals
// shorter than 1 ms and timer-based sleep otherwise. It is single-shot:
// call Run once per Sender.
type Sender struct {
	conn   *net.UDPConn
	cfg    SenderConfig
	body   []byte
	flowID uint32
	sent   atomic.Int64
}

// NewSender constructs a Sender. conn must already be connected (e.g. via
// net.DialUDP) — Sender uses Write, not WriteTo. The body of every packet
// is allocated and randomized here.
func NewSender(conn *net.UDPConn, cfg SenderConfig) (*Sender, error) {
	if conn == nil {
		return nil, errors.New("stream: nil conn")
	}
	if cfg.PacketSize < HeaderSize {
		return nil, fmt.Errorf("%w: %d < %d", ErrPacketTooSmall, cfg.PacketSize, HeaderSize)
	}
	if cfg.PacketSize > MaxPacketSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrPacketTooLarge, cfg.PacketSize, MaxPacketSize)
	}
	if cfg.RateBPS <= 0 {
		return nil, errors.New("stream: rate_bps must be > 0")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	body := make([]byte, cfg.PacketSize-HeaderSize)
	if _, err := rand.Read(body); err != nil {
		return nil, fmt.Errorf("stream: rand body: %w", err)
	}

	flowID := cfg.FlowID
	if flowID == 0 {
		var f [4]byte
		if _, err := rand.Read(f[:]); err != nil {
			return nil, fmt.Errorf("stream: rand flow_id: %w", err)
		}
		flowID = binary.BigEndian.Uint32(f[:])
	}

	return &Sender{conn: conn, cfg: cfg, body: body, flowID: flowID}, nil
}

// FlowID returns the 32-bit flow identifier stamped into every packet.
// The receiver matches against this when accepting traffic.
func (s *Sender) FlowID() uint32 { return s.flowID }

// Sent returns the cumulative count of packets successfully written.
// Safe to call from any goroutine.
func (s *Sender) Sent() int64 { return s.sent.Load() }

// Run blocks emitting packets until ctx is done or cfg.Duration elapses.
// It returns nil on clean completion (duration reached) or whichever
// error occurred (including ctx.Err()).
func (s *Sender) Run(ctx context.Context) error {
	bitsPerPacket := int64(s.cfg.PacketSize) * 8
	// interval = bitsPerPacket / RateBPS seconds → in nanoseconds:
	intervalNS := (bitsPerPacket * int64(time.Second)) / s.cfg.RateBPS
	if intervalNS < 1 {
		intervalNS = 1
	}
	interval := time.Duration(intervalNS)
	busyWait := interval < time.Millisecond

	buf := make([]byte, s.cfg.PacketSize)
	t0 := s.cfg.Now()

	var seq uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// If paused, block here. The gate's wall-clock pause window
		// folds into the offset, which shifts the drift-free schedule
		// so resume picks the cadence back up without bursting. One
		// lock acquisition per iteration in the steady state.
		var pausedFor time.Duration
		if s.cfg.Gate != nil {
			var err error
			if pausedFor, err = s.cfg.Gate.WaitAndOffset(ctx); err != nil {
				return err
			}
		}
		now := s.cfg.Now()
		// Duration is active send time: pauses extend the deadline so
		// a 60s test always emits 60s worth of packets regardless of
		// how long the user paused.
		if s.cfg.Duration > 0 && now.Sub(t0)-pausedFor >= s.cfg.Duration {
			return nil
		}

		target := t0.Add(time.Duration(seq)*interval + pausedFor)
		if err := waitUntil(ctx, target, busyWait, s.cfg.Now); err != nil {
			return err
		}

		h := Header{
			Magic:    Magic,
			FlowID:   s.flowID,
			Seq:      seq,
			TxUnixNS: s.cfg.Now().UnixNano(),
		}
		EncodeHeader(buf, h)
		copy(buf[HeaderSize:], s.body)

		if _, err := s.conn.Write(buf); err != nil {
			return fmt.Errorf("stream: write seq %d: %w", seq, err)
		}
		s.sent.Add(1)
		seq++
	}
}

// waitUntil blocks until now() >= target, honoring ctx. For target intervals
// shorter than 1 ms it busy-waits; otherwise it sleeps via a timer so other
// goroutines can run. Returns ctx.Err() if context is canceled mid-wait.
func waitUntil(ctx context.Context, target time.Time, busy bool, now func() time.Time) error {
	if busy {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !now().Before(target) {
				return nil
			}
		}
	}
	wait := target.Sub(now())
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
