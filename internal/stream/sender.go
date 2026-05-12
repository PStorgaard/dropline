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
	// Duration bounds active send time. Zero means "run until ctx is done".
	Duration time.Duration
	// FlowID overrides the per-session random flow id when non-zero.
	FlowID uint32
	// Now is an injectable clock for tests; defaults to time.Now.
	Now func() time.Time
	// WriteTarget, when non-nil, switches the inner write path from
	// conn.Write (connected socket) to conn.WriteToUDP(buf, target)
	// (unconnected socket). This is the server-side reverse-direction
	// path: the server reuses its listening UDP socket — which is bound
	// but not connected — to transmit back to a specific client address
	// captured from the forward stream. Setting WriteTarget on a
	// connected socket would yield EISCONN on Linux; leaving it nil on
	// an unconnected socket would yield EDESTADDRREQ. Constructors of
	// SenderConfig must pick the right one for the conn they pass.
	WriteTarget *net.UDPAddr
	// Token is the 64-bit per-session secret stamped into every
	// outbound packet header. Zero is the legacy/test "no token" value
	// — the server-side hub treats expectedToken=0 as "accept any".
	// Production paths (cmd/dropline/{trace,serve}.go) always pass a
	// non-zero token; tests and loopback fixtures may omit it.
	Token uint64
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

// NewSender constructs a Sender. By default conn must already be
// connected (e.g. via net.DialUDP) — Sender uses Write. If
// cfg.WriteTarget is set, Sender instead uses WriteToUDP(buf,
// WriteTarget), and conn must be an unconnected listening socket (e.g.
// from net.ListenUDP). The body of every packet is allocated and
// randomized here.
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
		now := s.cfg.Now()
		if s.cfg.Duration > 0 && now.Sub(t0) >= s.cfg.Duration {
			return nil
		}

		target := t0.Add(time.Duration(seq) * interval)
		if err := waitUntil(ctx, target, busyWait, s.cfg.Now); err != nil {
			return err
		}

		h := Header{
			Magic:    Magic,
			FlowID:   s.flowID,
			Seq:      seq,
			TxUnixNS: s.cfg.Now().UnixNano(),
			Token:    s.cfg.Token,
		}
		EncodeHeader(buf, h)
		copy(buf[HeaderSize:], s.body)

		var werr error
		if s.cfg.WriteTarget != nil {
			_, werr = s.conn.WriteToUDP(buf, s.cfg.WriteTarget)
		} else {
			_, werr = s.conn.Write(buf)
		}
		if werr != nil {
			return fmt.Errorf("stream: write seq %d: %w", seq, werr)
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
