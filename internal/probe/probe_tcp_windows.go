//go:build windows

package probe

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// TCPProber on Windows is a permanent stub. Windows raw ICMP sockets
// drop TimeExceeded (issue #1) and iphlpapi.dll!IcmpSendEcho2Ex only
// speaks ICMP echo. WinDivert can send arbitrary TCP at TTL and capture
// ICMP returns, but it requires a kernel driver and CGO — neither is
// acceptable for dropline (CGO_ENABLED=0 is enforced for static
// binaries; see CLAUDE.md). The stub matches the wire shape so
// renderers don't branch on platform: NewTCP returns a real prober,
// Run emits one Snapshot{Supported:false} so the aggregator latches
// the unsupported state, then blocks on ctx.
type TCPProber struct {
	cfg       TCPConfig
	out       chan<- TCPSnapshot
	closeOnce sync.Once
}

// NewTCP validates config and returns a stub prober. Same validation as
// the Unix prober so misuse surfaces consistently across platforms.
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
	return &TCPProber{cfg: cfg, out: out}, nil
}

// Close is a no-op on Windows; the stub holds no resources.
func (p *TCPProber) Close() error {
	p.closeOnce.Do(func() {})
	return nil
}

// Run emits exactly one Snapshot{Supported:false} and blocks on ctx.
// The single emission is load-bearing — it gives the aggregator a
// definitive "this platform doesn't do TCP-mode probing" signal so the
// renderer can show "(unsupported)" rather than "(no data yet)".
func (p *TCPProber) Run(ctx context.Context) error {
	if p.out != nil {
		snap := TCPSnapshot{
			At:        time.Now(),
			Supported: false,
			Port:      p.cfg.Port,
		}
		select {
		case p.out <- snap:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}
