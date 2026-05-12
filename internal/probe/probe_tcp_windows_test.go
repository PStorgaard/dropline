//go:build windows

package probe

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestTCPProberWindows_StubEmitsUnsupported(t *testing.T) {
	out := make(chan TCPSnapshot, 1)
	p, err := NewTCP(TCPConfig{
		Target:      net.IPv4(127, 0, 0, 1),
		Port:        443,
		MTRInterval: 200 * time.Millisecond,
		MaxHops:     5,
	}, out)
	if err != nil {
		t.Fatalf("NewTCP: %v", err)
	}
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	select {
	case snap := <-out:
		if snap.Supported {
			t.Fatalf("Windows stub must emit Supported=false, got true")
		}
		if len(snap.Hops) != 0 {
			t.Fatalf("Windows stub must emit empty Hops, got %d", len(snap.Hops))
		}
		if snap.Port != 443 {
			t.Fatalf("snapshot Port: got %d want 443", snap.Port)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for unsupported snapshot")
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run after cancel: got %v want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return within 2s of cancel")
	}
}

func TestTCPProberWindows_RejectsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  TCPConfig
	}{
		{"nil target", TCPConfig{Port: 443, MTRInterval: time.Second, MaxHops: 5}},
		{"max hops 0", TCPConfig{Target: net.IPv4(1, 2, 3, 4), Port: 443, MTRInterval: time.Second, MaxHops: 0}},
		{"interval 0", TCPConfig{Target: net.IPv4(1, 2, 3, 4), Port: 443, MTRInterval: 0, MaxHops: 5}},
		{"port 0", TCPConfig{Target: net.IPv4(1, 2, 3, 4), Port: 0, MTRInterval: time.Second, MaxHops: 5}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewTCP(c.cfg, nil); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}
