//go:build windows

package probe

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestIPv4AddrRoundTrip verifies the IPAddr ULONG packing matches Win32
// expectations: 1.2.3.4 → bytes [1,2,3,4] → ULONG 0x04030201.
func TestIPv4AddrRoundTrip(t *testing.T) {
	cases := []struct {
		ip   string
		want uint32
	}{
		{"1.2.3.4", 0x04030201},
		{"127.0.0.1", 0x0100007f},
		{"8.8.8.8", 0x08080808},
		{"255.255.255.255", 0xffffffff},
	}
	for _, c := range cases {
		ip := net.ParseIP(c.ip)
		got := ipv4ToInAddr(ip)
		if got != c.want {
			t.Errorf("ipv4ToInAddr(%s) = %#x, want %#x", c.ip, got, c.want)
		}
		back := inAddrToIPv4(got).String()
		if back != c.ip {
			t.Errorf("round trip %s: got %s", c.ip, back)
		}
	}
}

// TestNewLoopback exercises the DLL/proc bind path end-to-end against
// 127.0.0.1. Skipped when iphlpapi is unavailable (Wine, locked-down
// SKUs) — same fallback discipline as kerneldrops_windows.go.
func TestNewLoopback(t *testing.T) {
	if err := iphlpapiDLL.Load(); err != nil {
		t.Skipf("iphlpapi.dll unavailable: %v", err)
	}
	p, err := New(Config{
		Target:      net.ParseIP("127.0.0.1"),
		MTRInterval: 100 * time.Millisecond,
		MaxHops:     2,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// At least one probe should have settled against loopback.
	p.mu.Lock()
	sent := p.hops[0].sent
	p.mu.Unlock()
	if sent == 0 {
		t.Errorf("ttl=1 never probed against 127.0.0.1")
	}
}

// TestNewRejectsBadConfig guards the shared config validation path on
// the Windows branch.
func TestNewRejectsBadConfig(t *testing.T) {
	if _, err := New(Config{}, nil); err == nil {
		t.Errorf("New(zero cfg) = nil err, want non-nil")
	}
	if _, err := New(Config{Target: net.ParseIP("::1"), MTRInterval: time.Second, MaxHops: 30}, nil); err == nil {
		t.Errorf("New(ipv6 target) = nil err, want non-nil")
	}
}
