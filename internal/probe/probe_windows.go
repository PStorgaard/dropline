//go:build windows

package probe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows raw ICMP sockets reliably deliver EchoReply but drop
// TimeExceeded — a documented user-space limitation. This prober uses
// iphlpapi.dll!IcmpSendEcho2Ex, which is the same path tracert.exe
// takes; it surfaces TimeExceeded correctly and does not require
// Administrator. See issue #1.
//
// DLL/proc binding mirrors internal/stream/kerneldrops_windows.go.

var (
	iphlpapiDLL         = windows.NewLazySystemDLL("iphlpapi.dll")
	procIcmpCreateFile  = iphlpapiDLL.NewProc("IcmpCreateFile")
	procIcmpCloseHandle = iphlpapiDLL.NewProc("IcmpCloseHandle")
	procIcmpSendEcho2Ex = iphlpapiDLL.NewProc("IcmpSendEcho2Ex")
)

// IcmpSendEcho2Ex reply Status codes (from ipexport.h / iphlpapi).
const (
	ipSuccess           = 0
	ipReqTimedOut       = 11010
	ipTTLExpiredTransit = 11013
)

// ipOptionInformation matches Win32 IP_OPTION_INFORMATION on amd64
// (8-byte alignment for the pointer). TTL is the only field we set.
type ipOptionInformation struct {
	TTL         uint8
	Tos         uint8
	Flags       uint8
	OptionsSize uint8
	OptionsData uintptr
}

// icmpEchoReply matches Win32 ICMP_ECHO_REPLY on amd64. The struct's
// 40-byte total includes 4 bytes of padding between Reserved and Data
// for 8-byte pointer alignment; Go's default field layout produces this
// naturally.
type icmpEchoReply struct {
	Address       uint32
	Status        uint32
	RoundTripTime uint32
	DataSize      uint16
	Reserved      uint16
	Data          uintptr
	Options       ipOptionInformation
}

// Prober is the Windows forward TTL-walker. Each sweep spawns one
// goroutine per TTL, each issuing a synchronous IcmpSendEcho2Ex; the
// underlying Win32 API is thread-safe on a shared handle.
type Prober struct {
	cfg    Config
	handle windows.Handle
	out    chan<- Snapshot

	mu       sync.Mutex
	hops     []*hopState
	terminus int // smallest TTL ever returning IP_SUCCESS, 0 if none

	closeOnce sync.Once
}

// New resolves the iphlpapi procs and opens an ICMP handle. Returns a
// clear error if iphlpapi.dll or any required proc cannot be loaded —
// callers (privcheck) treat that as "no ICMP probing capability".
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
	if err := iphlpapiDLL.Load(); err != nil {
		return nil, fmt.Errorf("probe: load iphlpapi.dll: %w", err)
	}
	for _, p := range []*windows.LazyProc{procIcmpCreateFile, procIcmpCloseHandle, procIcmpSendEcho2Ex} {
		if err := p.Find(); err != nil {
			return nil, fmt.Errorf("probe: resolve %s: %w", p.Name, err)
		}
	}

	r1, _, e := procIcmpCreateFile.Call()
	handle := windows.Handle(r1)
	if handle == windows.InvalidHandle {
		return nil, fmt.Errorf("probe: IcmpCreateFile: %w", e)
	}

	hops := make([]*hopState, cfg.MaxHops)
	for i := range hops {
		hops[i] = &hopState{}
	}

	return &Prober{
		cfg:    cfg,
		handle: handle,
		out:    out,
		hops:   hops,
	}, nil
}

// Close releases the ICMP handle. Safe to call multiple times.
// IcmpCloseHandle cancels any in-flight IcmpSendEcho2Ex calls on the
// same handle, which is how Run unblocks sweep workers on ctx
// cancellation.
func (p *Prober) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.mu.Lock()
		h := p.handle
		p.handle = windows.InvalidHandle
		p.mu.Unlock()
		if h == 0 || h == windows.InvalidHandle {
			return
		}
		r1, _, e := procIcmpCloseHandle.Call(uintptr(h))
		if r1 == 0 {
			err = e
		}
	})
	return err
}

// Run drives the prober until ctx is cancelled. Returns nil on a clean
// cancellation. The ICMP handle is closed on exit regardless of how
// Run terminated.
func (p *Prober) Run(ctx context.Context) error {
	defer func() { _ = p.Close() }()

	// Closing the handle on ctx cancellation unblocks any in-flight
	// IcmpSendEcho2Ex syscalls so the current sweep drains promptly.
	cancelWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = p.Close()
		case <-cancelWatch:
		}
	}()
	defer close(cancelWatch)

	ticker := time.NewTicker(p.cfg.MTRInterval)
	defer ticker.Stop()

	at := p.sweep()
	p.emit(at)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			at := p.sweep()
			p.emit(at)
		}
	}
}

// sweep fires one ICMP probe per TTL (1..maxTTL) in parallel and
// returns when all probes have replied or timed out. Returns the
// sweep-start time so emit() can stamp the resulting Snapshot.
func (p *Prober) sweep() time.Time {
	at := time.Now()
	p.mu.Lock()
	maxTTL := len(p.hops)
	if p.terminus > 0 && p.terminus < maxTTL {
		maxTTL = p.terminus
	}
	p.mu.Unlock()

	var wg sync.WaitGroup
	for ttl := 1; ttl <= maxTTL; ttl++ {
		wg.Add(1)
		go func(ttl int) {
			defer wg.Done()
			p.probe(ttl)
		}(ttl)
	}
	wg.Wait()
	return at
}

// probe issues one IcmpSendEcho2Ex synchronously at the given TTL and
// folds the result into hop state. Treats IP_SUCCESS as terminus reply,
// IP_TTL_EXPIRED_TRANSIT as an intermediate-hop reply; anything else
// (timeout, unreachable, etc.) surfaces as loss via the sent/recv gap.
func (p *Prober) probe(ttl int) {
	p.mu.Lock()
	p.hops[ttl-1].sent++
	handle := p.handle
	p.mu.Unlock()
	if handle == 0 || handle == windows.InvalidHandle {
		return
	}

	timeoutMS := uint32(lossTimeout(p.cfg.MTRInterval) / time.Millisecond)
	opts := ipOptionInformation{TTL: uint8(ttl)} // #nosec G115 -- TTL ≤ 255
	// 32-byte payload mirrors what tracert/ping send by default.
	payload := [32]byte{}
	// Reply buffer must hold at least ICMP_ECHO_REPLY (40 B on amd64)
	// + RequestSize + 8 (Win32-recommended IO_STATUS_BLOCK margin). 256
	// is comfortable headroom for a single reply.
	var reply [256]byte
	dest := ipv4ToInAddr(p.cfg.Target)

	r1, _, _ := procIcmpSendEcho2Ex.Call(
		uintptr(handle),
		0, 0, 0, // event, apc routine, apc context
		0, // source address (0 = let kernel choose)
		uintptr(dest),
		uintptr(unsafe.Pointer(&payload[0])),
		uintptr(uint32(len(payload))),
		uintptr(unsafe.Pointer(&opts)),
		uintptr(unsafe.Pointer(&reply[0])),
		uintptr(uint32(len(reply))),
		uintptr(timeoutMS),
	)
	if r1 == 0 {
		// 0 replies = timeout, handle closed, or other failure. Loss
		// surfaces naturally via sent without a matching recv.
		return
	}
	er := (*icmpEchoReply)(unsafe.Pointer(&reply[0]))
	var isEcho bool
	switch er.Status {
	case ipSuccess:
		isEcho = true
	case ipTTLExpiredTransit:
		isEcho = false
	default:
		return
	}
	src := inAddrToIPv4(er.Address)
	rttMS := float64(er.RoundTripTime)

	p.mu.Lock()
	p.hops[ttl-1].recordReply(src, rttMS, isEcho)
	if isEcho && (p.terminus == 0 || ttl < p.terminus) {
		p.terminus = ttl
	}
	p.mu.Unlock()
}

// emit pushes a Snapshot to p.out, non-blocking. Slow consumers cause
// snapshot drops, matching the rule in agg/stream.
func (p *Prober) emit(at time.Time) {
	if p.out == nil {
		return
	}
	p.mu.Lock()
	snap := buildSnapshot(p.hops, p.terminus, at)
	p.mu.Unlock()
	select {
	case p.out <- snap:
	default:
	}
}

// ipv4ToInAddr packs an IPv4 net.IP into a Win32 IPAddr (ULONG in
// network byte order, which reads as little-endian on x86/amd64 from
// the four bytes a.b.c.d).
func ipv4ToInAddr(ip net.IP) uint32 {
	ip4 := ip.To4()
	if ip4 == nil {
		return 0
	}
	return uint32(ip4[0]) | uint32(ip4[1])<<8 | uint32(ip4[2])<<16 | uint32(ip4[3])<<24
}

// inAddrToIPv4 unpacks a Win32 IPAddr back into net.IP.
func inAddrToIPv4(addr uint32) net.IP {
	return net.IPv4(byte(addr), byte(addr>>8), byte(addr>>16), byte(addr>>24))
}
