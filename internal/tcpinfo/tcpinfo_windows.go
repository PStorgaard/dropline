//go:build windows

package tcpinfo

import (
	"errors"
	"net"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/windows"
)

// sioTCPInfo is the IOCTL code for SIO_TCP_INFO: _WSAIORW(IOC_VENDOR,
// 39). Available since Windows Vista with TCP_INFO_v0; v1 requires
// Windows 10 build 20348. We always request v0 (set version DWORD =
// 0 in the input buffer) for maximum compatibility — v0 carries every
// field we care about (BytesRetrans, BytesOut, RttUs, MinRttUs).
const sioTCPInfo uint32 = 0xD8000027

// tcpInfoV0 mirrors the Win32 TCP_INFO_v0 struct from mstcpip.h. The
// field order and explicit padding bytes are critical — wrong
// alignment yields garbage values. The 88-byte total matches the
// Microsoft documentation.
type tcpInfoV0 struct {
	State             uint32  // 0
	Mss               uint32  // 4
	ConnectionTimeMs  uint64  // 8
	TimestampsEnabled uint8   // 16  (BOOLEAN)
	_                 [3]byte // 17..19 padding to 4-byte boundary
	RttUs             uint32  // 20
	MinRttUs          uint32  // 24
	BytesInFlight     uint32  // 28
	Cwnd              uint32  // 32
	SndWnd            uint32  // 36
	RcvWnd            uint32  // 40
	RcvBuf            uint32  // 44
	BytesOut          uint64  // 48
	BytesIn           uint64  // 56
	BytesReordered    uint32  // 64
	BytesRetrans      uint32  // 68
	FastRetrans       uint32  // 72
	DupAcksIn         uint32  // 76
	TimeoutEpisodes   uint32  // 80
	SynRetrans        uint8   // 84
	_                 [3]byte // 85..87 trailing pad to 88
}

// New returns a Windows Sampler that reads TCP_INFO_v0 via WSAIoctl on
// conn's underlying SOCKET handle. On the first Sample() failure (the
// most common cause being a pre-Win10-1703 host that doesn't implement
// SIO_TCP_INFO), the Sampler latches into a disabled state and from
// then on returns Stats{Supported: false} with nil error — renderers
// branch on Supported to display "unsupported" rather than "0
// retransmits". Same pattern as kerneldrops_windows.go's
// GetUdpStatisticsEx fallback.
func New(conn *net.TCPConn) (Sampler, error) {
	if conn == nil {
		return nil, errors.New("tcpinfo: nil conn")
	}
	return &windowsSampler{conn: conn}, nil
}

type windowsSampler struct {
	conn *net.TCPConn
	// disabled flips once a Sample() invocation fails — typically
	// because the host's Winsock doesn't expose SIO_TCP_INFO. After
	// that every Sample() returns Stats{Supported: false} with nil
	// error so the caller can mark the corroboration view as
	// unsupported instead of misreporting "0 retransmits".
	disabled atomic.Bool
}

// Sample issues WSAIoctl(SIO_TCP_INFO, v0) on conn's SOCKET and
// translates the resulting TCP_INFO_v0 into the cross-platform Stats
// shape.
func (s *windowsSampler) Sample() (Stats, error) {
	if s.disabled.Load() {
		return Stats{Supported: false}, nil
	}
	rc, err := s.conn.SyscallConn()
	if err != nil {
		return Stats{}, err
	}
	var info tcpInfoV0
	var version uint32 // 0 = TCP_INFO_v0
	var bytesReturned uint32
	var sErr error
	cErr := rc.Control(func(fd uintptr) {
		sErr = windows.WSAIoctl(
			windows.Handle(fd),
			sioTCPInfo,
			(*byte)(unsafe.Pointer(&version)),
			uint32(unsafe.Sizeof(version)),
			(*byte)(unsafe.Pointer(&info)),
			uint32(unsafe.Sizeof(info)),
			&bytesReturned,
			nil, // OVERLAPPED (synchronous)
			0,   // completion routine
		)
	})
	if cErr != nil {
		return Stats{}, cErr
	}
	if sErr != nil {
		// Most common cause on older hosts: WSAEINVAL because
		// SIO_TCP_INFO isn't implemented. Latch the disabled flag and
		// signal "unsupported" via Stats.Supported=false so renderers
		// can distinguish this from "0 retransmits observed".
		s.disabled.Store(true)
		return Stats{Supported: false}, nil
	}
	return Stats{
		Supported:    true,
		BytesRetrans: uint64(info.BytesRetrans),
		BytesOut:     info.BytesOut,
		RttUs:        info.RttUs,
		MinRttUs:     info.MinRttUs,
	}, nil
}

func (s *windowsSampler) Close() error { return nil }
