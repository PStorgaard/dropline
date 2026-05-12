//go:build linux

package tcpinfo

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

// New returns a Linux Sampler that reads TCP_INFO via getsockopt on
// conn's underlying file descriptor. The Sampler keeps a reference to
// conn but does not call any read/write methods on it — Sample() only
// invokes the syscall.RawConn Control path. Closing conn is the
// caller's responsibility.
func New(conn *net.TCPConn) (Sampler, error) {
	if conn == nil {
		return nil, errors.New("tcpinfo: nil conn")
	}
	return &linuxSampler{conn: conn}, nil
}

type linuxSampler struct {
	conn *net.TCPConn
}

// Sample issues a getsockopt(IPPROTO_TCP, TCP_INFO) on conn's fd and
// converts the kernel's tcp_info into the cross-platform Stats shape.
// BytesRetrans is synthesized from tcpi_total_retrans * tcpi_snd_mss
// because the Linux struct field tcpi_bytes_retrans (kernel >= 4.15)
// isn't exposed in older golang.org/x/sys/unix.TCPInfo layouts; the
// segment-count × MSS approximation is good enough for a corroboration
// signal even where bytes_retrans would be exact.
func (s *linuxSampler) Sample() (Stats, error) {
	rc, err := s.conn.SyscallConn()
	if err != nil {
		return Stats{}, err
	}
	var info *unix.TCPInfo
	var sErr error
	cErr := rc.Control(func(fd uintptr) {
		info, sErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	})
	if cErr != nil {
		return Stats{}, cErr
	}
	if sErr != nil {
		return Stats{}, sErr
	}
	if info == nil {
		return Stats{}, errors.New("tcpinfo: getsockopt returned nil info")
	}
	bytesRetrans := uint64(info.Total_retrans) * uint64(info.Snd_mss)
	// tcpi_segs_out × tcpi_snd_mss is a close proxy for total bytes
	// transmitted; tcpi_bytes_sent (kernel >= 4.15) would be exact but
	// is omitted from older x/sys layouts. The corroboration call site
	// uses BytesOut as a denominator for the retransmit fraction, so a
	// few-MSS overestimate is acceptable.
	bytesOut := uint64(info.Segs_out) * uint64(info.Snd_mss)
	return Stats{
		Supported:    true,
		BytesRetrans: bytesRetrans,
		BytesOut:     bytesOut,
		RttUs:        info.Rtt,
		MinRttUs:     info.Min_rtt,
	}, nil
}

func (s *linuxSampler) Close() error { return nil }
