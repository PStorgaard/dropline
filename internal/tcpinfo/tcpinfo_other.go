//go:build !linux && !windows

package tcpinfo

import (
	"errors"
	"net"
)

// New returns a no-op Sampler on platforms without a TCP_INFO retrieval
// path implemented in this package (macOS, *BSD, anything not Linux or
// Windows). macOS exposes the related TCP_CONNECTION_INFO socket option
// but with a different struct layout; supporting it would mean another
// platform-specific file and is deferred until there's demand. Until
// then dropline's TCP-corroboration probe still RUNS on these
// platforms — the kernel just returns zero stats and the renderer
// shows zeros instead of retransmit counts.
func New(conn *net.TCPConn) (Sampler, error) {
	if conn == nil {
		return nil, errors.New("tcpinfo: nil conn")
	}
	return nopSampler{}, nil
}
