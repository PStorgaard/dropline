package control

import (
	"context"
	"net"
	"sync"
	"time"
)

// Conn is a synchronous client-side wrapper around a control TCP connection.
// Send and Recv block; the caller is responsible for sequencing the protocol
// (Hello → Ready/Error → loop Stats/Final/ReverseHopUpdate → Close).
//
// Send is safe for concurrent use — the writeMu serializes frames so the
// reverse-hop forwarder and the stream-stats forwarder on the server side
// can both publish without tearing each other's bytes. Recv stays
// lock-free; the protocol has at most one reader per side.
type Conn struct {
	nc        net.Conn
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// Dial establishes a TCP control connection. The returned *Conn owns the
// underlying net.Conn until Close is called.
//
// v1 is IPv4-only by spec, and the UDP stream + ICMP probe paths both
// pin to IPv4. Forcing "tcp4" here keeps the control channel from
// silently splitting onto IPv6 (or a different A record under
// round-robin DNS) when the caller passes a hostname rather than a
// pre-resolved IPv4.
func Dial(ctx context.Context, addr string) (*Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp4", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{nc: nc}, nil
}

// NewConn wraps an already-established net.Conn (useful for net.Pipe in tests
// and for the server side after Accept).
func NewConn(nc net.Conn) *Conn {
	return &Conn{nc: nc}
}

// Send marshals and writes a single message frame. Safe to call from
// multiple goroutines concurrently.
//
// A WriteTimeout is applied around the underlying Write so that a peer
// whose TCP receive window stalls cannot hold writeMu indefinitely and
// wedge every other concurrent sender on this Conn. On timeout the
// caller observes a deadline-exceeded error; session drivers should
// treat this as a terminal session error.
func (c *Conn) Send(m Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.nc.SetWriteDeadline(time.Now().Add(WriteTimeout))
	defer func() { _ = c.nc.SetWriteDeadline(time.Time{}) }()
	return WriteMessage(c.nc, m)
}

// Recv reads and decodes a single message frame, blocking until one arrives.
func (c *Conn) Recv() (Message, error) { return ReadMessage(c.nc) }

// SetDeadline applies an I/O deadline to the underlying connection.
func (c *Conn) SetDeadline(t time.Time) error { return c.nc.SetDeadline(t) }

// RemoteAddr returns the remote network address of the underlying connection.
// Used by the server to learn the client's IP for the reverse trace target.
func (c *Conn) RemoteAddr() net.Addr { return c.nc.RemoteAddr() }

// Close closes the underlying connection. Safe to call multiple times.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.nc.Close() })
	return c.closeErr
}
