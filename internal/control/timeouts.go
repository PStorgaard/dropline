package control

import "time"

// HelloReadTimeout bounds the time the server will wait for a peer's
// Hello frame after Accept. Without this, a TCP peer that opens the
// connection and never writes pins one goroutine + one fd until the
// kernel times the connection out, which is the slow-loris regime.
//
// Var (not const) so tests can dial it down; production callers should
// treat it as immutable.
var HelloReadTimeout = 10 * time.Second

// WriteTimeout bounds the time a single Send may block writing into
// the kernel send buffer. With concurrent senders on one Conn (the
// stats and reverse-hop forwarders both publish on the server side),
// an unbounded Write would hold writeMu forever and wedge every other
// writer if the peer's receive window stalled.
//
// Var (not const) so tests can dial it down; production callers should
// treat it as immutable.
var WriteTimeout = 10 * time.Second
