package control

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// Handler is invoked once per accepted connection after the client's Hello
// has been read. The handler owns *Conn for the rest of the session and is
// responsible for sending Ready (or Error) and any subsequent messages.
//
// The handler should honor ctx; ListenAndServe does not forcibly close
// in-flight connections on shutdown.
type Handler func(ctx context.Context, c *Conn, hello *Hello) error

// ProbeHandler is the parallel dispatch for connections whose first
// message is a TCPProbe rather than a Hello. The dropline TCP-corro-
// boration probe re-uses the control-channel port (one bound socket
// for both control and probe traffic); the server's accept loop sniffs
// the first message and routes accordingly. ProbeHandler receives the
// raw net.Conn — not a control.Conn — because the probe protocol after
// the first message is just a unidirectional stream of dummy bytes
// that don't follow the control framing.
//
// ProbeHandler may be nil; if it is, a TCPProbe first-message produces
// a typed Error reply on the *control.Conn and the connection is
// closed.
type ProbeHandler func(ctx context.Context, nc net.Conn, probe *TCPProbe)

// Server accepts control-channel connections and dispatches each to Handler
// after parsing the client's Hello. Post-Hello admission policy (e.g.
// max concurrent *sessions*) lives in the Handler closure — see
// cmd/dropline/serve.go's newServeHandler — because it needs Hello fields
// (flow_id, etc.). Pre-Hello fd/goroutine caps fundamentally can't live
// there since the Handler isn't invoked until Hello is parsed; that cap
// is MaxPendingConns below.
//
// MaxPendingConns bounds the number of accepted-but-not-yet-admitted
// connections (those still in the HelloReadTimeout window or actively
// running a Handler). Zero means unlimited (test default). When the cap
// is reached, additional accepted connections are closed immediately
// without spawning a goroutine or sending a typed Error frame —
// honest backpressure that doesn't pay Conn-framing setup costs against
// a flood. Per-IP rate limiting is the next layer and is out of scope.
type Server struct {
	Addr            string
	Handler         Handler
	ProbeHandler    ProbeHandler
	MaxPendingConns int
}

// ListenAndServe binds Addr and serves until ctx is canceled. Accept errors
// after ctx cancellation are swallowed.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.Handler == nil {
		return errors.New("control: Server.Handler is nil")
	}
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("control: listen: %w", err)
	}
	return s.serve(ctx, ln)
}

// Serve runs the accept loop on an existing listener. Useful when the caller
// needs to pick the bound address (e.g. via net.Listen("tcp", "127.0.0.1:0"))
// before the loop starts.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s.Handler == nil {
		return errors.New("control: Server.Handler is nil")
	}
	return s.serve(ctx, ln)
}

func (s *Server) serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	// Pre-Hello concurrency cap. nil sem == unlimited (back-compat for
	// tests that construct Server{} without setting MaxPendingConns).
	// The slot is held for the full handle lifetime; HelloReadTimeout
	// inside handle bounds the worst-case hold for a slow-loris peer.
	var sem chan struct{}
	if s.MaxPendingConns > 0 {
		sem = make(chan struct{}, s.MaxPendingConns)
	}
	var tempDelay time.Duration
	for {
		nc, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isTemporary(err) {
				// Back off on transient errors (EMFILE under fd pressure,
				// ECONNABORTED races) instead of killing the listener. The
				// schedule mirrors net/http's Server.Serve.
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if tempDelay > time.Second {
					tempDelay = time.Second
				}
				select {
				case <-time.After(tempDelay):
				case <-ctx.Done():
					return nil
				}
				continue
			}
			return fmt.Errorf("control: accept: %w", err)
		}
		tempDelay = 0
		if sem != nil {
			select {
			case sem <- struct{}{}:
			default:
				_ = nc.Close()
				continue
			}
			go func() {
				defer func() { <-sem }()
				s.handle(ctx, nc)
			}()
			continue
		}
		go s.handle(ctx, nc)
	}
}

// isTemporary reports whether err is a transient accept error that
// callers should retry rather than treat as fatal. The interface check
// matches what net.Listener implementations historically returned;
// errors.Is(err, syscall.EMFILE) catches the modern wrapped form.
func isTemporary(err error) bool {
	var te interface{ Temporary() bool }
	if errors.As(err, &te) && te.Temporary() {
		return true
	}
	return false
}

func (s *Server) handle(ctx context.Context, nc net.Conn) {
	c := NewConn(nc)
	// Bound the Hello read so an idle peer cannot pin this goroutine
	// indefinitely. Clear the deadline once the first message is parsed
	// so the Handler can set its own I/O policy.
	_ = nc.SetReadDeadline(time.Now().Add(HelloReadTimeout))
	msg, err := c.Recv()
	if err != nil {
		_ = c.Close()
		return
	}
	_ = nc.SetReadDeadline(time.Time{})
	switch m := msg.(type) {
	case *Hello:
		defer c.Close()
		_ = s.Handler(ctx, c, m)
	case *TCPProbe:
		// The probe handler reads/discards bytes directly off the raw
		// connection until close — no further control framing applies.
		// We intentionally do NOT defer c.Close here so the handler
		// owns the conn lifecycle (the c wrapper holds buffered state
		// we'd otherwise discard mid-read).
		if s.ProbeHandler == nil {
			_ = c.Send(&Error{Type: TypeError, Reason: "tcp_probe not supported by this server"})
			_ = c.Close()
			return
		}
		s.ProbeHandler(ctx, nc, m)
		_ = nc.Close()
	default:
		_ = c.Send(&Error{Type: TypeError, Reason: "expected hello"})
		_ = c.Close()
	}
}
