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

// Server accepts control-channel connections and dispatches each to Handler
// after parsing the client's Hello. Concurrency caps (e.g. max concurrent
// sessions) live in the Handler closure — see cmd/dropline/serve.go's
// newServeHandler — so this package stays free of admission policy.
type Server struct {
	Addr    string
	Handler Handler
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
	defer c.Close()
	// Bound the Hello read so an idle peer cannot pin this goroutine
	// indefinitely. Clear the deadline once Hello is parsed so the
	// Handler can set its own I/O policy.
	_ = nc.SetReadDeadline(time.Now().Add(HelloReadTimeout))
	msg, err := c.Recv()
	if err != nil {
		return
	}
	_ = nc.SetReadDeadline(time.Time{})
	hello, ok := msg.(*Hello)
	if !ok {
		_ = c.Send(&Error{Type: TypeError, Reason: "expected hello"})
		return
	}
	_ = s.Handler(ctx, c, hello)
}
