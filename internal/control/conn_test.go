package control

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestPipeRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	client := NewConn(a)
	server := NewConn(b)
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		msg, err := server.Recv()
		if err != nil {
			done <- err
			return
		}
		hello, ok := msg.(*Hello)
		if !ok || hello.Mode != "loss" {
			done <- &testErr{"unexpected hello"}
			return
		}
		if err := server.Send(&Ready{Type: TypeReady, SessionID: "sess-1"}); err != nil {
			done <- err
			return
		}
		for i := 0; i < 3; i++ {
			if err := server.Send(&Stats{Type: TypeStats, T: float64(i + 1), Recv: int64(1000 * (i + 1))}); err != nil {
				done <- err
				return
			}
		}
		if err := server.Send(&Final{Type: TypeFinal, Stats: FinalStats{Recv: 3000, DurationS: 3.0}}); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	if err := client.Send(&Hello{Type: TypeHello, Version: 1, Mode: "loss"}); err != nil {
		t.Fatalf("client.Send hello: %v", err)
	}
	msg, err := client.Recv()
	if err != nil {
		t.Fatalf("client.Recv ready: %v", err)
	}
	if r, ok := msg.(*Ready); !ok || r.SessionID != "sess-1" {
		t.Fatalf("unexpected ready: %#v", msg)
	}
	for i := 0; i < 3; i++ {
		msg, err := client.Recv()
		if err != nil {
			t.Fatalf("client.Recv stats %d: %v", i, err)
		}
		if s, ok := msg.(*Stats); !ok || s.T != float64(i+1) {
			t.Fatalf("stats %d mismatch: %#v", i, msg)
		}
	}
	msg, err = client.Recv()
	if err != nil {
		t.Fatalf("client.Recv final: %v", err)
	}
	if _, ok := msg.(*Final); !ok {
		t.Fatalf("expected Final, got %#v", msg)
	}

	if err := <-done; err != nil {
		t.Fatalf("server side: %v", err)
	}
}

func TestServerListenAndServe(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	srv := &Server{
		Handler: func(_ context.Context, c *Conn, hello *Hello) error {
			if hello.Mode != "loss" {
				return c.Send(&Error{Type: TypeError, Reason: "bad mode"})
			}
			if err := c.Send(&Ready{Type: TypeReady, SessionID: "s"}); err != nil {
				return err
			}
			return c.Send(&Final{Type: TypeFinal, Stats: FinalStats{Recv: 42}})
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx, ln) }()

	dialCtx, dialCancel := context.WithTimeout(ctx, 2*time.Second)
	defer dialCancel()
	c, err := Dial(dialCtx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if err := c.Send(&Hello{Type: TypeHello, Version: 1, Mode: "loss"}); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv ready: %v", err)
	}
	if _, ok := msg.(*Ready); !ok {
		t.Fatalf("expected Ready, got %#v", msg)
	}
	msg, err = c.Recv()
	if err != nil {
		t.Fatalf("Recv final: %v", err)
	}
	if f, ok := msg.(*Final); !ok || f.Stats.Recv != 42 {
		t.Fatalf("expected Final{Recv:42}, got %#v", msg)
	}
}

func TestServerCtxCancelStops(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &Server{Handler: func(_ context.Context, _ *Conn, _ *Hello) error { return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return within 2s of ctx cancel")
	}
}

type testErr struct{ s string }

func (e *testErr) Error() string { return e.s }

// Server.handle must close the connection within HelloReadTimeout if
// the peer opens TCP but never sends a Hello, so a slow-loris client
// cannot pin one goroutine + fd per dangling connection.
func TestServerHelloReadTimeoutClosesIdleConn(t *testing.T) {
	prev := HelloReadTimeout
	HelloReadTimeout = 150 * time.Millisecond
	t.Cleanup(func() { HelloReadTimeout = prev })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &Server{Handler: func(_ context.Context, _ *Conn, _ *Hello) error { return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Serve(ctx, ln) }()

	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()

	// Reading on an idle TCP peer should return EOF (or another error)
	// once the server closes its end after the deadline. If the server
	// never closes, we hit the test-side deadline and fail.
	_ = nc.SetReadDeadline(time.Now().Add(HelloReadTimeout + 2*time.Second))
	buf := make([]byte, 16)
	n, err := nc.Read(buf)
	if err == nil {
		t.Fatalf("expected server to close idle conn, got %d bytes", n)
	}
}

// Server must cap the number of accepted-but-not-yet-admitted
// connections via MaxPendingConns. Beyond the cap, additional accepted
// conns are closed immediately so a TCP flood cannot pin one goroutine
// + fd per dangling connection for the full HelloReadTimeout window.
func TestServerCapsPreHelloConnections(t *testing.T) {
	prev := HelloReadTimeout
	HelloReadTimeout = 500 * time.Millisecond
	t.Cleanup(func() { HelloReadTimeout = prev })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &Server{
		Handler:         func(_ context.Context, _ *Conn, _ *Hello) error { return nil },
		MaxPendingConns: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Serve(ctx, ln) }()

	dial := func() net.Conn {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		return nc
	}

	// Fill both pre-Hello slots and keep them held by sending nothing.
	c1 := dial()
	defer c1.Close()
	c2 := dial()
	defer c2.Close()

	// Give the accept loop a beat to register both. Without this, the
	// race between Dial returning and the server's `sem <- struct{}{}`
	// could leave the third dial racing with c2's slot acquire.
	time.Sleep(50 * time.Millisecond)

	// Third connect should be accepted-then-closed by the server well
	// before HelloReadTimeout fires. We verify by reading: a server
	// close lands as EOF or reset, and it must arrive within a window
	// much shorter than HelloReadTimeout.
	c3 := dial()
	defer c3.Close()
	rejectDeadline := time.Now().Add(HelloReadTimeout / 2)
	_ = c3.SetReadDeadline(rejectDeadline)
	if _, err := c3.Read(make([]byte, 16)); err == nil {
		t.Fatalf("over-cap conn was not closed; got data instead")
	}
	if time.Now().After(rejectDeadline) {
		t.Fatalf("over-cap conn took too long to close (>=%s); cap not enforced", HelloReadTimeout/2)
	}

	// Free a slot — closing c1 returns EOF on the server's blocking
	// read inside handle, which exits and fires the defer that releases
	// the sem token. EOF is prompt; a short sleep is enough for the
	// server-side goroutine to unwind.
	_ = c1.Close()
	time.Sleep(100 * time.Millisecond)
	c4 := dial()
	defer c4.Close()
	_ = c4.SetReadDeadline(time.Now().Add(HelloReadTimeout / 2))
	if _, err := c4.Read(make([]byte, 16)); err == nil {
		t.Fatalf("post-release conn unexpectedly received data")
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("post-release conn closed by server before deadline (slot not freed): %v", err)
	}
}

// Conn.Send must not hold writeMu indefinitely when the peer stops
// reading. With WriteTimeout set short, a stuck reader should cause
// Send to fail within the timeout window, and a concurrent Send on
// the same Conn must not be blocked appreciably longer than that.
func TestSendWriteTimeoutUnwedgesPeers(t *testing.T) {
	prev := WriteTimeout
	WriteTimeout = 200 * time.Millisecond
	t.Cleanup(func() { WriteTimeout = prev })

	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	// Never read from b — every Write into a blocks at the kernel until
	// the deadline fires. (net.Pipe is synchronous; this is the
	// strictest form of the stuck-peer scenario.)
	writer := NewConn(a)

	// First sender wedges; second sender races behind it. With the fix,
	// the first returns within ~WriteTimeout and the second then takes
	// at most another WriteTimeout. Allow generous slack to avoid CI
	// flakes.
	slack := 2 * time.Second
	msg := &Stats{Type: TypeStats, T: 1, Recv: 1}

	errCh := make(chan error, 2)
	t0 := time.Now()
	go func() { errCh <- writer.Send(msg) }()
	go func() { errCh <- writer.Send(msg) }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err == nil {
				t.Errorf("Send %d: expected timeout error, got nil", i)
			}
		case <-time.After(2*WriteTimeout + slack):
			t.Fatalf("Send %d did not return within %v (writeMu wedge regression?)", i, 2*WriteTimeout+slack)
		}
	}
	if elapsed := time.Since(t0); elapsed > 2*WriteTimeout+slack {
		t.Errorf("two serialized Sends took %v, want <= %v", elapsed, 2*WriteTimeout+slack)
	}
}

// TestConcurrentSendDoesNotInterleave proves Conn.Send is safe to call
// from multiple goroutines: with N concurrent writers each sending its
// own marker payload, the reader side must decode every frame intact.
// A frame torn at the length-prefix would surface as a decode error.
func TestConcurrentSendDoesNotInterleave(t *testing.T) {
	a, b := net.Pipe()
	writer := NewConn(a)
	reader := NewConn(b)
	defer writer.Close()
	defer reader.Close()

	const writers = 8
	const perWriter = 50
	totalFrames := writers * perWriter

	// Producers: half send Stats, half send ReverseHopUpdate, both with
	// a marker int we can read back to confirm payload integrity.
	for w := 0; w < writers; w++ {
		w := w
		go func() {
			for i := 0; i < perWriter; i++ {
				marker := int64(w*1000 + i)
				var msg Message
				if w%2 == 0 {
					msg = &Stats{Type: TypeStats, T: float64(marker), Recv: marker}
				} else {
					msg = &ReverseHopUpdate{Type: TypeReverseHopUpdate, TTL: int(marker), Addr: "1.2.3.4"}
				}
				if err := writer.Send(msg); err != nil {
					t.Errorf("Send: %v", err)
					return
				}
			}
		}()
	}

	deadline := time.Now().Add(5 * time.Second)
	if err := reader.SetDeadline(deadline); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	for i := 0; i < totalFrames; i++ {
		msg, err := reader.Recv()
		if err != nil {
			t.Fatalf("Recv frame %d/%d: %v", i, totalFrames, err)
		}
		switch m := msg.(type) {
		case *Stats:
			if int64(m.T) != m.Recv {
				t.Fatalf("torn Stats payload: T=%v Recv=%d", m.T, m.Recv)
			}
		case *ReverseHopUpdate:
			if m.Addr != "1.2.3.4" {
				t.Fatalf("torn ReverseHopUpdate payload: Addr=%q", m.Addr)
			}
		default:
			t.Fatalf("unexpected message type %T", msg)
		}
	}
}
