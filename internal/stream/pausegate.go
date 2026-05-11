package stream

import (
	"context"
	"sync"
	"time"
)

// PauseGate coordinates pause / resume of the sender between the trace
// driver's TUI thread and the sender's run loop. The gate is purely a
// synchronization primitive: it tracks accumulated wall-clock pause
// time so the sender can apply it as an offset to its drift-free
// pacing schedule (target = t0 + n*interval + gate.PausedFor()),
// preserving sequence continuity across pause cycles without bursting
// on resume.
//
// The zero value is unusable; construct via NewPauseGate.
type PauseGate struct {
	mu     sync.Mutex
	cond   *sync.Cond
	now    func() time.Time
	paused bool
	// pausedAt is the wall-clock time the current pause started. Zero
	// when not paused.
	pausedAt time.Time
	// pausedFor accumulates total wall-clock time spent paused across
	// all completed pause cycles. Read by the sender's Run loop on
	// every iteration.
	pausedFor time.Duration
}

// NewPauseGate constructs a fresh gate. now is an injectable clock for
// tests; pass nil for time.Now.
func NewPauseGate(now func() time.Time) *PauseGate {
	if now == nil {
		now = time.Now
	}
	g := &PauseGate{now: now}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// Pause flips the gate to the paused state. Idempotent — calling
// Pause while already paused is a no-op.
func (g *PauseGate) Pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused {
		return
	}
	g.paused = true
	g.pausedAt = g.now()
}

// Resume flips the gate to the running state and folds the elapsed
// pause window into pausedFor. Idempotent — calling Resume while
// already running is a no-op. Wakes any goroutine blocked in
// WaitWhilePaused.
func (g *PauseGate) Resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.paused {
		return
	}
	g.pausedFor += g.now().Sub(g.pausedAt)
	g.paused = false
	g.pausedAt = time.Time{}
	g.cond.Broadcast()
}

// IsPaused reports whether the gate is currently paused.
func (g *PauseGate) IsPaused() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.paused
}

// PausedFor returns the total time the gate has spent paused. While a
// pause is in progress this includes the time elapsed since the pause
// started, so the sender's offset reflects "real-time-paused-so-far"
// during a long pause as well as completed cycles.
func (g *PauseGate) PausedFor() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	d := g.pausedFor
	if g.paused {
		d += g.now().Sub(g.pausedAt)
	}
	return d
}

// WaitWhilePaused blocks until the gate is unpaused or ctx is canceled.
// Returns immediately when the gate is already running. Returns ctx.Err
// if the context cancels mid-wait.
func (g *PauseGate) WaitWhilePaused(ctx context.Context) error {
	_, err := g.waitAndOffsetLocked(ctx)
	return err
}

// WaitAndOffset is the sender's hot-path entry point: it blocks while
// paused (returns ctx.Err on cancel) and returns the accumulated pause
// offset under a single lock acquisition in the steady (running)
// state. Folds the equivalent of WaitWhilePaused + PausedFor so the
// per-packet sender loop locks once instead of twice.
func (g *PauseGate) WaitAndOffset(ctx context.Context) (time.Duration, error) {
	return g.waitAndOffsetLocked(ctx)
}

func (g *PauseGate) waitAndOffsetLocked(ctx context.Context) (time.Duration, error) {
	g.mu.Lock()
	if !g.paused {
		d := g.pausedFor
		g.mu.Unlock()
		return d, nil
	}
	// Spawn a watcher to broadcast on ctx cancel so a paused waiter
	// can unblock without polling. Self-cleaning via the done channel
	// when the wait ends naturally.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		case <-done:
		}
	}()
	for g.paused && ctx.Err() == nil {
		g.cond.Wait()
	}
	close(done)
	d := g.pausedFor
	if g.paused {
		d += g.now().Sub(g.pausedAt)
	}
	g.mu.Unlock()
	return d, ctx.Err()
}
