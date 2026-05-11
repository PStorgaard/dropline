package stream

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// fakeNow returns a closure-backed Now that advances on each call. The
// gate uses Now to stamp pause/resume; tests need to control it.
func fakeNow(start *atomic.Int64) func() time.Time {
	return func() time.Time {
		return time.Unix(0, start.Load())
	}
}

func TestPauseGateInitiallyRunning(t *testing.T) {
	g := NewPauseGate(time.Now)
	if g.IsPaused() {
		t.Errorf("new gate should be running, not paused")
	}
	if got := g.PausedFor(); got != 0 {
		t.Errorf("new gate PausedFor = %s, want 0", got)
	}
}

func TestPauseGatePauseResumeAccumulates(t *testing.T) {
	var clock atomic.Int64
	clock.Store(int64(time.Second))
	g := NewPauseGate(fakeNow(&clock))

	g.Pause()
	if !g.IsPaused() {
		t.Errorf("Pause() should mark gate paused")
	}
	clock.Store(int64(3 * time.Second)) // +2s while paused
	g.Resume()
	if g.IsPaused() {
		t.Errorf("Resume() should unpause")
	}
	if got := g.PausedFor(); got != 2*time.Second {
		t.Errorf("PausedFor after one cycle = %s, want 2s", got)
	}

	// Second cycle accumulates.
	clock.Store(int64(5 * time.Second))
	g.Pause()
	clock.Store(int64(8 * time.Second)) // +3s
	g.Resume()
	if got := g.PausedFor(); got != 5*time.Second {
		t.Errorf("PausedFor after two cycles = %s, want 5s", got)
	}
}

func TestPauseGatePausedForGrowsDuringActivePause(t *testing.T) {
	var clock atomic.Int64
	clock.Store(int64(time.Second))
	g := NewPauseGate(fakeNow(&clock))

	g.Pause()
	clock.Store(int64(time.Second) + int64(750*time.Millisecond))
	if got := g.PausedFor(); got != 750*time.Millisecond {
		t.Errorf("active-pause PausedFor = %s, want 750ms", got)
	}
}

func TestPauseGateIdempotent(t *testing.T) {
	g := NewPauseGate(time.Now)
	g.Pause()
	g.Pause() // second call is no-op
	if !g.IsPaused() {
		t.Errorf("double Pause should still be paused")
	}
	g.Resume()
	g.Resume() // no-op
	if g.IsPaused() {
		t.Errorf("double Resume should still be running")
	}
}

func TestPauseGateWaitWhilePausedRunningReturnsImmediately(t *testing.T) {
	g := NewPauseGate(time.Now)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := g.WaitWhilePaused(ctx); err != nil {
		t.Fatalf("WaitWhilePaused (running): %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Millisecond {
		t.Errorf("WaitWhilePaused on running gate took %s; should be near-zero", elapsed)
	}
}

func TestPauseGateWaitWhilePausedUnblocksOnResume(t *testing.T) {
	g := NewPauseGate(time.Now)
	g.Pause()

	released := make(chan struct{})
	go func() {
		_ = g.WaitWhilePaused(context.Background())
		close(released)
	}()

	// Verify it's actually waiting before we resume.
	select {
	case <-released:
		t.Fatalf("WaitWhilePaused returned before Resume")
	case <-time.After(20 * time.Millisecond):
	}

	g.Resume()

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatalf("WaitWhilePaused did not return within 1s of Resume")
	}
}

func TestPauseGateWaitWhilePausedHonorsCtxCancel(t *testing.T) {
	g := NewPauseGate(time.Now)
	g.Pause()

	ctx, cancel := context.WithCancel(context.Background())
	released := make(chan error, 1)
	go func() {
		released <- g.WaitWhilePaused(ctx)
	}()

	cancel()
	select {
	case err := <-released:
		if err == nil {
			t.Errorf("WaitWhilePaused after cancel should return ctx.Err, got nil")
		}
	case <-time.After(time.Second):
		t.Fatalf("WaitWhilePaused did not return within 1s of ctx cancel")
	}
}
