package stream

import (
	"testing"
	"time"
)

// helper: feed a sequence of seqs at uniform 1ms inter-arrival.
func feed(a *Accumulator, t0 time.Time, seqs ...uint64) {
	for i, s := range seqs {
		arrival := t0.Add(time.Duration(i+1) * time.Millisecond)
		h := Header{
			Magic:    Magic,
			FlowID:   1,
			Seq:      s,
			TxUnixNS: t0.Add(time.Duration(i) * time.Millisecond).UnixNano(),
		}
		a.Observe(h, arrival)
	}
}

func TestAccumulatorPerfectSequence(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	a := NewAccumulator(t0)
	feed(a, t0, 0, 1, 2, 3, 4)
	s := a.Snapshot(t0.Add(10 * time.Millisecond))
	if s.Recv != 5 {
		t.Fatalf("recv: want 5, got %d", s.Recv)
	}
	if s.Lost != 0 || s.OutOfOrder != 0 || s.Duplicates != 0 {
		t.Fatalf("expected zero loss/ooo/dup, got %+v", s)
	}
	if s.Bursts.Max != 0 {
		t.Fatalf("max burst: want 0, got %d", s.Bursts.Max)
	}
}

func TestAccumulatorBurstBucketing(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	a := NewAccumulator(t0)
	// Drop seq 1 → burst of 1
	// Drop seq 3,4 → burst of 2
	// Drop seq 6..14 → burst of 9 (3-9 bucket)
	// Drop seq 16..115 → burst of 100 (100+ bucket)
	seqs := []uint64{0, 2, 5, 15, 116}
	feed(a, t0, seqs...)
	s := a.Snapshot(t0.Add(time.Second))
	if got := s.Bursts.One; got != 1 {
		t.Errorf("bucket 1: want 1, got %d", got)
	}
	if got := s.Bursts.Two; got != 1 {
		t.Errorf("bucket 2: want 1, got %d", got)
	}
	if got := s.Bursts.Three9; got != 1 {
		t.Errorf("bucket 3-9: want 1, got %d", got)
	}
	if got := s.Bursts.HundredUp; got != 1 {
		t.Errorf("bucket 100+: want 1, got %d", got)
	}
	if got := s.Bursts.Max; got != 100 {
		t.Errorf("max: want 100, got %d", got)
	}
	if got := s.Lost; got != 1+2+9+100 {
		t.Errorf("lost: want 112, got %d", got)
	}
}

func TestAccumulatorOutOfOrder(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	a := NewAccumulator(t0)
	feed(a, t0, 0, 2, 1, 3) // 1 arrives after 2
	s := a.Snapshot(t0.Add(time.Second))
	if s.Recv != 4 {
		t.Errorf("recv: want 4, got %d", s.Recv)
	}
	if s.OutOfOrder != 1 {
		t.Errorf("out_of_order: want 1, got %d", s.OutOfOrder)
	}
	if s.Duplicates != 0 {
		t.Errorf("duplicates: want 0, got %d", s.Duplicates)
	}
	// Lost was incremented to 1 when seq=2 advanced expectedSeq past
	// the gap at seq=1, then rolled back when seq=1 arrived late.
	if s.Lost != 0 {
		t.Errorf("lost (after OOO recovery): want 0, got %d", s.Lost)
	}
}

// A late packet filling a previously-counted gap rolls the lost
// counter back, separating true network loss from reordering.
func TestAccumulatorLostRollbackOnReorder(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	a := NewAccumulator(t0)
	// Sequence 0, 3, 4, 1, 2: seq 3 advances expectedSeq past 1 and 2,
	// counting both as lost; seqs 1 and 2 arriving later should each
	// decrement lost back to zero (true loss = 0, OOO = 2).
	feed(a, t0, 0, 3, 4, 1, 2)
	s := a.Snapshot(t0.Add(time.Second))
	if s.Lost != 0 {
		t.Errorf("lost: want 0 (both gap fills recovered), got %d", s.Lost)
	}
	if s.OutOfOrder != 2 {
		t.Errorf("out_of_order: want 2, got %d", s.OutOfOrder)
	}
	// Burst histogram intentionally does NOT roll back — the gap of 2
	// at the moment seq=3 arrived is permanent in the streaming model.
	if s.Bursts.Two != 1 {
		t.Errorf("bursts.Two: want 1 (gap of 2 stays recorded), got %d", s.Bursts.Two)
	}
}

func TestAccumulatorDuplicates(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	a := NewAccumulator(t0)
	feed(a, t0, 0, 1, 1, 2, 2, 2)
	s := a.Snapshot(t0.Add(time.Second))
	if s.Recv != 3 {
		t.Errorf("recv: want 3 (unique), got %d", s.Recv)
	}
	if s.Duplicates != 3 {
		t.Errorf("duplicates: want 3, got %d", s.Duplicates)
	}
}

// Jitter EWMA should be positive and finite when inter-arrival deltas
// vary between packets. With a constant-transit stream J stays at 0.
func TestAccumulatorJitterMonotonic(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	a := NewAccumulator(t0)
	// Two packets sent 10ms apart but arriving 12ms then 5ms apart →
	// transit varies → non-zero jitter.
	cases := []struct {
		seq            uint64
		txOffsetMicro  int64
		recvOffsetMico int64
	}{
		{0, 0, 0},
		{1, 10_000, 12_000},
		{2, 20_000, 17_000},
	}
	for _, c := range cases {
		h := Header{Magic: Magic, FlowID: 1, Seq: c.seq, TxUnixNS: t0.Add(time.Duration(c.txOffsetMicro) * time.Microsecond).UnixNano()}
		arrival := t0.Add(time.Duration(c.recvOffsetMico) * time.Microsecond)
		a.Observe(h, arrival)
	}
	s := a.Snapshot(t0.Add(time.Second))
	if s.JitterMS <= 0 {
		t.Fatalf("jitter: want > 0, got %f", s.JitterMS)
	}
}

func TestAccumulatorInitialGap(t *testing.T) {
	// First packet observed has seq 5 → seqs 0..4 are recorded as a burst
	// of 5 immediately, classified into the 3-9 bucket.
	t0 := time.Unix(1_000_000_000, 0)
	a := NewAccumulator(t0)
	feed(a, t0, 5, 6)
	s := a.Snapshot(t0.Add(time.Second))
	if s.Lost != 5 {
		t.Errorf("lost: want 5, got %d", s.Lost)
	}
	if s.Bursts.Three9 != 1 {
		t.Errorf("bucket 3-9: want 1, got %d", s.Bursts.Three9)
	}
	if s.Bursts.Max != 5 {
		t.Errorf("max: want 5, got %d", s.Bursts.Max)
	}
}

func TestAccumulatorLocalDropsAndKernel(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	a := NewAccumulator(t0)
	a.AddLocalDrop(7)
	a.AddLocalDrop(3)
	a.SetKernelDrops(42)
	s := a.Snapshot(t0)
	if s.LocalDrops != 10 {
		t.Errorf("local drops: want 10, got %d", s.LocalDrops)
	}
	if s.KernelDrops != 42 {
		t.Errorf("kernel drops: want 42, got %d", s.KernelDrops)
	}
}
