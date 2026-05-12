package stream

import (
	"errors"
	"testing"
)

// scriptedSampler returns the next int64 from values on each call, then
// errs on err once the script is exhausted. If errs is non-nil at a given
// index, that error is returned instead of values[index].
type scriptedSampler struct {
	values []int64
	errs   []error
	idx    int
}

func (s *scriptedSampler) Sample() (int64, error) {
	if s.idx >= len(s.values) {
		return 0, errors.New("scriptedSampler: out of script")
	}
	i := s.idx
	s.idx++
	if i < len(s.errs) && s.errs[i] != nil {
		return 0, s.errs[i]
	}
	return s.values[i], nil
}

func TestBaselineSamplerFirstCallReturnsZero(t *testing.T) {
	b := newBaselineSampler(&scriptedSampler{values: []int64{1000}})
	got, err := b.Sample()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != 0 {
		t.Errorf("first call: want 0, got %d", got)
	}
	if !b.ready || b.baseline != 1000 {
		t.Errorf("baseline not latched: ready=%v baseline=%d", b.ready, b.baseline)
	}
}

func TestBaselineSamplerReturnsDeltas(t *testing.T) {
	b := newBaselineSampler(&scriptedSampler{values: []int64{1000, 1000, 1005, 1042}})
	cases := []int64{0, 0, 5, 42}
	for i, want := range cases {
		got, err := b.Sample()
		if err != nil {
			t.Fatalf("sample %d err: %v", i, err)
		}
		if got != want {
			t.Errorf("sample %d: want %d, got %d", i, want, got)
		}
	}
}

func TestBaselineSamplerPropagatesError(t *testing.T) {
	want := errors.New("sampler boom")
	b := newBaselineSampler(&scriptedSampler{
		values: []int64{0},
		errs:   []error{want},
	})
	if _, err := b.Sample(); !errors.Is(err, want) {
		t.Fatalf("want %v, got %v", want, err)
	}
	// A failed first sample must not latch a baseline — the next
	// successful read should be treated as the baseline.
	b2 := newBaselineSampler(&scriptedSampler{
		values: []int64{0, 500, 520},
		errs:   []error{want, nil, nil},
	})
	if _, err := b2.Sample(); err == nil {
		t.Fatal("want error on first call, got nil")
	}
	got, err := b2.Sample()
	if err != nil {
		t.Fatalf("second sample err: %v", err)
	}
	if got != 0 {
		t.Errorf("post-error first success: want 0 (new baseline), got %d", got)
	}
	got, err = b2.Sample()
	if err != nil {
		t.Fatalf("third sample err: %v", err)
	}
	if got != 20 {
		t.Errorf("delta after baseline: want 20, got %d", got)
	}
}

func TestBaselineSamplerReanchorsOnCounterReset(t *testing.T) {
	// Underlying goes 1000, 1100, 5 (reset), 7. After the reset the
	// wrapper must re-anchor at 5 and report 0, then 2.
	b := newBaselineSampler(&scriptedSampler{values: []int64{1000, 1100, 5, 7}})
	wantSeq := []int64{0, 100, 0, 2}
	for i, want := range wantSeq {
		got, err := b.Sample()
		if err != nil {
			t.Fatalf("sample %d err: %v", i, err)
		}
		if got != want {
			t.Errorf("sample %d: want %d, got %d", i, want, got)
		}
	}
}
