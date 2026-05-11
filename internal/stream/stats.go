package stream

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// BurstBuckets holds the loss-burst histogram counts and max-burst.
// Buckets per spec § internal/stream: 1, 2, 3-9, 10-99, 100+.
//
// Deliberate duplicate of control.BurstBuckets — the wire schema in
// internal/control owns its own copy so neither layer imports the other.
// Field tags and shape MUST stay in lockstep.
type BurstBuckets struct {
	One       int64 `json:"1"`
	Two       int64 `json:"2"`
	Three9    int64 `json:"3-9"`
	Ten99     int64 `json:"10-99"`
	HundredUp int64 `json:"100+"`
	Max       int64 `json:"max"`
}

// Snapshot is an immutable point-in-time view of receiver-side counters,
// produced once per second by Receiver and at test end. Construction is
// guarded by Accumulator's mutex; consumers may share without locking.
type Snapshot struct {
	// Elapsed seconds since the accumulator's t0.
	T float64
	// Cumulative counts.
	Recv        int64
	Lost        int64
	OutOfOrder  int64
	Duplicates  int64
	LocalDrops  int64 // receiver ring-overflow, NOT network loss
	KernelDrops int64
	// Highest seq observed so far (used by callers that need a "sent" estimate
	// when no sender-side count is available).
	MaxSeq uint64
	// Jitter (RFC 3550 EWMA) in milliseconds.
	JitterMS float64
	// Burst histogram of consecutive-loss runs.
	Bursts BurstBuckets
}

// Accumulator is the receiver's stat-keeping state. All exported methods
// are safe for concurrent use.
//
// Design note: per spec § internal/stream, the burst histogram is computed
// streamingly: each time we observe a seq strictly greater than the next
// expected seq, the gap is recorded as one burst. Out-of-order recovery
// (a late packet filling a gap) is counted in OutOfOrder but does not
// retroactively shrink a burst. This matches the "good-enough" streaming
// model the spec assumes; refining it would require deferring burst
// recording past a reorder window.
type Accumulator struct {
	mu sync.Mutex

	t0 time.Time

	recv        int64
	outOfOrder  int64
	duplicates  int64
	lost        int64
	kernelDrops int64

	// localDrops is touched by the drainer (direct-conn mode) or the
	// Run goroutine (hub-fed mode) without holding mu, so the stats
	// goroutine doesn't contend with it in Observe.
	localDrops atomic.Int64

	expectedSeq uint64
	maxSeq      uint64
	seenAny     bool

	// seen tracks every seq ever observed, for duplicate detection. At a
	// 60-second test running 1042 pps the map peaks at ~63k entries; the
	// memory cost (~3MB at 48B/entry) is acceptable for v1. A bitmap-backed
	// implementation can replace this if profiling shows it matters.
	seen map[uint64]struct{}

	// RFC 3550 jitter state.
	lastArrival   time.Time
	lastTransitMS float64
	hasTransit    bool
	jitterMS      float64

	bursts BurstBuckets
}

// NewAccumulator returns a fresh Accumulator with t0 as the test start time.
// All elapsed timestamps in Snapshots are measured from t0.
func NewAccumulator(t0 time.Time) *Accumulator {
	return &Accumulator{
		t0:   t0,
		seen: make(map[uint64]struct{}),
	}
}

// Observe records a single packet identified by its decoded header and the
// time at which the receiver pulled it off the socket. It is safe to call
// from one goroutine only — typically the stats goroutine.
func (a *Accumulator) Observe(h Header, arrival time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, dup := a.seen[h.Seq]; dup {
		a.duplicates++
		return
	}
	a.seen[h.Seq] = struct{}{}
	a.recv++

	if !a.seenAny {
		a.seenAny = true
		a.maxSeq = h.Seq
		a.expectedSeq = h.Seq + 1
		if h.Seq > 0 {
			// Initial gap from 0..seq-1: record as one burst.
			a.recordBurst(int64(h.Seq))
			a.lost += int64(h.Seq) // #nosec G115 -- bounded by sender duration
		}
	} else if h.Seq >= a.expectedSeq {
		gap := h.Seq - a.expectedSeq
		if gap > 0 {
			a.recordBurst(int64(gap)) // #nosec G115 -- bounded by sender duration
			a.lost += int64(gap)      // #nosec G115 -- bounded by sender duration
		}
		a.expectedSeq = h.Seq + 1
		if h.Seq > a.maxSeq {
			a.maxSeq = h.Seq
		}
	} else {
		// seq < expectedSeq, fresh — late arrival of a gap that was
		// previously counted as lost when expectedSeq advanced past it.
		// Roll lost back; bursts stay as-is (streaming model per design note).
		a.outOfOrder++
		if a.lost > 0 {
			a.lost--
		}
	}

	a.updateJitter(h, arrival)
}

func (a *Accumulator) updateJitter(h Header, arrival time.Time) {
	// transit_ms = arrival - tx, in milliseconds. We only need the *changes*
	// in transit between successive packets, so absolute clock skew between
	// sender and receiver cancels out.
	transitMS := float64(arrival.UnixNano()-h.TxUnixNS) / 1e6
	if !a.hasTransit {
		a.hasTransit = true
		a.lastTransitMS = transitMS
		a.lastArrival = arrival
		return
	}
	d := math.Abs(transitMS - a.lastTransitMS)
	// RFC 3550: J = J + (|D| - J) / 16
	a.jitterMS += (d - a.jitterMS) / 16
	a.lastTransitMS = transitMS
	a.lastArrival = arrival
}

func (a *Accumulator) recordBurst(n int64) {
	if n <= 0 {
		return
	}
	switch {
	case n == 1:
		a.bursts.One++
	case n == 2:
		a.bursts.Two++
	case n <= 9:
		a.bursts.Three9++
	case n <= 99:
		a.bursts.Ten99++
	default:
		a.bursts.HundredUp++
	}
	if n > a.bursts.Max {
		a.bursts.Max = n
	}
}

// AddLocalDrop records a drainer ring-overflow. These bytes were dropped
// inside the receiver process and must not be conflated with network loss.
// Lock-free: localDrops is atomic so the drainer hot path doesn't contend
// with the stats goroutine for mu.
func (a *Accumulator) AddLocalDrop(n int64) {
	a.localDrops.Add(n)
}

// SetKernelDrops overwrites the cumulative kernel-drop count. The sampler
// is platform-specific and runs out-of-band of the packet path.
func (a *Accumulator) SetKernelDrops(n int64) {
	a.mu.Lock()
	a.kernelDrops = n
	a.mu.Unlock()
}

// Snapshot returns an immutable view of the current counters at time now.
// now is typically time.Now() from the caller; making it a parameter keeps
// the accumulator deterministic in tests.
func (a *Accumulator) Snapshot(now time.Time) Snapshot {
	localDrops := a.localDrops.Load()
	a.mu.Lock()
	defer a.mu.Unlock()
	return Snapshot{
		T:           now.Sub(a.t0).Seconds(),
		Recv:        a.recv,
		Lost:        a.lost,
		OutOfOrder:  a.outOfOrder,
		Duplicates:  a.duplicates,
		LocalDrops:  localDrops,
		KernelDrops: a.kernelDrops,
		MaxSeq:      a.maxSeq,
		JitterMS:    a.jitterMS,
		Bursts:      a.bursts,
	}
}
