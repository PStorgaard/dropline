package agg

import (
	"reflect"
	"testing"

	"github.com/PStorgaard/dropline/internal/control"
	"github.com/PStorgaard/dropline/internal/stream"
)

// BurstBuckets is mirrored once on the wire (control.BurstBuckets) and
// once on the value side (stream.BurstBuckets, re-used by agg). The two
// shapes must stay in lockstep — any field addition breaks JSON output.
// This test fails loudly if a future change drifts them apart.
func TestBurstBucketsParity(t *testing.T) {
	wire := reflect.TypeOf(control.BurstBuckets{})
	value := reflect.TypeOf(stream.BurstBuckets{})
	if wire.NumField() != value.NumField() {
		t.Fatalf("field count drift: control=%d stream=%d", wire.NumField(), value.NumField())
	}
	for i := 0; i < wire.NumField(); i++ {
		w, v := wire.Field(i), value.Field(i)
		if w.Name != v.Name || w.Type != v.Type || w.Tag != v.Tag {
			t.Errorf("field %d drift:\n control: %+v\n stream:  %+v", i, w, v)
		}
	}
}

// Sanity: zero-value StateSnapshot has nil Buckets, zero StreamView,
// and is safe to compare with reflect.DeepEqual against another zero.
func TestStateSnapshotZeroValue(t *testing.T) {
	a := StateSnapshot{}
	b := StateSnapshot{}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("zero values not deep-equal")
	}
	if a.Buckets != nil {
		t.Fatal("zero Buckets should be nil slice")
	}
}
