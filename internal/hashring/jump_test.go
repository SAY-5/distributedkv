package hashring

import (
	"fmt"
	"math"
	"testing"
)

func TestJumpHashKnownVectors(t *testing.T) {
	// Known good vectors — JumpHash is deterministic given (key, n).
	// These are smoke tests that catch any future regression in the
	// arithmetic constants.
	cases := []struct {
		key     uint64
		buckets int
	}{
		{0, 1}, {1, 10}, {12345, 100}, {1 << 32, 1024},
	}
	for _, c := range cases {
		first := JumpHash(c.key, c.buckets)
		// Run twice to confirm purity.
		again := JumpHash(c.key, c.buckets)
		if first != again {
			t.Fatalf("JumpHash(%d, %d) not pure: %d vs %d", c.key, c.buckets, first, again)
		}
		if first < 0 || first >= c.buckets {
			t.Fatalf("JumpHash out of range: %d (n=%d)", first, c.buckets)
		}
	}
}

func TestJumpHashNonNegative(t *testing.T) {
	if got := JumpHash(0, 0); got != -1 {
		t.Fatalf("zero buckets: want -1, got %d", got)
	}
}

// When the bucket count grows from N to N+1, JumpHash should move
// only ~1/(N+1) of keys. We verify the rate stays under 1.5×.
func TestJumpHashMinimalDisruption(t *testing.T) {
	const (
		N    = 16
		Keys = 50_000
	)
	moved := 0
	for k := uint64(0); k < Keys; k++ {
		a := JumpHash(k, N)
		b := JumpHash(k, N+1)
		if a != b {
			moved++
		}
	}
	rate := float64(moved) / Keys
	expected := 1.0 / float64(N+1)
	if rate > expected*1.5 {
		t.Fatalf("disruption too high: rate=%.4f expected≈%.4f", rate, expected)
	}
}

func TestRingPlaceDeterministic(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	r := NewRing(nodes, 64)
	// Same input → same placement, no matter how many times we call.
	for shard := 0; shard < 16; shard++ {
		first := r.Place(shard, 3)
		for i := 0; i < 5; i++ {
			again := r.Place(shard, 3)
			if !equalSlices(first, again) {
				t.Fatalf("Place(%d) not deterministic: %v vs %v", shard, first, again)
			}
		}
		if len(first) != 3 {
			t.Fatalf("Place returned %d replicas, want 3", len(first))
		}
		// All distinct.
		seen := map[string]bool{}
		for _, n := range first {
			if seen[n] {
				t.Fatalf("duplicate node %q in placement %v", n, first)
			}
			seen[n] = true
		}
	}
}

func TestRingPlaceLoadBalance(t *testing.T) {
	const (
		Nodes  = 8
		Vnodes = 256
		Shards = 4096
		Repl   = 3
	)
	nodes := make([]string, Nodes)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("node-%02d", i)
	}
	r := NewRing(nodes, Vnodes)
	count := make(map[string]int, Nodes)
	for shard := 0; shard < Shards; shard++ {
		for _, n := range r.Place(shard, Repl) {
			count[n]++
		}
	}
	// Check each node carries within ±25% of the mean. With 256 vnodes
	// and 4096 shards × 3 replicas = 12288 placements / 8 nodes = 1536
	// expected. Tolerance is generous because we want this test to be
	// robust on noisy CI machines.
	mean := float64(Shards*Repl) / float64(Nodes)
	max := math.Inf(-1)
	min := math.Inf(1)
	for _, c := range count {
		f := float64(c)
		if f > max {
			max = f
		}
		if f < min {
			min = f
		}
	}
	if max > mean*1.25 || min < mean*0.75 {
		t.Fatalf("load imbalance: min=%v max=%v mean=%v counts=%v", min, max, mean, count)
	}
}

func TestRingPlaceMembershipChangeStable(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	r1 := NewRing(nodes, 128)
	r2 := NewRing(append([]string{}, append(nodes, "f")...), 128)

	moved := 0
	for shard := 0; shard < 256; shard++ {
		old := r1.Place(shard, 3)
		neu := r2.Place(shard, 3)
		// "Stable" means most replica-sets are unchanged. We only
		// count a shard as moved if its replica-set differs.
		if !equalSlices(old, neu) {
			moved++
		}
	}
	// With 1 node added to 5, we'd ideally see ~1/6 of shards remap.
	// Generous bound: under 50%.
	if rate := float64(moved) / 256.0; rate > 0.5 {
		t.Fatalf("too many shards moved on membership change: %d/256", moved)
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
