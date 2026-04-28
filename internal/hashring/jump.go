// Package hashring provides the routing layer:
//
//	JumpHash    — Google's jump consistent hash for key→shard.
//	Ring        — vnode-based consistent hash for shard→nodes.
//
// Both are pure functions of the inputs; no global state, no locks.
package hashring

import (
	"hash/fnv"
	"sort"
)

// JumpHash returns the bucket in [0, numBuckets) for the given 64-bit
// key per Lamping & Veach 2014. It allocates nothing, runs in O(log N),
// and gives a stable mapping: when numBuckets grows from N to N+1,
// only ~1/(N+1) of keys move. That's the property we want for adding
// a shard: minimal data movement, no preallocated ring.
func JumpHash(key uint64, numBuckets int) int {
	if numBuckets <= 0 {
		return -1
	}
	var b, j int64 = -1, 0
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// HashKey produces a deterministic 64-bit digest for a string key.
// We use FNV-1a — fast, no external deps, more than enough for hashing
// keys across a few hundred shards.
func HashKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// Ring assigns each shard to a deterministic ordered set of nodes via
// a vnode-based consistent hash. For each (node, vnodeIdx) we hash the
// pair to a 64-bit slot; the ring is the slots sorted ascending. To
// place a shard with replication factor R we walk the ring from
// hash(shardID) and pick the first R distinct nodes.
//
// Why vnodes: a plain N-node consistent hash places ~1/N of shards on
// each node but with high variance. With V vnodes per node the
// variance drops as 1/sqrt(V). 128 is the standard choice; large
// enough for even spread, small enough that the Sort is cheap.
type Ring struct {
	slots []slot // sorted ascending by hash
}

type slot struct {
	hash uint64
	node string
}

// NewRing builds a ring from the given node IDs.
func NewRing(nodes []string, vnodesPerNode int) *Ring {
	if vnodesPerNode <= 0 {
		vnodesPerNode = 128
	}
	r := &Ring{slots: make([]slot, 0, len(nodes)*vnodesPerNode)}
	for _, n := range nodes {
		for v := 0; v < vnodesPerNode; v++ {
			r.slots = append(r.slots, slot{
				hash: vnodeHash(n, v),
				node: n,
			})
		}
	}
	sort.Slice(r.slots, func(i, j int) bool {
		return r.slots[i].hash < r.slots[j].hash
	})
	return r
}

// Place returns the ordered list of `replicas` distinct node IDs that
// own the given shard. Deterministic for a fixed input.
func (r *Ring) Place(shardID int, replicas int) []string {
	if len(r.slots) == 0 || replicas <= 0 {
		return nil
	}
	target := shardHash(shardID)
	// Binary search for the first slot ≥ target; wrap if past the end.
	idx := sort.Search(len(r.slots), func(i int) bool {
		return r.slots[i].hash >= target
	})
	out := make([]string, 0, replicas)
	seen := make(map[string]struct{}, replicas)
	for i := 0; i < len(r.slots) && len(out) < replicas; i++ {
		s := r.slots[(idx+i)%len(r.slots)]
		if _, dup := seen[s.node]; dup {
			continue
		}
		seen[s.node] = struct{}{}
		out = append(out, s.node)
	}
	return out
}

// Nodes returns the unique node IDs in the ring, sorted.
func (r *Ring) Nodes() []string {
	seen := make(map[string]struct{})
	for _, s := range r.slots {
		seen[s.node] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func vnodeHash(node string, vnode int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(node))
	_, _ = h.Write([]byte{byte(vnode), byte(vnode >> 8), byte(vnode >> 16), byte(vnode >> 24)})
	return h.Sum64()
}

func shardHash(shardID int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("shard:"))
	_, _ = h.Write([]byte{byte(shardID), byte(shardID >> 8), byte(shardID >> 16), byte(shardID >> 24)})
	return h.Sum64()
}
