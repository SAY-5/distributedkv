// Package multishard wires the JumpHash router to N independent Raft
// groups inside a single kvd binary. Each shard owns its own FSM,
// log, snapshot store, and TCP transport; the request handler picks
// a shard via JumpHash(key) before dispatching to that shard's Apply.
//
// This is what the v1 architecture promised but only unit-tested:
// hashring.JumpHash + a vnode ring. v3 makes it real.
package multishard

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/SAY-5/distributedkv/internal/fsm"
	"github.com/SAY-5/distributedkv/internal/hashring"
	"github.com/SAY-5/distributedkv/internal/raftnode"
	"github.com/hashicorp/raft"
)

// Shard is one Raft group + its FSM. The cluster owns the lifecycle.
type Shard struct {
	ID   int
	Node *raftnode.Node
	FSM  *fsm.KV
}

// Cluster is the per-node multi-shard coordinator.
type Cluster struct {
	mu     sync.RWMutex
	shards []*Shard
}

// Config describes how to spin up the local node's shards.
type Config struct {
	NodeID     string         // unique within the deployment
	NumShards  int            // total shards in the cluster (must be the same on every node)
	BasePort   int            // shard 0 listens on BasePort, shard N on BasePort+N
	BindHost   string         // typically "0.0.0.0"
	AdvertHost string         // typically the node's reachable hostname
	DataDir    string         // /<dir>/<shard_id>/{log.bolt,...}
	Bootstrap  bool           // true on cluster init
	Peers      []raft.Server  // for bootstrap: the per-shard peer list, advert addrs
}

// Spawn brings up `cfg.NumShards` shards on this node. The peers list
// provided is reused per shard with port offsets — the assumption is
// that every node runs the same shard count on consecutive ports
// starting at BasePort. Real production would use a registry; for
// the demo this is sufficient and observable.
func Spawn(cfg Config) (*Cluster, error) {
	if cfg.NumShards <= 0 {
		return nil, errors.New("multishard: NumShards must be > 0")
	}
	if cfg.NodeID == "" {
		return nil, errors.New("multishard: NodeID required")
	}
	c := &Cluster{}
	for i := 0; i < cfg.NumShards; i++ {
		shardPeers := offsetPeers(cfg.Peers, i)
		bindAddr := fmt.Sprintf("%s:%d", cfg.BindHost, cfg.BasePort+i)
		advert := fmt.Sprintf("%s:%d", cfg.AdvertHost, cfg.BasePort+i)
		kv := fsm.New()
		node, err := raftnode.New(raftnode.Config{
			NodeID:     fmt.Sprintf("%s/s%d", cfg.NodeID, i),
			BindAddr:   bindAddr,
			AdvertAddr: advert,
			DataDir:    filepath.Join(cfg.DataDir, fmt.Sprintf("shard-%d", i)),
			Bootstrap:  cfg.Bootstrap,
			Peers:      shardPeers,
			FSM:        kv,
		})
		if err != nil {
			c.Shutdown()
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
		c.shards = append(c.shards, &Shard{ID: i, Node: node, FSM: kv})
	}
	return c, nil
}

func offsetPeers(peers []raft.Server, shardIdx int) []raft.Server {
	out := make([]raft.Server, 0, len(peers))
	for _, p := range peers {
		out = append(out, raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("%s/s%d", p.ID, shardIdx)),
			Address: raft.ServerAddress(offsetAddr(string(p.Address), shardIdx)),
		})
	}
	return out
}

func offsetAddr(addr string, idx int) string {
	colon := strings.LastIndex(addr, ":")
	if colon < 0 {
		return addr
	}
	host := addr[:colon]
	var port int
	_, _ = fmt.Sscanf(addr[colon+1:], "%d", &port)
	return fmt.Sprintf("%s:%d", host, port+idx)
}

// ShardFor maps a key to a shard ID via JumpHash.
func (c *Cluster) ShardFor(key string) int {
	return hashring.JumpHash(hashring.HashKey(key), c.NumShards())
}

// NumShards returns the live shard count.
func (c *Cluster) NumShards() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.shards)
}

// Get returns the shard at the given index, or nil if out of range.
func (c *Cluster) Get(idx int) *Shard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if idx < 0 || idx >= len(c.shards) {
		return nil
	}
	return c.shards[idx]
}

// All returns a snapshot of every shard. Caller must not mutate.
func (c *Cluster) All() []*Shard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Shard, len(c.shards))
	copy(out, c.shards)
	return out
}

// Shutdown stops every shard's Raft node. Safe to call multiple times.
func (c *Cluster) Shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.shards {
		if s != nil && s.Node != nil {
			_ = s.Node.Shutdown()
		}
	}
	c.shards = nil
}

// LeaderShards returns the IDs of shards where this node is the leader.
// Used by /metrics so an operator sees per-node leadership distribution.
func (c *Cluster) LeaderShards() []int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var leaders []int
	for _, s := range c.shards {
		if s.Node.State() == "Leader" {
			leaders = append(leaders, s.ID)
		}
	}
	return leaders
}

// WaitAllLeaders blocks until every shard on this node has a leader
// (any node, not necessarily us). Used by tests + the bootstrap path
// so we don't service requests before every shard's group has agreed.
func (c *Cluster) WaitAllLeaders(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		all := len(c.shards) > 0
		for _, s := range c.shards {
			if s.Node.Leader() == "" {
				all = false
				break
			}
		}
		c.mu.RUnlock()
		if all {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// WaitAnyLeader blocks until at least one shard on this node has
// elected a leader (any node, not necessarily us). Used by tests +
// the bootstrap path so we don't service requests before consensus.
func (c *Cluster) WaitAnyLeader(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		any := false
		for _, s := range c.shards {
			if s.Node.Leader() != "" {
				any = true
				break
			}
		}
		c.mu.RUnlock()
		if any {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
