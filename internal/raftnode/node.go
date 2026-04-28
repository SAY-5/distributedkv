// Package raftnode wraps hashicorp/raft into a small, opinionated
// constructor.
//
// Design choices:
//
//   - Use TCPTransport (not the gRPC transport) — fewer moving parts;
//     boltdb covers durability; nothing in the path requires a stream
//     multiplexer beyond what raft.NetworkTransport already provides.
//   - Use raft-boltdb/v2 for the log store and the stable store. Single
//     boltdb file each; bounded growth (hashicorp/raft truncates).
//   - File-based snapshot store retained at 3 — enough for safety,
//     small enough to avoid disk creep on dev clusters.
//   - Bootstrap is opt-in via Config.Bootstrap. Other nodes join an
//     already-bootstrapped cluster by being listed in the leader's
//     Voters; we expose AddVoter / RemoveVoter on Node.
package raftnode

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Config sets up a single Raft node.
type Config struct {
	NodeID     string         // unique within the cluster
	BindAddr   string         // host:port the raft transport listens on
	AdvertAddr string         // address peers should reach us at (defaults to BindAddr)
	DataDir    string         // working dir for raft state
	Bootstrap  bool           // true on the first node only
	Peers      []raft.Server  // for bootstrap: initial voter set (must include self)
	FSM        raft.FSM // application-supplied state machine
}

// Node owns one Raft replica.
type Node struct {
	Raft   *raft.Raft
	cfg    Config
	transp *raft.NetworkTransport
}

// New starts a Raft node according to cfg. The node is ready when the
// constructor returns; if Bootstrap was set and no existing state was
// found, the cluster is bootstrapped to cfg.Peers.
func New(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("raftnode: NodeID required")
	}
	if cfg.BindAddr == "" {
		return nil, errors.New("raftnode: BindAddr required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("raftnode: DataDir required")
	}
	if cfg.FSM == nil {
		return nil, errors.New("raftnode: FSM required")
	}
	if cfg.AdvertAddr == "" {
		cfg.AdvertAddr = cfg.BindAddr
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data: %w", err)
	}
	snapDir := filepath.Join(cfg.DataDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir snapshots: %w", err)
	}

	logStore, err := boltdb.NewBoltStore(filepath.Join(cfg.DataDir, "log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("log store: %w", err)
	}
	stableStore, err := boltdb.NewBoltStore(filepath.Join(cfg.DataDir, "stable.bolt"))
	if err != nil {
		return nil, fmt.Errorf("stable store: %w", err)
	}
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snap store: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", cfg.AdvertAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve advert: %w", err)
	}
	transp, err := raft.NewTCPTransport(cfg.BindAddr, addr, 4, 5*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("transport: %w", err)
	}

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(cfg.NodeID)
	// Slightly snappier elections for the demo cluster. In a wide-area
	// deployment you'd want these closer to defaults (1s/2s).
	rcfg.HeartbeatTimeout = 500 * time.Millisecond
	rcfg.ElectionTimeout = 500 * time.Millisecond
	rcfg.LeaderLeaseTimeout = 250 * time.Millisecond
	rcfg.CommitTimeout = 50 * time.Millisecond
	rcfg.SnapshotInterval = 30 * time.Second
	rcfg.SnapshotThreshold = 1024
	rcfg.LogLevel = "WARN"

	r, err := raft.NewRaft(rcfg, cfg.FSM, logStore, stableStore, snapStore, transp)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
		if err != nil {
			return nil, fmt.Errorf("has state: %w", err)
		}
		if !hasState {
			peers := cfg.Peers
			if len(peers) == 0 {
				peers = []raft.Server{{
					ID:      raft.ServerID(cfg.NodeID),
					Address: transp.LocalAddr(),
				}}
			}
			f := r.BootstrapCluster(raft.Configuration{Servers: peers})
			if err := f.Error(); err != nil {
				return nil, fmt.Errorf("bootstrap: %w", err)
			}
		}
	}

	return &Node{Raft: r, cfg: cfg, transp: transp}, nil
}

// Apply submits a command to the Raft log. Only the leader will accept
// it; followers return raft.ErrNotLeader.
func (n *Node) Apply(cmd []byte, timeout time.Duration) (any, error) {
	f := n.Raft.Apply(cmd, timeout)
	if err := f.Error(); err != nil {
		return nil, err
	}
	return f.Response(), nil
}

// AddVoter adds a new voting member. Idempotent for already-present nodes.
func (n *Node) AddVoter(id, addr string, timeout time.Duration) error {
	f := n.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, timeout)
	return f.Error()
}

// RemoveServer removes a peer from the configuration.
func (n *Node) RemoveServer(id string, timeout time.Duration) error {
	f := n.Raft.RemoveServer(raft.ServerID(id), 0, timeout)
	return f.Error()
}

// Leader returns the current leader's address (empty if unknown).
func (n *Node) Leader() string {
	addr, _ := n.Raft.LeaderWithID()
	return string(addr)
}

// State returns the raft state name (Leader/Follower/Candidate).
func (n *Node) State() string {
	return n.Raft.State().String()
}

// VerifyLeader is a barrier read: it confirms with a quorum that we
// remain the leader. Used for linearizable reads via ReadIndex.
func (n *Node) VerifyLeader() error {
	return n.Raft.VerifyLeader().Error()
}

// Barrier waits for all preceding log entries to be applied to the FSM,
// which combined with VerifyLeader gives linearizability for reads.
func (n *Node) Barrier(timeout time.Duration) error {
	return n.Raft.Barrier(timeout).Error()
}

// Shutdown stops the node cleanly and closes its transport.
func (n *Node) Shutdown() error {
	if err := n.Raft.Shutdown().Error(); err != nil {
		return err
	}
	return n.transp.Close()
}

// LocalAddr returns the address other nodes should use to reach this one.
func (n *Node) LocalAddr() string { return string(n.transp.LocalAddr()) }
