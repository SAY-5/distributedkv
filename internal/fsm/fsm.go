// Package fsm is the in-memory state machine that's driven by Raft.
//
// It implements hashicorp/raft.FSM:
//
//	Apply(*raft.Log)        — interpret one committed command
//	Snapshot() (FSMSnapshot, error)
//	Restore(io.ReadCloser)
//
// Commands are JSON-encoded for cross-version readability. Snapshots
// are gzipped JSON. Both formats trade a small CPU cost for being
// trivially debuggable when something goes wrong in production.
package fsm

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// CommandKind enumerates the FSM verbs.
type CommandKind string

const (
	OpPut    CommandKind = "PUT"
	OpDelete CommandKind = "DEL"
	OpCAS    CommandKind = "CAS"
	OpTouch  CommandKind = "TOUCH"
)

// Command is the JSON wire form of a Raft log entry.
type Command struct {
	Kind            CommandKind `json:"k"`
	Key             string      `json:"key"`
	Value           []byte      `json:"v,omitempty"`
	ExpectedVersion uint64      `json:"ev,omitempty"` // 0 = no CAS check
	TTLSec          int64       `json:"ttl,omitempty"`
	Issued          int64       `json:"t,omitempty"` // unix milli (issuer clock)
}

// Entry is the in-memory record per key.
type Entry struct {
	Value     []byte    `json:"v"`
	Version   uint64    `json:"ver"`
	UpdatedAt time.Time `json:"updated_at"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// ApplyResult is what FSM.Apply returns. Callers (the gRPC layer) use
// it to decide whether to surface an error to the client.
type ApplyResult struct {
	OK              bool
	NewVersion      uint64
	ConflictVersion uint64 // if a CAS failed, the on-disk version that didn't match
	Err             string
}

// KV is the FSM. Safe for concurrent reads; writes only via Apply,
// which Raft serializes for us.
type KV struct {
	mu      sync.RWMutex
	data    map[string]Entry
	version uint64 // global monotonic; applied count
	now     func() time.Time
	watch   *WatchManager
}

// New constructs an empty FSM.
func New() *KV {
	return &KV{
		data:  make(map[string]Entry),
		now:   time.Now,
		watch: NewWatchManager(),
	}
}

// Watch exposes the WatchManager so callers can Subscribe / inspect
// /metrics counters. Returns nil only if the FSM is uninitialized.
func (k *KV) Watch() *WatchManager { return k.watch }

// SetClock is used in tests to make time deterministic.
func (k *KV) SetClock(now func() time.Time) {
	k.mu.Lock()
	k.now = now
	k.mu.Unlock()
}

// --- raft.FSM ---------------------------------------------------------

// Apply interprets a single Raft log entry. Errors are returned as
// values inside ApplyResult so they're committed to the log alongside
// the operation — a CAS conflict is a *normal* outcome, not a Raft
// error.
func (k *KV) Apply(log *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return ApplyResult{OK: false, Err: "bad command: " + err.Error()}
	}
	res := k.applyLocked(log, cmd)
	// Notify watchers *outside* the FSM mutex (the publish path takes
	// its own lock and can fan out to many subscribers; we don't want
	// that on the apply critical path).
	if r, ok := res.(ApplyResult); ok && r.OK && k.watch != nil {
		k.watch.Publish(WatchEvent{
			Kind:    cmd.Kind,
			Key:     cmd.Key,
			Value:   cmd.Value,
			Version: r.NewVersion,
			At:      k.now(),
		})
	}
	return res
}

func (k *KV) applyLocked(log *raft.Log, cmd Command) any {
	k.mu.Lock()
	defer k.mu.Unlock()
	// CAS is a tagging convenience for the log; it executes as a PUT
	// with an explicit expected version. Rewrite once, before the
	// switch, so a single code path handles both.
	if cmd.Kind == OpCAS {
		cmd.Kind = OpPut
	}
	now := k.now()
	switch cmd.Kind {
	case OpPut:
		cur, exists := k.data[cmd.Key]
		// CAS check (PUT carries an expected version too — used by Cas RPC)
		if cmd.ExpectedVersion != 0 {
			var curVer uint64
			if exists {
				curVer = cur.Version
			}
			if cmd.ExpectedVersion != curVer {
				return ApplyResult{OK: false, ConflictVersion: curVer, Err: "version_mismatch"}
			}
		}
		var newVer uint64 = 1
		if exists {
			newVer = cur.Version + 1
		}
		entry := Entry{
			Value:     append([]byte(nil), cmd.Value...),
			Version:   newVer,
			UpdatedAt: now,
		}
		if cmd.TTLSec > 0 {
			entry.ExpiresAt = now.Add(time.Duration(cmd.TTLSec) * time.Second)
		}
		k.data[cmd.Key] = entry
		k.version++
		return ApplyResult{OK: true, NewVersion: newVer}

	case OpDelete:
		cur, exists := k.data[cmd.Key]
		if !exists {
			return ApplyResult{OK: true, NewVersion: 0}
		}
		if cmd.ExpectedVersion != 0 && cur.Version != cmd.ExpectedVersion {
			return ApplyResult{OK: false, ConflictVersion: cur.Version, Err: "version_mismatch"}
		}
		delete(k.data, cmd.Key)
		k.version++
		return ApplyResult{OK: true}

	case OpTouch:
		cur, exists := k.data[cmd.Key]
		if !exists {
			return ApplyResult{OK: false, Err: "not_found"}
		}
		if cmd.TTLSec <= 0 {
			cur.ExpiresAt = time.Time{}
		} else {
			cur.ExpiresAt = now.Add(time.Duration(cmd.TTLSec) * time.Second)
		}
		cur.Version++
		k.data[cmd.Key] = cur
		k.version++
		return ApplyResult{OK: true, NewVersion: cur.Version}

	default:
		return ApplyResult{OK: false, Err: "unknown kind: " + string(cmd.Kind)}
	}
}

// --- read API (called outside Raft) -----------------------------------

// Get returns the value, version, and existence flag for a key.
// TTL-expired entries are reported as missing but not actively swept.
func (k *KV) Get(key string) (value []byte, version uint64, ok bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	e, exists := k.data[key]
	if !exists {
		return nil, 0, false
	}
	if !e.ExpiresAt.IsZero() && k.now().After(e.ExpiresAt) {
		return nil, 0, false
	}
	out := append([]byte(nil), e.Value...)
	return out, e.Version, true
}

// Len returns the number of live keys (TTL not enforced for the count).
func (k *KV) Len() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return len(k.data)
}

// Version returns the global monotonic apply counter — useful for
// readers that want to confirm they've seen up to N.
func (k *KV) Version() uint64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.version
}

// --- snapshots --------------------------------------------------------

type snapshotState struct {
	Data    map[string]Entry `json:"data"`
	Version uint64           `json:"version"`
}

// Snapshot returns a point-in-time copy of the FSM. Raft will hold no
// locks while the snapshot persists, so we deep-copy here and let the
// snapshot.Persist run lock-free.
func (k *KV) Snapshot() (raft.FSMSnapshot, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	cp := make(map[string]Entry, len(k.data))
	for kk, vv := range k.data {
		// Defensive copy of value bytes.
		cp[kk] = Entry{
			Value:     append([]byte(nil), vv.Value...),
			Version:   vv.Version,
			UpdatedAt: vv.UpdatedAt,
			ExpiresAt: vv.ExpiresAt,
		}
	}
	return &fsmSnapshot{state: snapshotState{Data: cp, Version: k.version}}, nil
}

// Restore replaces the FSM contents from a snapshot.
func (k *KV) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	gr, err := gzip.NewReader(rc)
	if err != nil {
		return fmt.Errorf("gzip open: %w", err)
	}
	defer gr.Close()
	var s snapshotState
	if err := json.NewDecoder(gr).Decode(&s); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if s.Data == nil {
		s.Data = make(map[string]Entry)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data = s.Data
	k.version = s.Version
	return nil
}

type fsmSnapshot struct {
	state snapshotState
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	gw := gzip.NewWriter(sink)
	if err := json.NewEncoder(gw).Encode(&s.state); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("encode: %w", err)
	}
	if err := gw.Close(); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("gzip close: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
