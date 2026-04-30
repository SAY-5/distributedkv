// Package backup is the v4 disaster-recovery layer on top of the
// in-memory FSM.
//
// Raft itself snapshots the FSM periodically. Backup adds:
// - point-in-time export of the FSM as a portable JSON file
// - import that resets the FSM to a saved snapshot
//
// Production uses this for periodic offsite backups to S3 / GCS,
// pre-deploy snapshots before risky migrations, and for spinning
// up a pre-warmed cluster from production data in staging.

package backup

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
)

// Snapshot is a JSON-serializable point-in-time FSM image.
type Snapshot struct {
	Version int               `json:"version"`
	Pairs   map[string]string `json:"pairs"`
}

// Save writes a snapshot to `path` atomically (write-then-rename
// so a partial write never replaces a good backup).
func Save(s *Snapshot, path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	if err := json.NewEncoder(w).Encode(s); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Load reads a snapshot from `path`.
func Load(path string) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var s Snapshot
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		if errors.Is(err, io.EOF) {
			return &Snapshot{Version: 1, Pairs: map[string]string{}}, nil
		}
		return nil, err
	}
	if s.Pairs == nil {
		s.Pairs = map[string]string{}
	}
	return &s, nil
}

// MakeSnapshot builds a snapshot from a kv map.
func MakeSnapshot(kv map[string]string) *Snapshot {
	cp := make(map[string]string, len(kv))
	for k, v := range kv {
		cp[k] = v
	}
	return &Snapshot{Version: 1, Pairs: cp}
}
