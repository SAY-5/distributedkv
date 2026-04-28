// Watcher fan-out: when the FSM applies a write, anyone subscribed to
// a prefix that matches the changed key gets notified.
//
// Design choices:
//   - Channels per subscriber, not a global event bus, so a slow
//     subscriber doesn't head-of-line block the others.
//   - Bounded channel (capacity 64) — if a subscriber falls behind it
//     gets dropped and reconnects instead of forcing the FSM to block.
//   - Subscriptions are prefix-matched on the key, not regex; cheap
//     and covers the 95% case (tenants, namespaces, etc.).

package fsm

import (
	"strings"
	"sync"
	"time"
)

// WatchEvent is what subscribers receive.
type WatchEvent struct {
	Kind    CommandKind `json:"kind"`
	Key     string      `json:"key"`
	Value   []byte      `json:"value,omitempty"`
	Version uint64      `json:"version"`
	At      time.Time   `json:"at"`
}

// Subscription is the receive side. Close to unsubscribe.
type Subscription struct {
	id     uint64
	prefix string
	C      chan WatchEvent
	mgr    *WatchManager
}

func (s *Subscription) Close() {
	if s.mgr != nil {
		s.mgr.unsubscribe(s.id)
	}
}

// WatchManager owns the subscriber list. The KV's Apply path calls
// Publish after every successful write.
type WatchManager struct {
	mu     sync.RWMutex
	nextID uint64
	subs   map[uint64]*Subscription
	dropped uint64
}

func NewWatchManager() *WatchManager {
	return &WatchManager{subs: map[uint64]*Subscription{}}
}

// Subscribe returns a Subscription whose C channel emits WatchEvents
// for keys matching `prefix`. Pass an empty prefix to receive every
// event. The caller must Close() it.
func (w *WatchManager) Subscribe(prefix string, bufSize int) *Subscription {
	if bufSize <= 0 {
		bufSize = 64
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextID++
	s := &Subscription{
		id:     w.nextID,
		prefix: prefix,
		C:      make(chan WatchEvent, bufSize),
		mgr:    w,
	}
	w.subs[s.id] = s
	return s
}

func (w *WatchManager) unsubscribe(id uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if s, ok := w.subs[id]; ok {
		delete(w.subs, id)
		close(s.C)
	}
}

// Publish notifies any subscribers whose prefix matches the event key.
// Non-blocking: if a subscriber's channel is full it gets dropped and
// removed (the operator sees the dropped counter and knows to reconnect).
func (w *WatchManager) Publish(e WatchEvent) {
	w.mu.RLock()
	matches := make([]*Subscription, 0, 4)
	for _, s := range w.subs {
		if s.prefix == "" || strings.HasPrefix(e.Key, s.prefix) {
			matches = append(matches, s)
		}
	}
	w.mu.RUnlock()
	var toRemove []uint64
	for _, s := range matches {
		select {
		case s.C <- e:
			// delivered
		default:
			toRemove = append(toRemove, s.id)
		}
	}
	if len(toRemove) > 0 {
		w.mu.Lock()
		for _, id := range toRemove {
			if s, ok := w.subs[id]; ok {
				delete(w.subs, id)
				close(s.C)
				w.dropped++
			}
		}
		w.mu.Unlock()
	}
}

// SubCount is for /metrics.
func (w *WatchManager) SubCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.subs)
}

// Dropped is for /metrics — total dropped subscribers since boot.
func (w *WatchManager) Dropped() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.dropped
}
