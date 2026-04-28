package fsm_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/SAY-5/distributedkv/internal/fsm"
	"github.com/hashicorp/raft"
)

func mustApplyOK(t *testing.T, kv *fsm.KV, c fsm.Command) {
	t.Helper()
	data, _ := json.Marshal(c)
	res, ok := kv.Apply(&raft.Log{Data: data}).(fsm.ApplyResult)
	if !ok || !res.OK {
		t.Fatalf("apply failed: %+v", res)
	}
}

func TestWatchReceivesPutAndDelete(t *testing.T) {
	kv := fsm.New()
	sub := kv.Watch().Subscribe("", 16)
	defer sub.Close()

	mustApplyOK(t, kv, fsm.Command{Kind: fsm.OpPut, Key: "users:alice", Value: []byte("v1")})
	mustApplyOK(t, kv, fsm.Command{Kind: fsm.OpPut, Key: "users:alice", Value: []byte("v2")})
	mustApplyOK(t, kv, fsm.Command{Kind: fsm.OpDelete, Key: "users:alice"})

	want := []struct {
		kind fsm.CommandKind
		ver  uint64
	}{
		{fsm.OpPut, 1},
		{fsm.OpPut, 2},
		{fsm.OpDelete, 0},
	}
	for i, w := range want {
		select {
		case ev := <-sub.C:
			if ev.Kind != w.kind || ev.Version != w.ver {
				t.Fatalf("event %d: got kind=%s ver=%d, want %s/%d",
					i, ev.Kind, ev.Version, w.kind, w.ver)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

func TestWatchPrefixFilter(t *testing.T) {
	kv := fsm.New()
	users := kv.Watch().Subscribe("users:", 16)
	defer users.Close()
	orders := kv.Watch().Subscribe("orders:", 16)
	defer orders.Close()

	mustApplyOK(t, kv, fsm.Command{Kind: fsm.OpPut, Key: "users:bob", Value: []byte("x")})
	mustApplyOK(t, kv, fsm.Command{Kind: fsm.OpPut, Key: "orders:1", Value: []byte("y")})

	select {
	case e := <-users.C:
		if e.Key != "users:bob" {
			t.Fatalf("users sub got wrong key: %s", e.Key)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("users sub timed out")
	}
	select {
	case e := <-orders.C:
		if e.Key != "orders:1" {
			t.Fatalf("orders sub got wrong key: %s", e.Key)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("orders sub timed out")
	}
	// users sub must NOT have received the order event.
	select {
	case e := <-users.C:
		t.Fatalf("users sub leaked event: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchSlowConsumerDropped(t *testing.T) {
	kv := fsm.New()
	sub := kv.Watch().Subscribe("", 2)
	// Don't drain; channel fills, then we get dropped.
	for i := 0; i < 10; i++ {
		mustApplyOK(t, kv, fsm.Command{Kind: fsm.OpPut, Key: "k", Value: []byte("v")})
	}
	// Drain whatever did make it; channel should eventually close
	// because the manager dropped us.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, alive := <-sub.C
		if !alive {
			break
		}
	}
	if kv.Watch().Dropped() == 0 {
		t.Fatal("expected dropped > 0")
	}
}

func TestWatchOnlyPublishedOnSuccess(t *testing.T) {
	kv := fsm.New()
	sub := kv.Watch().Subscribe("", 4)
	defer sub.Close()
	// Failed CAS — version mismatch — must NOT publish.
	data, _ := json.Marshal(fsm.Command{
		Kind: fsm.OpCAS, Key: "x", Value: []byte("v"), ExpectedVersion: 99,
	})
	res, _ := kv.Apply(&raft.Log{Data: data}).(fsm.ApplyResult)
	if res.OK {
		t.Fatal("expected CAS to fail")
	}
	select {
	case e := <-sub.C:
		t.Fatalf("CAS-failure must not publish, got %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}
