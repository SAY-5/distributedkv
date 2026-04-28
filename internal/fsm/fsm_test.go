package fsm

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func mustApply(t *testing.T, kv *KV, c Command) ApplyResult {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	res := kv.Apply(&raft.Log{Data: data})
	r, ok := res.(ApplyResult)
	if !ok {
		t.Fatalf("Apply returned %T, want ApplyResult", res)
	}
	return r
}

func TestPutGet(t *testing.T) {
	kv := New()
	r := mustApply(t, kv, Command{Kind: OpPut, Key: "a", Value: []byte("hello")})
	if !r.OK || r.NewVersion != 1 {
		t.Fatalf("unexpected put result: %+v", r)
	}
	v, ver, ok := kv.Get("a")
	if !ok || string(v) != "hello" || ver != 1 {
		t.Fatalf("get: ok=%v v=%q ver=%d", ok, v, ver)
	}
}

func TestVersionMonotone(t *testing.T) {
	kv := New()
	for i := 0; i < 5; i++ {
		mustApply(t, kv, Command{Kind: OpPut, Key: "k", Value: []byte("v")})
	}
	_, ver, ok := kv.Get("k")
	if !ok || ver != 5 {
		t.Fatalf("expected ver=5 got %d (ok=%v)", ver, ok)
	}
}

func TestCASConflict(t *testing.T) {
	kv := New()
	mustApply(t, kv, Command{Kind: OpPut, Key: "x", Value: []byte("1")})
	// Try to CAS with stale expected version.
	r := mustApply(t, kv, Command{Kind: OpCAS, Key: "x", ExpectedVersion: 99, Value: []byte("2")})
	if r.OK {
		t.Fatalf("CAS should have rejected stale version, got %+v", r)
	}
	if r.ConflictVersion != 1 {
		t.Fatalf("expected ConflictVersion=1 got %d", r.ConflictVersion)
	}
	// Correct CAS succeeds.
	r = mustApply(t, kv, Command{Kind: OpCAS, Key: "x", ExpectedVersion: 1, Value: []byte("2")})
	if !r.OK || r.NewVersion != 2 {
		t.Fatalf("CAS happy path: %+v", r)
	}
}

func TestDelete(t *testing.T) {
	kv := New()
	mustApply(t, kv, Command{Kind: OpPut, Key: "a", Value: []byte("v")})
	r := mustApply(t, kv, Command{Kind: OpDelete, Key: "a"})
	if !r.OK {
		t.Fatalf("delete: %+v", r)
	}
	if _, _, ok := kv.Get("a"); ok {
		t.Fatalf("key should be gone")
	}
}

func TestTTLExpiry(t *testing.T) {
	kv := New()
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	kv.SetClock(func() time.Time { return now })

	mustApply(t, kv, Command{Kind: OpPut, Key: "tmp", Value: []byte("v"), TTLSec: 5})
	if _, _, ok := kv.Get("tmp"); !ok {
		t.Fatal("key should be present right after PUT")
	}
	// Advance the clock past expiry.
	now = now.Add(10 * time.Second)
	kv.SetClock(func() time.Time { return now })
	if _, _, ok := kv.Get("tmp"); ok {
		t.Fatal("key should be expired")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	src := New()
	for i := 0; i < 100; i++ {
		mustApply(t, src, Command{
			Kind: OpPut, Key: kvkey(i), Value: []byte(kvkey(i) + "-value"),
		})
	}
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &mockSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	snap.Release()

	dst := New()
	if err := dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != src.Len() {
		t.Fatalf("len mismatch: %d vs %d", dst.Len(), src.Len())
	}
	for i := 0; i < 100; i++ {
		v, ver, ok := dst.Get(kvkey(i))
		if !ok || string(v) != kvkey(i)+"-value" || ver != 1 {
			t.Fatalf("restore[%d]: ok=%v v=%q ver=%d", i, ok, v, ver)
		}
	}
}

func TestRestoreInvalidGzip(t *testing.T) {
	dst := New()
	err := dst.Restore(io.NopCloser(bytes.NewReader([]byte("not gzip"))))
	if err == nil {
		t.Fatal("expected error on bad snapshot")
	}
}

func TestSnapshotIsValidGzip(t *testing.T) {
	kv := New()
	mustApply(t, kv, Command{Kind: OpPut, Key: "a", Value: []byte("b")})
	snap, _ := kv.Snapshot()
	sink := &mockSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	gr, err := gzip.NewReader(bytes.NewReader(sink.Bytes()))
	if err != nil {
		t.Fatalf("snapshot is not a valid gzip stream: %v", err)
	}
	_, _ = io.Copy(io.Discard, gr)
	_ = gr.Close()
}

func kvkey(i int) string {
	return string(rune('a'+(i/26))) + string(rune('a'+(i%26)))
}

// --- raft.SnapshotSink test double ----

type mockSink struct {
	bytes.Buffer
	cancelled bool
}

func (m *mockSink) ID() string  { return "test" }
func (m *mockSink) Cancel() error { m.cancelled = true; return nil }
func (m *mockSink) Close() error  { return nil }
