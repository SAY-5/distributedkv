package backup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveThenLoadRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	original := MakeSnapshot(map[string]string{"a": "1", "b": "2"})
	if err := Save(original, path); err != nil {
		t.Fatal(err)
	}
	recovered, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Pairs["a"] != "1" || recovered.Pairs["b"] != "2" {
		t.Errorf("recovered pairs: %+v", recovered.Pairs)
	}
}

func TestSaveAtomicReplacesPrevious(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	if err := Save(MakeSnapshot(map[string]string{"a": "old"}), path); err != nil {
		t.Fatal(err)
	}
	if err := Save(MakeSnapshot(map[string]string{"a": "new"}), path); err != nil {
		t.Fatal(err)
	}
	r, _ := Load(path)
	if r.Pairs["a"] != "new" {
		t.Errorf("expected new, got %s", r.Pairs["a"])
	}
}

func TestLoadMissingReturnsError(t *testing.T) {
	_, err := Load("/tmp/does-not-exist-distkv.json")
	if err == nil {
		t.Errorf("expected error on missing file")
	}
}

func TestLoadEmptyFileReturnsEmptySnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(path, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Pairs) != 0 {
		t.Errorf("expected empty pairs")
	}
}

func TestMakeSnapshotIsCopy(t *testing.T) {
	src := map[string]string{"a": "1"}
	s := MakeSnapshot(src)
	src["a"] = "mutated"
	if s.Pairs["a"] != "1" {
		t.Errorf("snapshot leaked the source map")
	}
}
