package multishard_test

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/SAY-5/distributedkv/internal/multishard"
	"github.com/hashicorp/raft"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().(*net.TCPAddr)
	_ = l.Close()
	return addr.Port
}

func TestSingleNodeMultiShardSpawnAndKeyRouting(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	base := freePort(t)
	dir := filepath.Join(t.TempDir(), "node")
	cfg := multishard.Config{
		NodeID:     "n1",
		NumShards:  4,
		BasePort:   base,
		BindHost:   "127.0.0.1",
		AdvertHost: "127.0.0.1",
		DataDir:    dir,
		Bootstrap:  true,
		Peers: []raft.Server{
			{ID: "n1", Address: raft.ServerAddress(addr("127.0.0.1", base))},
		},
	}
	c, err := multishard.Spawn(cfg)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	t.Cleanup(c.Shutdown)
	if !c.WaitAllLeaders(15 * time.Second) {
		t.Fatal("not every shard elected a leader within deadline")
	}
	if c.NumShards() != 4 {
		t.Fatalf("expected 4 shards got %d", c.NumShards())
	}
	// Single-node bootstrap → this node is the leader on every shard.
	if got := len(c.LeaderShards()); got != 4 {
		t.Fatalf("expected 4 leader shards, got %d", got)
	}
}

func TestShardForIsDeterministicAndSpread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	base := freePort(t)
	dir := filepath.Join(t.TempDir(), "node")
	c, err := multishard.Spawn(multishard.Config{
		NodeID:     "n1",
		NumShards:  4,
		BasePort:   base,
		BindHost:   "127.0.0.1",
		AdvertHost: "127.0.0.1",
		DataDir:    dir,
		Bootstrap:  true,
		Peers: []raft.Server{
			{ID: "n1", Address: raft.ServerAddress(addr("127.0.0.1", base))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Shutdown)
	c.WaitAnyLeader(5 * time.Second)

	// Determinism: same key always lands on the same shard.
	for _, k := range []string{"a", "b", "users:42", "watched:k1"} {
		got := c.ShardFor(k)
		for i := 0; i < 5; i++ {
			if c.ShardFor(k) != got {
				t.Fatalf("ShardFor(%q) is not deterministic", k)
			}
		}
	}
	// Spread: with N=4 and a few hundred keys, every shard should
	// see at least some keys.
	count := map[int]int{}
	for i := 0; i < 400; i++ {
		count[c.ShardFor("k:" + itoa(i))]++
	}
	for shard := 0; shard < 4; shard++ {
		if count[shard] == 0 {
			t.Fatalf("shard %d got 0 keys (counts=%v)", shard, count)
		}
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	base := freePort(t)
	c, err := multishard.Spawn(multishard.Config{
		NodeID:     "n1",
		NumShards:  2,
		BasePort:   base,
		BindHost:   "127.0.0.1",
		AdvertHost: "127.0.0.1",
		DataDir:    filepath.Join(t.TempDir(), "node"),
		Bootstrap:  true,
		Peers: []raft.Server{
			{ID: "n1", Address: raft.ServerAddress(addr("127.0.0.1", base))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.WaitAnyLeader(5 * time.Second)
	c.Shutdown()
	c.Shutdown() // must not panic
	if c.NumShards() != 0 {
		t.Fatalf("expected 0 shards after Shutdown, got %d", c.NumShards())
	}
}

func addr(host string, port int) string {
	return host + ":" + itoa(port)
}

func itoa(i int) string {
	// stdlib's strconv would do, but this avoids the import in a test file.
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		i = -i
		neg = true
	}
	digs := []byte{}
	for i > 0 {
		digs = append([]byte{byte('0' + i%10)}, digs...)
		i /= 10
	}
	if neg {
		digs = append([]byte{'-'}, digs...)
	}
	return string(digs)
}
