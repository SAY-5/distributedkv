package raftnode_test

// Integration tests that spin up real Raft clusters in-process using
// loopback TCP. Each test creates fresh data directories and free
// ports; teardown is best-effort.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SAY-5/distributedkv/internal/client"
	"github.com/SAY-5/distributedkv/internal/fsm"
	"github.com/SAY-5/distributedkv/internal/raftnode"
	"github.com/SAY-5/distributedkv/internal/server"
	"github.com/hashicorp/raft"
)

type testNode struct {
	id        string
	raftAddr  string
	httpAddr  string
	dataDir   string
	node      *raftnode.Node
	kv        *fsm.KV
	srv       *server.Server
	httpSrv   *http.Server
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func startNode(t *testing.T, id string, peers []raft.Server, httpPeers map[string]string, bootstrap bool) *testNode {
	t.Helper()
	raftAddr := ""
	for _, p := range peers {
		if string(p.ID) == id {
			raftAddr = string(p.Address)
		}
	}
	if raftAddr == "" {
		t.Fatalf("peer %q not in peer list", id)
	}
	httpAddr := httpPeers[id]
	dir := t.TempDir()
	kv := fsm.New()
	cfg := raftnode.Config{
		NodeID:    id,
		BindAddr:  raftAddr,
		DataDir:   filepath.Join(dir, id),
		Bootstrap: bootstrap,
		Peers:     peers,
		FSM:       kv,
	}
	node, err := raftnode.New(cfg)
	if err != nil {
		t.Fatalf("raft.New[%s]: %v", id, err)
	}
	srv := server.New(node, kv, httpAddr, httpPeers)
	httpSrv := &http.Server{Addr: httpAddr, Handler: srv.Routes(), ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = httpSrv.ListenAndServe() }()
	// Wait for the HTTP listener to be reachable.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", httpAddr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	tn := &testNode{
		id: id, raftAddr: raftAddr, httpAddr: httpAddr,
		dataDir: cfg.DataDir, node: node, kv: kv, srv: srv, httpSrv: httpSrv,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
		_ = node.Shutdown()
	})
	return tn
}

func waitForLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.node.State() == "Leader" {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader within %v", timeout)
	return nil
}

func TestThreeNodeClusterReplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in -short mode")
	}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	httpAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	peers := []raft.Server{
		{ID: "n1", Address: raft.ServerAddress(addrs[0])},
		{ID: "n2", Address: raft.ServerAddress(addrs[1])},
		{ID: "n3", Address: raft.ServerAddress(addrs[2])},
	}
	httpPeers := map[string]string{"n1": httpAddrs[0], "n2": httpAddrs[1], "n3": httpAddrs[2]}

	n1 := startNode(t, "n1", peers, httpPeers, true)
	n2 := startNode(t, "n2", peers, httpPeers, false)
	n3 := startNode(t, "n3", peers, httpPeers, false)
	nodes := []*testNode{n1, n2, n3}

	leader := waitForLeader(t, nodes, 8*time.Second)
	t.Logf("leader: %s @ %s", leader.id, leader.raftAddr)

	c := client.New(httpAddrs...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write one key.
	ver, err := c.Put(ctx, "hello", []byte("world"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if ver != 1 {
		t.Fatalf("expected version 1, got %d", ver)
	}

	// Read it back linearizably.
	v, gotVer, err := c.Get(ctx, "hello", false)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "world" || gotVer != 1 {
		t.Fatalf("got v=%q ver=%d", v, gotVer)
	}

	// Confirm replication: every node's FSM should converge.
	deadline := time.Now().Add(3 * time.Second)
	for {
		ok := true
		for _, n := range nodes {
			vv, _, found := n.kv.Get("hello")
			if !found || string(vv) != "world" {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("FSM did not converge across replicas")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestWritesSurviveLeaderLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in -short mode")
	}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	httpAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	peers := []raft.Server{
		{ID: "a", Address: raft.ServerAddress(addrs[0])},
		{ID: "b", Address: raft.ServerAddress(addrs[1])},
		{ID: "c", Address: raft.ServerAddress(addrs[2])},
	}
	httpPeers := map[string]string{"a": httpAddrs[0], "b": httpAddrs[1], "c": httpAddrs[2]}

	a := startNode(t, "a", peers, httpPeers, true)
	b := startNode(t, "b", peers, httpPeers, false)
	c := startNode(t, "c", peers, httpPeers, false)
	nodes := []*testNode{a, b, c}

	leader := waitForLeader(t, nodes, 8*time.Second)
	cl := client.New(httpAddrs...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%d", i)
		if _, err := cl.Put(ctx, key, []byte("v")); err != nil {
			t.Fatalf("pre-kill put: %v", err)
		}
	}

	// Shut down the current leader.
	t.Logf("shutting down leader %s", leader.id)
	ctxSD, cancelSD := context.WithTimeout(context.Background(), 2*time.Second)
	_ = leader.httpSrv.Shutdown(ctxSD)
	cancelSD()
	if err := leader.node.Shutdown(); err != nil {
		t.Logf("shutdown err (ok if already closed): %v", err)
	}

	// Survivors must elect a new leader.
	survivors := []*testNode{}
	survivorEPs := []string{}
	for _, n := range nodes {
		if n.id != leader.id {
			survivors = append(survivors, n)
			survivorEPs = append(survivorEPs, n.httpAddr)
		}
	}
	newLeader := waitForLeader(t, survivors, 12*time.Second)
	if newLeader.id == leader.id {
		t.Fatal("'new' leader is the dead one — election didn't happen")
	}
	t.Logf("new leader: %s", newLeader.id)

	// Writes must keep succeeding through the survivor endpoints.
	cl2 := client.New(survivorEPs...)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if _, err := cl2.Put(ctx2, "after-kill", []byte("ok")); err != nil {
		t.Fatalf("post-kill put: %v", err)
	}
	v, _, err := cl2.Get(ctx2, "after-kill", false)
	if err != nil || string(v) != "ok" {
		t.Fatalf("post-kill get: v=%q err=%v", v, err)
	}
}

func TestStaleReadFromFollower(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in -short mode")
	}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	httpAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	peers := []raft.Server{
		{ID: "a", Address: raft.ServerAddress(addrs[0])},
		{ID: "b", Address: raft.ServerAddress(addrs[1])},
		{ID: "c", Address: raft.ServerAddress(addrs[2])},
	}
	httpPeers := map[string]string{"a": httpAddrs[0], "b": httpAddrs[1], "c": httpAddrs[2]}

	a := startNode(t, "a", peers, httpPeers, true)
	b := startNode(t, "b", peers, httpPeers, false)
	c := startNode(t, "c", peers, httpPeers, false)
	nodes := []*testNode{a, b, c}
	_ = waitForLeader(t, nodes, 8*time.Second)

	cl := client.New(httpAddrs...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cl.Put(ctx, "x", []byte("y")); err != nil {
		t.Fatal(err)
	}

	// Wait for replication, then issue a stale read against every node.
	time.Sleep(300 * time.Millisecond)
	for _, n := range nodes {
		c2 := client.New(n.httpAddr)
		v, _, err := c2.Get(ctx, "x", true)
		if err != nil {
			t.Fatalf("stale get on %s: %v", n.id, err)
		}
		if string(v) != "y" {
			t.Fatalf("stale get on %s: got %q", n.id, v)
		}
	}
}

func TestClusterInfoExposesTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in -short mode")
	}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	httpAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	peers := []raft.Server{
		{ID: "a", Address: raft.ServerAddress(addrs[0])},
		{ID: "b", Address: raft.ServerAddress(addrs[1])},
		{ID: "c", Address: raft.ServerAddress(addrs[2])},
	}
	httpPeers := map[string]string{"a": httpAddrs[0], "b": httpAddrs[1], "c": httpAddrs[2]}
	a := startNode(t, "a", peers, httpPeers, true)
	b := startNode(t, "b", peers, httpPeers, false)
	c := startNode(t, "c", peers, httpPeers, false)
	_ = waitForLeader(t, []*testNode{a, b, c}, 8*time.Second)

	resp, err := http.Get("http://" + httpAddrs[0] + "/v1/cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var info map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	servers, ok := info["servers"].([]any)
	if !ok || len(servers) != 3 {
		t.Fatalf("expected 3 servers, got %v", info["servers"])
	}
	leaders := 0
	for _, s := range servers {
		if s.(map[string]any)["is_leader"] == true {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly one leader, got %d", leaders)
	}
	if !strings.Contains(info["state"].(string), "Leader") &&
		!strings.Contains(info["state"].(string), "Follower") {
		t.Fatalf("unexpected state: %v", info["state"])
	}
}
