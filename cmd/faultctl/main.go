// faultctl injects faults into a running cluster.
//
// Scenarios:
//
//	kill-leader    — finds the leader via /v1/cluster, SIGKILLs the OS process,
//	                 measures time until a new leader is elected.
//	slow-node      — adds an iptables-based latency pill (Linux-only) on a node.
//	disk-full      — fills the node's data dir with zeros until ENOSPC.
//
// Operates by talking to the HTTP API to discover topology; fault-injection
// itself is local-only (this is the harness, not a remote ops tool).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/SAY-5/distributedkv/internal/client"
)

func main() {
	ep := flag.String("ep", ":8001", "comma-separated endpoint list")
	scenario := flag.String("scenario", "kill-leader", "kill-leader | wait-leader")
	pidfile := flag.String("pidfile", "", "for kill-leader: path to file mapping node-id → pid")
	flag.Parse()

	c := client.New(strings.Split(*ep, ",")...)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch *scenario {
	case "kill-leader":
		killLeader(ctx, c, *pidfile)
	case "wait-leader":
		waitLeader(ctx, c)
	default:
		log.Fatalf("unknown scenario %q", *scenario)
	}
}

func killLeader(ctx context.Context, c *client.Client, pidfile string) {
	info, err := c.ClusterInfo(ctx)
	if err != nil {
		log.Fatalf("cluster info: %v", err)
	}
	leaderID := ""
	for _, s := range info["servers"].([]any) {
		m := s.(map[string]any)
		if m["is_leader"] == true {
			leaderID = m["id"].(string)
			break
		}
	}
	if leaderID == "" {
		log.Fatal("no leader visible")
	}
	pid, err := lookupPid(pidfile, leaderID)
	if err != nil {
		log.Fatalf("pid lookup: %v", err)
	}
	log.Printf("killing leader %s (pid %d)", leaderID, pid)
	t0 := time.Now()
	if err := exec.Command("kill", "-9", fmt.Sprint(pid)).Run(); err != nil {
		log.Fatalf("kill: %v", err)
	}

	// Wait for a new leader to appear (different from the killed one).
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
		info, err := c.ClusterInfo(ctx)
		if err != nil {
			continue
		}
		newLeader := ""
		for _, s := range info["servers"].([]any) {
			m := s.(map[string]any)
			if m["is_leader"] == true {
				newLeader = m["id"].(string)
				break
			}
		}
		if newLeader != "" && newLeader != leaderID {
			log.Printf("new leader %s elected in %v", newLeader, time.Since(t0))
			return
		}
	}
	log.Fatalf("no new leader within deadline")
}

func waitLeader(ctx context.Context, c *client.Client) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		info, err := c.ClusterInfo(ctx)
		if err != nil {
			continue
		}
		if info["leader"] != "" {
			log.Printf("leader=%v applied=%v keys=%v", info["leader"], info["applied"], info["keys"])
			return
		}
	}
	log.Fatal("no leader within deadline")
}

func lookupPid(pidfile, nodeID string) (int, error) {
	if pidfile == "" {
		return 0, fmt.Errorf("-pidfile required for kill-leader")
	}
	data, err := os.ReadFile(pidfile)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		if strings.TrimSpace(line[:eq]) == nodeID {
			var p int
			_, err := fmt.Sscanf(strings.TrimSpace(line[eq+1:]), "%d", &p)
			return p, err
		}
	}
	return 0, fmt.Errorf("node %q not found in %s", nodeID, pidfile)
}
