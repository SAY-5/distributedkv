// kvd is the DistributedKV node daemon.
//
// Run a 3-node cluster locally:
//
//	kvd -id n1 -raft :7001 -http :8001 -data /tmp/d1 -bootstrap -peers 'n1=:7001,n2=:7002,n3=:7003'
//	kvd -id n2 -raft :7002 -http :8002 -data /tmp/d2 -peers 'n1=:7001,n2=:7002,n3=:7003'
//	kvd -id n3 -raft :7003 -http :8003 -data /tmp/d3 -peers 'n1=:7001,n2=:7002,n3=:7003'
//
// On bootstrap, the leader's peer list is the entire initial set.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/SAY-5/distributedkv/internal/fsm"
	"github.com/SAY-5/distributedkv/internal/raftnode"
	"github.com/SAY-5/distributedkv/internal/server"
	"github.com/hashicorp/raft"
)

func main() {
	id := flag.String("id", "", "unique node id (required)")
	raftAddr := flag.String("raft", ":7001", "raft transport bind address")
	advert := flag.String("advert", "", "raft advertise address (defaults to -raft)")
	httpAddr := flag.String("http", ":8001", "HTTP API bind address")
	dataDir := flag.String("data", "./data", "raft + fsm working dir")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap a new cluster")
	peerSpec := flag.String("peers", "", "comma-separated id=addr peer list (raft addrs); used at bootstrap")
	httpPeerSpec := flag.String("http-peers", "", "comma-separated id=addr peer list (HTTP addrs); for leader-redirect")
	webDir := flag.String("web", "", "path to the operator console assets (./web for dev)")
	flag.Parse()

	if *id == "" {
		log.Fatal("-id is required")
	}

	kv := fsm.New()
	peers := parsePeers(*peerSpec)
	httpPeers := parsePeerMap(*httpPeerSpec)
	if httpPeers[*id] == "" {
		httpPeers[*id] = *httpAddr
	}

	cfg := raftnode.Config{
		NodeID:     *id,
		BindAddr:   *raftAddr,
		AdvertAddr: *advert,
		DataDir:    *dataDir,
		Bootstrap:  *bootstrap,
		Peers:      peers,
		FSM:        kv,
	}
	node, err := raftnode.New(cfg)
	if err != nil {
		log.Fatalf("raftnode.New: %v", err)
	}

	srv := server.New(node, kv, *httpAddr, httpPeers)
	srv.WebDir = *webDir
	httpSrv := &http.Server{
		Addr:              *httpAddr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("kvd %s listening http=%s raft=%s bootstrap=%v",
			*id, *httpAddr, *raftAddr, *bootstrap)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	// Wait for SIGTERM/SIGINT.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Printf("kvd %s shutting down", *id)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	if err := node.Shutdown(); err != nil {
		log.Printf("raft shutdown: %v", err)
	}
}

func parsePeers(spec string) []raft.Server {
	if spec == "" {
		return nil
	}
	out := []raft.Server{}
	for _, p := range strings.Split(spec, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.IndexByte(p, '=')
		if eq < 0 {
			log.Fatalf("invalid peer spec %q (want id=addr)", p)
		}
		id, addr := p[:eq], p[eq+1:]
		out = append(out, raft.Server{
			ID:      raft.ServerID(id),
			Address: raft.ServerAddress(addr),
		})
	}
	return out
}

func parsePeerMap(spec string) map[string]string {
	out := map[string]string{}
	if spec == "" {
		return out
	}
	for _, p := range strings.Split(spec, ",") {
		eq := strings.IndexByte(p, '=')
		if eq < 0 {
			continue
		}
		out[strings.TrimSpace(p[:eq])] = strings.TrimSpace(p[eq+1:])
	}
	return out
}
