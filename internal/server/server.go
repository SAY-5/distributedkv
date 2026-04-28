// Package server is the HTTP/JSON data-plane.
//
// Endpoints:
//
//	GET    /v1/cluster                 — leader, peers, applied index
//	GET    /v1/healthz                 — liveness
//	PUT    /v1/kv/{key}                — write (request body is the value)
//	GET    /v1/kv/{key}                — linearizable read (default)
//	GET    /v1/kv/{key}?stale=1        — local follower read
//	DELETE /v1/kv/{key}                — delete
//	POST   /v1/kv/{key}/cas            — JSON {expected_version, value}
//	POST   /v1/cluster/join            — JSON {id, addr}
//	GET    /v1/stats                   — applied count, key count, p50/p95
//	GET    /metrics                    — Prometheus exposition
//
// Writes go through Raft. Followers HTTP-301 to the leader for
// PUT/DELETE/CAS so a client can blindly hit any node.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/SAY-5/distributedkv/internal/fsm"
	"github.com/SAY-5/distributedkv/internal/raftnode"
	"github.com/hashicorp/raft"
)

// Server bundles the Raft node and the FSM and exposes them as HTTP.
type Server struct {
	Node *raftnode.Node
	FSM  *fsm.KV
	// HTTPAddr is published in /v1/cluster — used by clients to dial
	// the leader directly (since Raft only knows raft addresses).
	HTTPAddr string
	// PeerHTTP is "node-id" → "host:port" for redirecting writes to the
	// leader. Populated at boot from the static peer list and updated
	// when nodes join.
	PeerHTTP map[string]string
	// WebDir is the path to the operator console assets. If empty, the
	// dashboard is not served.
	WebDir string

	mu        sync.Mutex
	latencies []float64 // ms; rolling window of last 1024 writes
	applyTO   time.Duration
}

// New wires a Server with sensible defaults.
func New(node *raftnode.Node, kv *fsm.KV, httpAddr string, peers map[string]string) *Server {
	s := &Server{
		Node: node, FSM: kv, HTTPAddr: httpAddr,
		PeerHTTP:  copyMap(peers),
		latencies: make([]float64, 0, 1024),
		applyTO:   3 * time.Second,
	}
	return s
}

// Routes returns an http.Handler with everything wired up. If WebDir
// is set on the Server, the operator console is served at "/".
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/healthz", s.handleHealth)
	mux.HandleFunc("/v1/cluster", s.handleCluster)
	mux.HandleFunc("/v1/cluster/join", s.handleJoin)
	mux.HandleFunc("/v1/kv/", s.handleKV)
	mux.HandleFunc("/v1/stats", s.handleStats)
	mux.HandleFunc("/metrics", s.handleMetrics)
	if s.WebDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(s.WebDir)))
	}
	return mux
}

// --- handlers ---------------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"node_id": string(s.Node.Raft.Stats()["state"]),
		"state":   s.Node.State(),
	})
}

func (s *Server) handleCluster(w http.ResponseWriter, _ *http.Request) {
	cfgFut := s.Node.Raft.GetConfiguration()
	if err := cfgFut.Error(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	servers := cfgFut.Configuration().Servers
	out := make([]map[string]any, 0, len(servers))
	for _, srv := range servers {
		out = append(out, map[string]any{
			"id":          string(srv.ID),
			"raft_addr":   string(srv.Address),
			"http_addr":   s.PeerHTTP[string(srv.ID)],
			"is_leader":   s.Node.Leader() == string(srv.Address),
			"suffrage":    srv.Suffrage.String(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"leader":  s.Node.Leader(),
		"state":   s.Node.State(),
		"applied": s.FSM.Version(),
		"keys":    s.FSM.Len(),
		"servers": out,
	})
}

type joinReq struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req joinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.ID == "" || req.RaftAddr == "" {
		writeErr(w, http.StatusBadRequest, errors.New("id and raft_addr required"))
		return
	}
	if s.Node.State() != "Leader" {
		s.redirectToLeader(w, r)
		return
	}
	if err := s.Node.AddVoter(req.ID, req.RaftAddr, 5*time.Second); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if req.HTTPAddr != "" {
		s.mu.Lock()
		s.PeerHTTP[req.ID] = req.HTTPAddr
		s.mu.Unlock()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/v1/kv/"):]
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.doRead(w, r, key)
	case http.MethodPut:
		s.doWrite(w, r, key, fsm.OpPut)
	case http.MethodDelete:
		s.doWrite(w, r, key, fsm.OpDelete)
	case http.MethodPost:
		// /v1/kv/{key}/cas (suffix)
		if len(key) > 4 && key[len(key)-4:] == "/cas" {
			s.doCAS(w, r, key[:len(key)-4])
			return
		}
		http.Error(w, "unsupported", http.StatusBadRequest)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) doRead(w http.ResponseWriter, r *http.Request, key string) {
	stale := r.URL.Query().Get("stale") == "1"
	if !stale {
		// Linearizable: barrier first so we've applied everything
		// committed up to now, *then* read.
		if err := s.Node.VerifyLeader(); err != nil {
			s.redirectToLeader(w, r)
			return
		}
		if err := s.Node.Barrier(2 * time.Second); err != nil {
			writeErr(w, http.StatusServiceUnavailable, err)
			return
		}
	}
	v, ver, ok := s.FSM.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"key": key, "found": false})
		return
	}
	w.Header().Set("X-DKV-Version", strconv.FormatUint(ver, 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(v)
}

func (s *Server) doWrite(w http.ResponseWriter, r *http.Request, key string, kind fsm.CommandKind) {
	if s.Node.State() != "Leader" {
		s.redirectToLeader(w, r)
		return
	}
	cmd := fsm.Command{Kind: kind, Key: key, Issued: time.Now().UnixMilli()}
	if kind == fsm.OpPut {
		body, err := io.ReadAll(io.LimitReader(r.Body, 4*1024*1024))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		cmd.Value = body
		if v := r.Header.Get("X-DKV-Expected-Version"); v != "" {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			cmd.ExpectedVersion = n
		}
		if v := r.Header.Get("X-DKV-TTL"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			cmd.TTLSec = n
		}
	}
	s.applyAndRespond(w, cmd)
}

type casReq struct {
	ExpectedVersion uint64 `json:"expected_version"`
	Value           string `json:"value"` // base64 or plain text — we treat it as bytes
}

func (s *Server) doCAS(w http.ResponseWriter, r *http.Request, key string) {
	if s.Node.State() != "Leader" {
		s.redirectToLeader(w, r)
		return
	}
	var req casReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cmd := fsm.Command{
		Kind:            fsm.OpCAS,
		Key:             key,
		Value:           []byte(req.Value),
		ExpectedVersion: req.ExpectedVersion,
		Issued:          time.Now().UnixMilli(),
	}
	s.applyAndRespond(w, cmd)
}

func (s *Server) applyAndRespond(w http.ResponseWriter, cmd fsm.Command) {
	data, err := json.Marshal(cmd)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	t0 := time.Now()
	resp, err := s.Node.Apply(data, s.applyTO)
	elapsed := float64(time.Since(t0).Microseconds()) / 1000.0
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	r, _ := resp.(fsm.ApplyResult)
	s.recordLatency(elapsed)
	if !r.OK {
		w.Header().Set("X-DKV-Conflict-Version", strconv.FormatUint(r.ConflictVersion, 10))
		writeJSON(w, http.StatusConflict, map[string]any{
			"ok": false, "err": r.Err, "conflict_version": r.ConflictVersion,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"version":     r.NewVersion,
		"elapsed_ms":  elapsed,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	cp := append([]float64(nil), s.latencies...)
	s.mu.Unlock()
	stats := map[string]any{
		"keys":    s.FSM.Len(),
		"applied": s.FSM.Version(),
		"state":   s.Node.State(),
		"leader":  s.Node.Leader(),
		"writes":  len(cp),
	}
	if len(cp) > 0 {
		sort.Float64s(cp)
		stats["p50_ms"] = cp[len(cp)/2]
		stats["p95_ms"] = cp[(len(cp)*95)/100]
		stats["p99_ms"] = cp[(len(cp)*99)/100]
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	// Hand-rolled Prometheus exposition. No external dep.
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	rs := s.Node.Raft.Stats()
	fmt.Fprintf(w, "# HELP dkv_keys live key count\n# TYPE dkv_keys gauge\ndkv_keys %d\n", s.FSM.Len())
	fmt.Fprintf(w, "# HELP dkv_applied_index FSM apply counter\n# TYPE dkv_applied_index counter\ndkv_applied_index %d\n", s.FSM.Version())
	fmt.Fprintf(w, "# HELP dkv_raft_state 1 if leader, else 0\n# TYPE dkv_raft_state gauge\ndkv_raft_state %d\n", boolToInt(s.Node.State() == "Leader"))
	for k, v := range rs {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			fmt.Fprintf(w, "dkv_raft_%s %g\n", sanitize(k), n)
		}
	}
}

func (s *Server) recordLatency(ms float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latencies = append(s.latencies, ms)
	if len(s.latencies) > 1024 {
		s.latencies = s.latencies[len(s.latencies)-1024:]
	}
}

func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leaderRaft := s.Node.Leader()
	if leaderRaft == "" {
		writeErr(w, http.StatusServiceUnavailable, errors.New("no leader"))
		return
	}
	cfg := s.Node.Raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	var leaderID string
	for _, srv := range cfg.Configuration().Servers {
		if string(srv.Address) == leaderRaft {
			leaderID = string(srv.ID)
			break
		}
	}
	s.mu.Lock()
	leaderHTTP := s.PeerHTTP[leaderID]
	s.mu.Unlock()
	if leaderHTTP == "" {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("leader %s http addr unknown", leaderID))
		return
	}
	w.Header().Set("Location", "http://"+leaderHTTP+r.URL.RequestURI())
	w.Header().Set("X-DKV-Leader", leaderID)
	w.WriteHeader(http.StatusTemporaryRedirect)
}

// --- helpers ----------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"ok": false, "err": err.Error()})
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Suppress unused-import warnings for tags we may add later.
var _ = raft.ServerID("")
var _ = context.TODO
