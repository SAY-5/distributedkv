// singlenode is the unreplicated baseline. Same FSM, same HTTP API,
// no Raft. Used to measure the throughput cost of consensus (the
// "≥80% of single-node baseline under fault" claim in the README).
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/SAY-5/distributedkv/internal/fsm"
	"github.com/hashicorp/raft"
)

func main() {
	addr := flag.String("http", ":8000", "HTTP bind address")
	flag.Parse()

	kv := fsm.New()
	mu := &sync.Mutex{} // serializes Apply (FSM is mutex-protected internally; this is just to mirror the leader's single-writer)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"mode":"singlenode"}`))
	})
	mux.HandleFunc("/v1/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/v1/kv/"):]
		if key == "" {
			http.Error(w, "key required", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			v, ver, ok := kv.Get(key)
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-DKV-Version", strconv.FormatUint(ver, 10))
			_, _ = w.Write(v)
		case http.MethodPut:
			body, err := io.ReadAll(io.LimitReader(r.Body, 4*1024*1024))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			cmd := fsm.Command{Kind: fsm.OpPut, Key: key, Value: body, Issued: time.Now().UnixMilli()}
			data, _ := json.Marshal(cmd)
			mu.Lock()
			res := kv.Apply(&raft.Log{Data: data})
			mu.Unlock()
			r := res.(fsm.ApplyResult)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": r.OK, "version": r.NewVersion})
		case http.MethodDelete:
			cmd := fsm.Command{Kind: fsm.OpDelete, Key: key}
			data, _ := json.Marshal(cmd)
			mu.Lock()
			kv.Apply(&raft.Log{Data: data})
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}
	})

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("singlenode listening %s", *addr)
	log.Fatal(srv.ListenAndServe())
}
