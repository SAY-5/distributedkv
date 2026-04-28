// loadgen drives synthetic PUT/GET traffic against the cluster and
// reports throughput + per-percentile latency. Pair with cmd/faultctl
// to measure write-throughput-under-fault.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SAY-5/distributedkv/internal/client"
)

func main() {
	ep := flag.String("ep", ":8001", "comma-separated endpoint list")
	dur := flag.Duration("d", 10*time.Second, "duration")
	conc := flag.Int("c", 16, "concurrency")
	rwRatio := flag.Float64("rw", 0.5, "fraction of ops that are writes (0..1)")
	keyspace := flag.Int("k", 10_000, "keyspace size")
	valSize := flag.Int("v", 256, "value size in bytes")
	rate := flag.Int("rate", 0, "target ops/sec total (0 = unlimited)")
	flag.Parse()

	c := client.New(strings.Split(*ep, ",")...)
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	value := make([]byte, *valSize)
	for i := range value {
		value[i] = byte('A' + (i % 26))
	}

	var (
		ops, errs   atomic.Uint64
		latsMu      sync.Mutex
		lats        = make([]float64, 0, 1<<20)
	)
	var (
		ticker *time.Ticker
	)
	if *rate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
	}

	t0 := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				if ticker != nil {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
				} else if ctx.Err() != nil {
					return
				}
				key := fmt.Sprintf("k%07d", r.Intn(*keyspace))
				start := time.Now()
				var err error
				if r.Float64() < *rwRatio {
					_, err = c.Put(ctx, key, value)
				} else {
					_, _, err = c.Get(ctx, key, false)
					if err == client.ErrNotFound {
						err = nil
					}
				}
				ms := float64(time.Since(start).Microseconds()) / 1000.0
				if err != nil {
					errs.Add(1)
					continue
				}
				ops.Add(1)
				latsMu.Lock()
				lats = append(lats, ms)
				latsMu.Unlock()
			}
		}(int64(i + 1))
	}
	wg.Wait()
	elapsed := time.Since(t0).Seconds()

	sort.Float64s(lats)
	p := func(q float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		idx := int(q * float64(len(lats)-1))
		return lats[idx]
	}
	log.Printf("loadgen: ops=%d errs=%d duration=%.2fs throughput=%.0f ops/s",
		ops.Load(), errs.Load(), elapsed, float64(ops.Load())/elapsed)
	log.Printf("latency  p50=%.2fms  p95=%.2fms  p99=%.2fms  max=%.2fms",
		p(0.5), p(0.95), p(0.99), p(1.0))
}
