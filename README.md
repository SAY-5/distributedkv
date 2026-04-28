# DistributedKV

A fault-tolerant, replicated key-value store. Three (or more) nodes,
one Raft group, linearizable reads, snapshot-based recovery, and a
distinctive SRE-terminal operator console.

```
   ┌──────── kvctl / SDK ────────┐
   │                             │
   ▼                             ▼
   ┌──────┐    raft     ┌──────┐    raft     ┌──────┐
   │  n1  │◀──────────▶│  n2  │◀──────────▶│  n3  │
   │  ★   │            │      │            │      │
   └──────┘            └──────┘            └──────┘
   (leader)            (follower)          (follower)
```

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full design
(routing, consensus, FSM, snapshots, perf targets).
See [`docs/DEPLOY.md`](./docs/DEPLOY.md) for the operator runbook.

## Quick start

```bash
docker compose up -d --build
sleep 3
docker compose exec n1 kvctl -ep n1:8001 info | jq
docker compose exec n1 kvctl -ep n1:8001,n2:8001,n3:8001 put hello world
docker compose exec n1 kvctl -ep n2:8001 get hello   # leader-redirect handled
```

Open `http://localhost:8001/` for the SRE-terminal operator console.

## Highlights

- **Battle-tested consensus.** `hashicorp/raft` + `raft-boltdb/v2`. We
  don't reinvent the elephant.
- **Linearizable reads via ReadIndex.** `VerifyLeader` + `Barrier`
  before serving GETs, so freshly-elected leaders don't return stale
  data. `?stale=1` opts into fast follower reads.
- **CAS with monotone versions.** Each key carries a `version`;
  optimistic concurrency works as expected.
- **Routing layer ready for shards.** Jump consistent hash for
  key→shard, vnode ring for shard→nodes. The architecture is single-
  shard today; the multi-shard router is in `internal/hashring` and
  unit-tested for load-balance and minimal disruption on membership
  changes.
- **Single-node baseline binary.** `cmd/singlenode` runs the same FSM
  with no Raft, for measuring the cost of consensus.
- **Chaos harness.** `cmd/faultctl` kills the leader and asserts a new
  one is elected within the deadline; `cmd/loadgen` is the same
  workload the bench harness uses.

## Tests

```
internal/hashring     ↔ jump-hash known vectors, vnode ring load
                        balance, minimal disruption on membership
internal/fsm          ↔ put/get, version monotone, CAS conflict, TTL
                        expiry, snapshot+restore round-trip, gzip
                        wire format
internal/raftnode     ↔ 3-node cluster spin-up, replication
                        convergence, leader-loss + new-leader
                        election, stale-follower reads, /v1/cluster
                        topology
```

```bash
go test -timeout 120s ./...
```

## Companion projects

Part of an eight-repo set:

- **[canvaslive](https://github.com/SAY-5/canvaslive)** — multiplayer OT whiteboard
- **[pluginforge](https://github.com/SAY-5/pluginforge)** — Web Worker plugin sandbox
- **[agentlab](https://github.com/SAY-5/agentlab)** — AI agent eval harness
- **[payflow](https://github.com/SAY-5/payflow)** — payments API
- **[queryflow](https://github.com/SAY-5/queryflow)** — natural-language SQL
- **[datachat](https://github.com/SAY-5/datachat)** — AI data analysis with sandboxed Python
- **distributedkv** — you're here. Sharded, replicated KV.
- **[inferencegateway](https://github.com/SAY-5/inferencegateway)** — high-throughput LLM serving frontend

## License

MIT.
