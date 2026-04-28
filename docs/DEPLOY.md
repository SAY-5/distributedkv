# DistributedKV — Deployment Guide

Single binary `kvd` per node, Raft over TCP, HTTP/JSON for the data plane.
Stateful: each node owns a data directory containing the Raft log
(`log.bolt`), the stable store (`stable.bolt`), and snapshot files.

## Quickstart (3-node cluster on docker compose)

```bash
docker compose up -d --build
sleep 3
docker compose exec n1 kvctl -ep n1:8001 info
docker compose exec n1 kvctl -ep n1:8001,n2:8001,n3:8001 put hello world
docker compose exec n1 kvctl -ep n2:8001 get hello   # served via leader-redirect
```

The operator console is at `http://localhost:8001/` (n1), `:8002`, `:8003`.

## Single-node baseline

For benchmarking the cost of consensus:

```bash
docker compose run --rm n1 singlenode -http :9000   # bypasses raft
```

The bench harness compares loadgen results against this baseline; the
README's "≥80% of single-node baseline under fault injection" claim is
the metric of record.

## Configuration

| flag           | default       | meaning |
|----------------|---------------|---------|
| `-id`          | (required)    | unique node ID within the cluster |
| `-raft`        | `:7001`       | TCP bind for raft transport |
| `-advert`      | (= `-raft`)   | address peers should reach us at |
| `-http`        | `:8001`       | HTTP API + console |
| `-data`        | `./data`      | log/stable/snapshot working dir |
| `-bootstrap`   | `false`       | first-boot of a fresh cluster |
| `-peers`       | (empty)       | `id=raft_addr,...` initial voter set (bootstrap only) |
| `-http-peers`  | (empty)       | `id=http_addr,...` for leader-redirect |
| `-web`         | (empty)       | path to operator console assets; omit to disable UI |

## Production checklist

- **Five voters per Raft group.** A 3-node demo tolerates 1 failure;
  for "always tolerate 2" run 5. Cluster sizes >7 are usually
  counterproductive — the commit fan-out becomes the bottleneck.
- **Persistent volumes.** Mount `/srv/data` on a real disk with fsync
  honored. The Raft log is fsync-on-Append; lying about durability
  here is how you get a split-brain on power loss.
- **Pre-allocate disk.** Boltdb grows as needed but doesn't shrink;
  budget 1-2× the live keyspace for the log + 1× for snapshots.
- **Snapshot retention.** We keep 3 snapshots × ~live-set-size on disk.
  Bump `SnapshotInterval` if writes are very heavy.
- **Backup.** A consistent backup is `kvctl ... info` to find the
  applied index, then a Raft log + snapshot directory copy. Restoring
  is "stop the cluster, copy data dir to a fresh node, bootstrap it
  alone, re-add the others".
- **Observability.** Every node serves `/metrics` (Prometheus
  exposition) and `/v1/stats` (JSON p50/p95/p99). The operator console
  reads both at 1.5 s intervals.

## Operational runbook

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Writes return 503 "no leader" | Quorum lost (n/2+1 nodes down) | Bring more nodes online; if data is salvageable, see "single-node restart" below |
| Writes return 307 to a dead node | Stale peer-HTTP map | Restart the redirecting node (it'll re-resolve from raft config) |
| `kvctl put` hangs > 5s | Apply timeout exceeded; either disk slow or quorum issue | Check `/metrics` `dkv_raft_lastContact_*` on each node |
| One node lags far behind | Slow disk; snapshot-install in progress | Watch `applied_index` in /v1/cluster — should catch up within a snapshot cycle |
| Disk fills up | Logs not truncating, or huge keyspace | Raise `SnapshotThreshold` / lower `SnapshotInterval`; consider splitting into more shards |

### Killing the leader (chaos test)

```bash
# Discover leader
kvctl -ep :8001 info | jq -r '.servers[] | select(.is_leader).id'
# Kill it (this is the harness, not a real ops tool)
docker compose kill -s KILL n1
# Confirm a new leader elected within ~1s
sleep 2 && kvctl -ep :8002 info
# Writes should resume immediately
kvctl -ep :8002 put after-kill ok
```

### Adding a node

```bash
# On the new node:
kvd -id n4 -raft :7001 -http :8001 -data /srv/data \
    -peers 'n1=n1:7001,n2=n2:7001,n3=n3:7001'

# On any cluster node:
curl -X POST -H 'Content-Type: application/json' \
     -d '{"id":"n4","raft_addr":"n4:7001","http_addr":"n4:8001"}' \
     http://n1:8001/v1/cluster/join
```

The leader appends a configuration entry, hashicorp/raft does the
joint-consensus dance, and the new node receives a snapshot via
InstallSnapshot. After ~1-3 s the cluster has 4 voters.

## Scaling notes

A single Raft group writes through one leader: total throughput is
bounded by that leader's disk + network. To scale linearly with cluster
size, partition the keyspace across multiple Raft groups (the
`hashring` package is the routing layer). Each shard runs its own
group; each node hosts one replica per shard. The architecture doc
covers the layout in `ARCHITECTURE.md § Routing layer`.

For >100 KRPS sustained writes, plan on 8-16 shards. The vnode ring
keeps load even within ±10% across nodes.
