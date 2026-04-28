# DistributedKV Architecture

## Overview

DistributedKV is a sharded, replicated, linearizable key-value store.
Keys are routed to one of N shards via consistent hashing; each shard
is a Raft replication group of 5 nodes; reads are linearizable via
ReadIndex; writes commit through Raft. The system is designed to
sustain 1M records and to maintain availability under one-replica-
per-shard fault injection.

Stack:

| Piece          | Tech                                                       |
|----------------|------------------------------------------------------------|
| Language       | Go 1.23                                                    |
| Consensus      | `github.com/hashicorp/raft` (proven, battle-tested)        |
| Raft transport | `github.com/hashicorp/raft-grpc-transport`                 |
| Log store      | `boltdb` (embedded) for Raft log + stable store            |
| FSM (state)    | In-memory map snapshotted to disk; gzipped snapshots       |
| Wire protocol  | gRPC (protobuf-defined); HTTP gateway optional             |
| Routing        | Consistent hashing (Jump Hash for the shard step,         |
|                |  vnode-ring for the Raft-group selection)                  |
| Ops            | Docker · docker-compose (5-node demo) · Kubernetes manifests |

## Data plane

```
                      ┌──────────────────────────────────────┐
                      │          Client (kvctl / SDK)         │
                      └───────────────┬──────────────────────┘
                                      │ gRPC PUT/GET/DELETE
                                      ▼
                      ┌──────────────────────────────────────┐
                      │        Any node in the cluster       │
                      │  (request router; talks to shard     │
                      │   leader if not local)               │
                      └─────┬───────────────┬───────────────┘
                            │               │ forward via gRPC
                            ▼               ▼
            ┌──────────────────────┐    ┌──────────────────────┐
            │   shard-0 leader     │    │   shard-N leader     │
            │   ┌────┐ ┌────┐ ┌────┐│    │   ┌────┐ ┌────┐ ┌────┐│
            │   │ R0 │ │ R1 │ │ R2 ││    │   │ R0 │ │ R1 │ │ R2 ││
            │   └────┘ └────┘ └────┘│    │   └────┘ └────┘ └────┘│
            │  Raft replication    │    │  Raft replication    │
            └──────────────────────┘    └──────────────────────┘
```

A 5-node demo cluster has 5 physical nodes and (by default) 4 shards,
with each shard's 3 voting replicas spread across the 5 nodes via the
shard router. (The 5/3 ratio is deliberate: a fault test that kills
one node per shard still leaves 2 voting replicas, a quorum.)

### Routing layer

- **Shard step (key → shard ID)**: Google's Jump Consistent Hash
  (`hash/maphash` for the key digest, then jump). Doesn't require a
  preallocated ring; rebalances move ~1/N of keys when N changes.
- **Replica placement (shard ID → 3 nodes)**: vnode-style ring with
  N=128 vnodes per node, hashed by `(shard_id, vnode_idx)` and
  ordered. Pick the first 3 distinct nodes from the sorted hash. The
  ring is stable as long as the membership list doesn't change;
  membership changes go through Raft on a meta-shard (shard 0 by
  convention).

### Linearizable reads

Naïve "ask the leader" reads aren't linearizable on their own — a
freshly-elected leader might not have applied all committed entries
yet. We use Raft's **ReadIndex** protocol via hashicorp/raft's
`Verify Leader` + `Apply` round trip: the leader notes its current
commit index, confirms with a quorum it's still leader, waits for
its FSM to apply up to that index, and returns the read. This is
linearizable.

Stale reads (`STALE_OK` flag set in the request) skip ReadIndex and
serve from the local follower's state — useful for dashboards.

## FSM (in-memory map)

Each shard owns a Go `map[string][]byte` plus a small metadata struct
(version, last_modified). Snapshots are gzipped JSON written to
`<data>/snapshots/snap-<index>.gz`. Restoration replays the snapshot
plus any Raft-log entries since.

The FSM applies four commands (defined as protobuf):

```
PutCmd       { key, value, expected_version (optional, for CAS) }
DeleteCmd    { key, expected_version (optional, for CAS) }
TouchCmd     { key, ttl_seconds }            // for TTL housekeeping
SnapshotMark { snapshot_id }                 // checkpointing
```

Versions are monotone per key; CAS rejects with `version_mismatch` if
the supplied expected version is stale.

## gRPC API

```protobuf
service KV {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Cas(CasRequest) returns (CasResponse);
  rpc Watch(WatchRequest) returns (stream WatchEvent);   // long-poll
  rpc ClusterInfo(Empty) returns (ClusterInfoResponse);
}
```

`Watch` streams events for a key prefix; implementation is a
fan-out from the FSM-apply hook to all live watch subscriptions.

## Shard rebalance (membership change)

Adding/removing a node:
1. The operator calls `kvctl member add <node-id> <addr>`.
2. The meta-shard's leader appends a `MembershipChange` Raft entry.
3. After commit, every shard recomputes the vnode ring with the new
   membership and identifies which shards now have the wrong replica
   set.
4. Affected shards initiate a Raft membership change to add the new
   replica + remove the old one. Hashicorp/raft already handles
   the joint-consensus step.
5. Snapshot transfer to the new replica happens via Raft's
   `InstallSnapshot` RPC.

This is conservative, slow, and correct. We don't try to be fancy.

## Fault injection / chaos harness

A separate Go program (`cmd/faultctl`) drives the cluster through
scripted scenarios:

- **Kill leader**: SIGKILL the current leader of shard X; assert
  client requests succeed within < 5s after a new leader's election.
- **Network partition**: iptables / pumba-style packet drop between
  two nodes; assert the cluster maintains majority and serves writes.
- **Disk full**: fill the data dir on one node; assert the node
  voluntarily steps down and the cluster moves on.
- **Slow replica**: inject 200ms latency on one node's gRPC; assert
  the cluster's p99 latency stays bounded.

The harness is the load test too. It runs a workload generator
(`cmd/loadgen`) producing random PUTs/GETs at a target rate, while
chaos events fire on a schedule. The metric of record is **write
throughput under fault** — the README's "80% above single-node
baseline" claim is what the harness asserts.

## Storage layout

```
/data/
  raft/
    log.bolt              ← Raft log (boltdb)
    stable.bolt           ← stable store
    snapshots/
      snap-12345.gz
  state/                  ← FSM checkpoints (rebuildable from snapshots)
```

Log is fsync'd on every Raft Append; the durability cost is
unavoidable for safety. Group commits batch multiple Apply calls.

## Configuration

```
DKV_NODE_ID                  unique within the cluster
DKV_DATA_DIR                 /data
DKV_GRPC_ADDR                :7000
DKV_RAFT_ADDR                :7001
DKV_PEERS                    "n1=10.0.0.1:7001,n2=10.0.0.2:7001,..."
DKV_SHARDS                   number of shards (default 16)
DKV_REPLICATION_FACTOR       per-shard replicas (default 3)
DKV_BOOTSTRAP                true on the first node only at cluster init
```

## Performance targets

- p50 PUT < 5ms, p99 < 30ms (5-node cluster, all healthy)
- p50 GET (linearizable) < 5ms (ReadIndex is local + 1 quorum confirm)
- Write throughput: aim for ≥ 80% of single-node baseline on the same
  hardware. Single-node baseline is measured by a separate
  `cmd/single-node` build that bypasses Raft and writes directly to
  the in-memory map. The bench harness reports the ratio.
- Sustains 1M records with steady-state memory < 1 GB per node.

## Testing

- **Unit**: consistent-hash determinism, jump-hash known vectors,
  FSM apply/snapshot/restore round trip, gRPC handler validation.
- **Integration**: 5-node single-process cluster (each node a
  goroutine + tcp listener on a unique port); writes survive a
  leader kill and a full restart.
- **Property**: Jepsen-lite — generator-driven random
  PUT/GET/CAS/DELETE workload under random faults, asserts
  linearizability via a per-key history check.
- **Bench**: `go test -bench` + `cmd/loadgen` for end-to-end
  throughput numbers.

## Non-goals

- **Cross-shard transactions**: not supported. Transactions span a
  single shard's Raft group only.
- **Strong consistency on reads from stale replicas**: by design the
  `STALE_OK` flag opts out of linearizability.
- **Geo-replication**: out of scope — Raft across WAN is fragile.
  Real deployments would layer a separate async replication system
  on top.
