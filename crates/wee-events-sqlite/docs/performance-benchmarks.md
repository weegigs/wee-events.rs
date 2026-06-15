# EventStore Performance Benchmarks

Benchmark results for the `wee-events` event store across all backends and
partition strategies. Results were collected using [criterion.rs](https://github.com/criterion-rs/criterion.rs)
with `--quick` mode on a local development machine (2026-03-31).

## Test Environment

- **Backends:** MemoryStore, in-memory SQLite, local filesystem SQLite, sqld (Docker via testcontainers)
- **Partition strategies:** Global, Type, Aggregate, Hashed(8), PartitionBy
- **Concurrency levels:** 2, 4, 8, 16, 32
- **Event payload:** `StoreValidationEvent` (small JSON, ~50 bytes serialized)

Run benchmarks with:

```sh
cargo bench -p wee-events --bench memory_store
cargo bench -p wee-events-sqlite --bench sqlite_store
```

## 1. Aggregate Creation (First Write)

The first publish to a new aggregate includes partition provisioning cost — creating
database files, tables, or remote namespaces.

| Strategy | Single Create | Notes |
|---------------------|---------------|----------------------------------------|
| Memory | 0.76 µs | Baseline — HashMap insert |
| In-mem SQLite Global| 9.7 µs | ~13x memory — SQLite table creation |
| Local Global | 51 µs | fsync dominates |
| Local Type | 47 µs | Similar — single partition exists |
| Local Hashed | 87 µs | New bucket file on first hit |
| Local Aggregate | **688 µs** | New DB file per aggregate |
| Local PartitionBy | **686 µs** | New DB file per partition key |
| sqld Global | 3.7 ms | Network round-trip + table creation |
| sqld Type | 2.0 ms | Namespace pre-exists from setup |

Per-aggregate and partition-by strategies pay a **~15x penalty** on creation
(688 µs vs 47 µs) because every new aggregate creates a new database file.

### Concurrent Creation: Spread vs Concentrated

"Spread" creates aggregates with distinct types (landing in different partitions).
"Concentrated" creates aggregates sharing one type (landing in the same partition).

At 32 concurrent creates (local stores):

| Strategy | Spread | Concentrated | Ratio |
|--------------|----------|--------------|--------|
| Global | 1.60 ms | 1.51 ms | 1.06x |
| Type | **2.17 ms** | 1.52 ms | **1.43x** |
| Aggregate | 26.5 ms | 36.9 ms | 0.72x |
| Hashed | 1.61 ms | 1.72 ms | 0.94x |
| PartitionBy | 24.5 ms | 25.1 ms | 0.98x |

TypeStrategy spread creation is ~43% slower than concentrated because each unique
type creates a new database file. AggregateStrategy shows the opposite —
concentrated is slower because all aggregates create their own partition regardless,
and contention on the partition directory adds overhead.

## 2. Steady-State Writes

Writes to existing aggregates where the partition is already provisioned.

| Metric | Memory | In-mem SQLite | Local (all) | sqld Type |
|-------------------|----------|---------------|-------------|-----------|
| Append (1 event) | 0.49 µs | 10.2 µs | 46–51 µs | 5.1 ms |
| Batch 1 | 0.49 µs | 9.9 µs | 45–50 µs | 7.1 ms |
| Batch 10 | 3.7 µs | 90 µs | 145–161 µs | 8.7 ms |
| Batch 50 | 16.8 µs | 441 µs | 522–598 µs | 18.8 ms |
| With revision | 915 µs | 2.17 ms | 2.2–2.3 ms | 4.6 ms |

**All local partition strategies perform identically on steady-state writes**
(45–51 µs range). Partition overhead is entirely in creation, not ongoing appends.

Batching is efficient: 10 events costs ~3x a single event (not 10x), showing
good transaction amortization.

`publish_with_revision` is ~2.2 ms across all local variants — the load-then-publish
round trip dominates. On sqld this is 4.6 ms (only ~2x local), because the two
operations pipeline well over the network.

## 3. Load Scaling

Read performance scaling by number of events in the aggregate.

| Events | Memory | In-mem SQLite | Local (all) | sqld Type |
|--------|----------|---------------|--------------|-----------|
| 0 | 156 ns | 3.2 µs | 3.7–4.9 µs | 936 µs |
| 1 | 148 ns | 3.7 µs | 4.6–4.9 µs | 917 µs |
| 10 | 829 ns | 7.4 µs | 8.1–8.6 µs | 1.18 ms |
| 50 | 3.8 µs | 22.6 µs | 23–25 µs | 2.04 ms |
| 100 | 7.6 µs | 42.3 µs | 42–43 µs | 2.65 ms |
| 500 | 42.5 µs | 193 µs | 193–205 µs | 7.5 ms |

Read cost is **identical across all partition strategies**. Once the partition is
resolved, the read path is pure SQLite regardless of strategy.

Scaling is linear: ~0.4 µs/event for local, ~13 µs/event for sqld. Per-event cost
is dominated by JSON deserialization (local) and network transfer (sqld).

Loading a non-existent aggregate costs about the same as loading one with 1 event,
confirming the query overhead is dominated by the SQLite round trip, not row scanning.

## 4. Partition Write Patterns

Concurrent writes to pre-existing aggregates: spread across different partitions
vs concentrated within the same partition vs contention on a single aggregate.

### Local Stores at 32 Concurrent Tasks

| Strategy | Spread | Concentrated | Contention | Spread/Conc |
|--------------|----------|--------------|------------|-------------|
| Global | 1.85 ms | 1.66 ms | 1.56 ms | 1.12x |
| Type | 1.65 ms | 1.60 ms | 1.60 ms | 1.03x |
| Aggregate | 1.95 ms | 2.11 ms | 1.54 ms | 0.92x |
| Hashed | 1.59 ms | 1.63 ms | 1.53 ms | 0.97x |
| PartitionBy | 1.95 ms | 1.99 ms | 1.48 ms | 0.98x |

**For local stores, partitioning provides no measurable write throughput benefit.**
Spread vs concentrated writes are within noise margin. All local SQLite stores
serialize writes through a Mutex per connection — concurrent tasks queue rather
than parallelize.

Contention (same aggregate) is consistently the fastest because all tasks hit the
same cached connection and partition lookup with no new connections opened.

### Remote (sqld) Creation Patterns

| Strategy | Spread @32 | Concentrated @32 |
|-------------------------|------------|-------------------|
| sqld Default Global | 59 ms | 101 ms |
| sqld Global (namespaced)| 77 ms | 72 ms |
| sqld Type (namespaced) | **19 ms** | **216 ms** |

`sqld_by_type` shows the most dramatic divergence: **spread is 11x faster than
concentrated at 32 tasks.** When each task targets a different namespace (spread),
the server handles them independently. When all tasks hit the same namespace
(concentrated), they serialize server-side. This is the opposite of the local
pattern — remote stores benefit significantly from partition spreading.

## 5. Partition Read Patterns

Concurrent reads from pre-existing aggregates (50 events each).

### Local Stores at 32 Concurrent Readers

| Strategy | Spread | Concentrated | Spread/Conc |
|--------------|---------|--------------|-------------|
| Global | 766 µs | 792 µs | 0.97x |
| Type | 791 µs | 779 µs | 1.02x |
| Aggregate | 775 µs | 787 µs | 0.98x |
| Hashed | 877 µs | 791 µs | 1.11x |
| PartitionBy | 770 µs | 790 µs | 0.97x |

**Reads show no spread/concentrated difference for local stores.** SQLite's WAL
mode allows concurrent readers without blocking, so partitioning has no impact.
The ~25 µs per-aggregate cost (~790 µs / 32) matches the single-aggregate load
time for 50 events.

## 6. Mixed Read/Write Workload

32 readers + 32 writers (64 total tasks) across spread partitions.

| Strategy | Time | Per-task |
|--------------|-----------|----------|
| Global | 2.69 ms | 42 µs |
| Type | 2.68 ms | 42 µs |
| Aggregate | 3.04 ms | 48 µs |
| Hashed | 2.49 ms | 39 µs |
| PartitionBy | 2.46 ms | 38 µs |

All strategies within 20% of each other. Writers do not significantly block readers.

## 7. sqld Container Stability

The sqld Docker container (via testcontainers) becomes unresponsive under sustained
heavy benchmark load — typically after batch-50 writes or extended concurrent
workloads. This limits the completeness of remote benchmark runs. The sqld_by_type
variant consistently completes all benchmarks, likely because type-based partitioning
spreads load across namespaces more evenly.

## Summary

| Dimension | Local stores | Remote (sqld) stores |
|--------------------------------|---------------------------------------------------|--------------------------------------------------------|
| **Creation cost** | Strategy-dependent: 47 µs (Global) to 688 µs (Aggregate) | 2–5 ms per aggregate + namespace setup |
| **Steady-state writes** | ~48 µs regardless of strategy | ~2–5 ms (network-dominated) |
| **Load scaling** | ~0.4 µs/event, strategy-independent | ~13 µs/event (transfer-dominated) |
| **Spread vs concentrated writes** | No difference (Mutex serializes locally) | Spread significantly faster (server parallelism) |
| **Spread vs concentrated reads** | No difference (WAL concurrent readers) | Expected benefit from server-side parallelism |
| **Write bottleneck** | fsync | Network round-trip |
| **Read bottleneck** | JSON deserialization | Network transfer |

### Implications for Strategy Selection

- **Local stores:** Partition strategy choice has negligible performance impact on
  reads and steady-state writes. Choose based on operational concerns (backup
  granularity, data isolation) rather than performance. Avoid per-aggregate
  partitioning if aggregate creation rate is high (15x creation penalty).

- **Remote stores:** Partition strategy matters significantly. Type-based or
  hashed partitioning enables server-side parallelism for concurrent workloads,
  delivering up to 11x throughput improvement over concentrated access patterns.
  Global strategy forces all operations through a single server-side bottleneck.
