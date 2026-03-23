# Multi-Database Sharding

## The Insight

An aggregate in event sourcing is a transactional consistency boundary. A SQLite database is a transactional consistency boundary. When these two concepts align — one database per aggregate, or per aggregate type — contention disappears by construction.

This mirrors DynamoDB's partition key sharding: the aggregate ID is the partition key, the SQLite file is the partition, and the filesystem is the partition manager.

## Partitioning Strategies

The `SqliteStore` internals (event persistence, document persistence, projections) are identical regardless of how databases are partitioned. Only the open/enumerate surface changes.

### Single Database (current)

All aggregates share one database file. Simplest deployment. Appropriate when a single actor owns all writes, or when the total data volume is small.

```
data/
  store.db          ← events + documents for all aggregates
```

Enumerate aggregates: `SELECT DISTINCT aggregate_type, aggregate_key FROM events`

### Database per Aggregate Type

One database per aggregate type. Reduces contention between unrelated aggregate types (e.g., campaigns vs. characters) while keeping same-type aggregates together.

```
data/
  campaign.db       ← events + documents for all campaigns
  character.db      ← events + documents for all characters
```

Enumerate aggregates: directory listing → per-file SQL query.

### Database per Aggregate

Each aggregate is its own file. Maximum isolation. Each actor owns exactly one file — no sharing, no mutex, no contention. The actor model, the ownership model, and the storage model all enforce the same boundary.

```
data/
  campaign/
    c1.db           ← events + documents for campaign c1
    c2.db           ← events + documents for campaign c2
  character/
    ch1.db          ← events + documents for character ch1
```

Enumerate aggregates: directory scan. The `AggregateId` `Display`↔`FromStr` round-trip (`"campaign:c1"`) maps directly to the path structure (`campaign/c1.db`).

## Why Events and Documents Belong Together

In any partitioning strategy, events and documents live in the same database file. They share a consistency boundary because documents are projections of that aggregate's events. Benefits:

- **Self-contained rebuild.** Delete the documents table, replay events, projections regenerate. No cross-database coordination.
- **Portable.** Copy the file and the entire aggregate — source of truth plus read model — moves with it.
- **Atomic projection** is possible (single transaction for publish + upsert) though not required. The current design commits events first, then projects — crash recovery rebuilds from events on restart.

## Operational Benefits of Database-per-Aggregate

- **Backup** is file copy. Archive old runs by moving files to cold storage.
- **Cleanup** is file deletion. No `DELETE FROM events WHERE aggregate_key = ?`.
- **Debugging** is file sharing. Send someone a `.db` file and they have the full history.
- **No index bloat.** Each database is small. Queries are fast without tuning.
- **WAL isolation.** Write-ahead logs don't interfere across aggregates.

## Cross-Aggregate Reads

The trade-off with per-aggregate sharding is cross-aggregate queries. "Show all runs" requires opening multiple files. Options:

1. **Scan and read.** At small scale (tens to hundreds of aggregates), open each file and read summary state. Fast enough for desktop applications.
2. **Lightweight index.** A separate index database with just aggregate IDs and summary metadata, updated by the application layer after each projection.
3. **Filesystem metadata.** File names, modification times, and directory structure carry information. For "list all campaigns," a directory listing suffices.

At game scale — dozens of runs with 5-30 turns each — option 1 is sufficient. Option 2 is trivial to add later if needed.

## Relationship to DynamoDB Sharding

The analogy is direct:

| DynamoDB | SQLite Multi-DB |
|----------|-----------------|
| Partition key | Aggregate ID |
| Partition | SQLite file |
| Partition manager | Filesystem |
| Hot partition → rebalance | Large aggregate → snapshot |
| Cross-partition query → scan | Cross-database query → file scan |
| GSI for cross-partition reads | Index DB for cross-aggregate reads |

The key difference: DynamoDB rebalances partitions automatically as they grow. With SQLite files, the equivalent operation for a large aggregate is snapshotting — store state at event N, replay only from the snapshot on load. The "partition" (file) stays the same, but the read cost drops.

## Implementation Notes

This is a design-space document, not an implementation plan. The current `SqliteStore` uses a single database. The internal code (`DocumentStore`, `SqliteEventStore`, projection functions) requires no changes to support any partitioning strategy — only the factory's `open` method and `enumerate_aggregates` surface would change.
