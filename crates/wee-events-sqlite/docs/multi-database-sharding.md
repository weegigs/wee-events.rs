# Multi-Database Sharding

## The Insight

An aggregate in event sourcing is a transactional consistency boundary. A SQLite database is a transactional consistency boundary. When these two concepts align for event storage — one database per aggregate, or per aggregate type — contention disappears by construction.

This mirrors DynamoDB's partition key sharding: the aggregate ID is the partition key, the SQLite file is the partition, and the filesystem is the partition manager.

## Partitioning Strategies

The SQLite event-store internals are identical regardless of how databases are partitioned. Only the open/enumerate surface changes.

### Single Database (current)

All aggregates share one database file. Simplest deployment. Appropriate when a single actor owns all writes, or when the total data volume is small.

```
data/
  store.db          ← events for all aggregates
```

Enumerate aggregates: `SELECT DISTINCT aggregate_type, aggregate_key FROM events`

### Database per Aggregate Type

One database per aggregate type. Reduces contention between unrelated aggregate types (e.g., campaigns vs. characters) while keeping same-type aggregates together.

```
data/
  campaign.db       ← events for all campaigns
  character.db      ← events for all characters
```

Enumerate aggregates: directory listing → per-file SQL query.

### Database per Aggregate

Each aggregate is its own file. Maximum isolation. Each actor owns exactly one file — no sharing, no mutex, no contention. The actor model, the ownership model, and the storage model all enforce the same boundary.

```
data/
  campaign/
    c1.db           ← events for campaign c1
    c2.db           ← events for campaign c2
  character/
    ch1.db          ← events for character ch1
```

Enumerate aggregates: directory scan. The `AggregateId` `Display`↔`FromStr` round-trip (`"campaign:c1"`) maps directly to the path structure (`campaign/c1.db`).

## Scope

This document is about event storage only: how to partition aggregate streams across SQLite files.

Projection layout is a separate concern. Aggregate boundaries are write-side consistency boundaries; read models are shaped by query needs and may be per-aggregate, per-type, or cross-aggregate. See `projection-and-document-storage.md`.

## Operational Benefits of Database-per-Aggregate

- **Backup** is file copy. Archive old runs by moving files to cold storage.
- **Cleanup** is file deletion. No `DELETE FROM events WHERE aggregate_key = ?`.
- **Debugging** is file sharing. Send someone a `.db` file and they have the full history.
- **No index bloat.** Each database is small. Queries are fast without tuning.
- **WAL isolation.** Write-ahead logs don't interfere across aggregates.

## Relationship to DynamoDB Sharding

The analogy is direct:

| DynamoDB | SQLite Multi-DB |
|----------|-----------------|
| Partition key | Aggregate ID |
| Partition | SQLite file |
| Partition manager | Filesystem |
| Hot partition → rebalance | Large aggregate → snapshot |
| Cross-partition query → scan | Cross-database aggregate enumeration → file scan |
| GSI for cross-partition reads | Separate read model or index store |

The key difference: DynamoDB rebalances partitions automatically as they grow. With SQLite files, the equivalent operation for a large aggregate is snapshotting — store state at event N, replay only from the snapshot on load. The "partition" (file) stays the same, but the read cost drops.

## Implementation Notes

This is a design-space document, not an implementation plan. The current stores use a single database file per process. The internal code (`DocumentStore`, `SqliteEventStore`, projection functions) requires no changes to support any partitioning strategy — only the open/enumerate surface would change.
