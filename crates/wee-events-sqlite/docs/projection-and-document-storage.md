# Projection and Document Storage

## The Distinction

Event streams are partitioned by aggregate because the aggregate is the write-side transactional consistency boundary.

Projections and documents are different. They are read models, and read models should be organized around query patterns. Sometimes that means one document per aggregate. Often it does not.

An order aggregate is a good write boundary. But useful read models might be:

- all orders for a customer
- all orders shipped on a specific day
- all open orders by warehouse
- daily revenue totals

Those are aggregations over many aggregates. Their natural storage layout is query-oriented, not aggregate-oriented.

## Why This Matters

If events are sharded per aggregate, storing projections in those same files makes cross-aggregate reads expensive by construction. Queries like "orders for customer c42" would require scanning many aggregate files unless a second index exists elsewhere.

That means projection layout should be chosen independently from event layout.

## Common Projection Layouts

### Aggregate-Local Documents

One document per aggregate, keyed by aggregate ID.

Good for:

- load current state for a single aggregate
- simple rebuilds
- portable debugging

Weak for:

- queries over many aggregates
- filtering and sorting by non-key fields

### Collection-Oriented Documents

One collection per read model type, keyed for the query shape.

Examples:

- `orders_by_id`
- `orders_by_customer`
- `orders_by_ship_date`

Good for:

- application screens and APIs
- list views
- secondary access patterns

This matches the current `DocumentStore` API well: documents are keyed by `(collection, key)`, which is a read-model shape, not a hard-coded aggregate boundary.

### Separate Read Database

Keep the event store partitioned by aggregate, but publish projections into a separate SQLite database dedicated to read models.

Good for:

- cross-aggregate queries
- indexing by query fields
- keeping write contention and read indexing concerns separate

This is the cleanest option once projections stop being aggregate-local.

## The Real Invariant

The important invariant is not that events and documents live together.

The important invariant is that documents are disposable and rebuildable from events.

That allows several valid designs:

- events and documents in one database
- events sharded by aggregate, documents in a type-level database
- events sharded by aggregate, documents in a separate read database
- multiple specialized projection stores for different query families

## Consistency Model

Events are the source of truth.

Documents are derived state. They may be updated in the same transaction as event publication, or asynchronously afterward. If they lag or are lost, they can be rebuilt by replaying events.

That means document storage should be optimized for read behavior, not mistaken for a write-side consistency boundary.

## Practical Guidance

For event storage:

- partition by aggregate or aggregate type when you want contention isolation

For document storage:

- start from the queries you need to answer
- group documents by read model type
- create separate indexes or stores for cross-aggregate queries
- only co-locate documents with aggregate events when the read pattern is aggregate-local

## Relationship to the Current Code

The current `DocumentStore` stores JSON by `(collection, key)` and supports listing a collection. That API already implies query-oriented organization:

- `collection` identifies the read model family
- `key` identifies a document within that family

The current implementation can therefore support both aggregate-local and cross-aggregate projection layouts. What changes is not the projection mechanism, but where projected documents are written and how they are keyed.
