//! Event store performance benchmark suite.
//!
//! Provides reusable benchmark functions and a macro for running criterion
//! benchmarks against any `EventStore` implementation. Benchmarks are
//! organized into groups that isolate specific costs:
//!
//! - **creation** — first write to a new aggregate (includes partition provisioning)
//! - **`steady_state`** — writes to existing aggregates
//! - **`load_scaling`** — reads scaling by event count
//! - **`partition_write`** — concurrent writes spread across vs concentrated in partitions
//! - **`partition_read`** — concurrent reads spread across vs concentrated in partitions
//!
//! # Usage
//!
//! ```text
//! use criterion::criterion_main;
//! wee_events::testing::store_bench_suite!(my_store, MyStore::new());
//! criterion_main!(my_store::benches);
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion};
use tokio::runtime::Runtime;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

use crate::id::AggregateId;
use crate::store::{EventStore, PublishOptions, RawEvent};
use crate::test_suite::make_raw_events;

/// Drives a `JoinSet` to completion, propagating panics. Replaces the prior
/// `join_all(...)` pattern which polled all futures on a single task — i.e.
/// not concurrent at all on a multi-thread runtime.
async fn drain<T: 'static>(mut set: JoinSet<T>) {
    while let Some(r) = set.join_next().await {
        r.expect("benchmark task panicked");
    }
}

/// Default concurrency levels for concurrent benchmarks.
pub const CONCURRENCY_LEVELS: &[usize] = &[2, 4, 8, 16, 32];

const LOAD_EVENT_COUNTS: &[usize] = &[1, 10, 50, 100, 500];
const BATCH_SIZES: &[usize] = &[1, 10, 50];

// ---------------------------------------------------------------------------
// Aggregate ID generators for partition-aware benchmarks
//
// Process-global atomic counter replaces `ulid::Ulid::new()` (microseconds +
// `String` alloc) with a single relaxed atomic increment + 16-hex-char format.
// Order doesn't matter for benches; only that IDs are unique across the run.
// ---------------------------------------------------------------------------

static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_unique() -> String {
    format!("{:016x}", BENCH_COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// Creates an aggregate ID guaranteed to be in a unique partition for most
/// strategies. Each call with a different `index` produces a different
/// aggregate type, which `TypeStrategy` maps to a distinct partition.
fn make_spread_id(index: usize) -> AggregateId {
    AggregateId::new(format!("spread-{index}"), next_unique())
}

/// Creates an aggregate ID that shares a single aggregate type with all other
/// concentrated IDs. `TypeStrategy` maps these to the same partition.
fn make_concentrated_id() -> AggregateId {
    AggregateId::new("concentrated", next_unique())
}

/// Creates a generic test aggregate ID (same as conformance suite).
fn make_test_id() -> AggregateId {
    AggregateId::new("bench", next_unique())
}

/// Builds a fresh `Vec<RawEvent>` of size `n` by cloning a single pre-encoded
/// template. JSON serialisation happens once per call to `make_raw_events`;
/// callers that need many vecs (one per spawned task) should hold this and
/// call `.clone()` outside the timed region.
fn prebuilt_raw_events(n: usize) -> Vec<RawEvent> {
    make_raw_events(n).1
}

/// Seeds an aggregate with `n` events. Returns the aggregate ID.
fn seed_aggregate<S: EventStore>(rt: &Runtime, store: &S, id: &AggregateId, event_count: usize) {
    if event_count == 0 {
        return;
    }
    let raw = prebuilt_raw_events(event_count);
    rt.block_on(store.publish(id, PublishOptions::default(), raw))
        .unwrap();
}

// ===========================================================================
// Group: creation — cost of first write (partition provisioning)
// ===========================================================================

/// Benchmark creating a single new aggregate (first publish).
/// This includes any partition/namespace provisioning cost.
pub fn bench_create_aggregate<S: EventStore>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &S,
    prefix: &str,
) {
    let raw_template = prebuilt_raw_events(1);
    c.bench_function(&format!("{prefix}/creation/single"), |b| {
        b.to_async(rt).iter_batched(
            || (make_test_id(), raw_template.clone()),
            |(id, raw)| async move {
                store
                    .publish(&id, PublishOptions::default(), raw)
                    .await
                    .unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

/// Benchmark creating N new aggregates concurrently, each in a different
/// partition (spread across partitions by varying aggregate type).
pub fn bench_create_spread<S: EventStore + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &Arc<S>,
    prefix: &str,
    levels: &[usize],
) {
    let raw_template = prebuilt_raw_events(1);
    let mut group = c.benchmark_group(format!("{prefix}/creation/spread"));
    for &n in levels {
        group.bench_function(format!("{n}"), |b| {
            // Setup builds N (id, raw) pairs outside the timed region. Routine
            // spawns one publish task per pair. `Arc::clone(store)` happens
            // once per spawned task — required for the 'static future bound —
            // not in the outer iter closure.
            b.to_async(rt).iter_batched(
                || -> Vec<(AggregateId, Vec<RawEvent>)> {
                    (0..n)
                        .map(|i| (make_spread_id(i), raw_template.clone()))
                        .collect()
                },
                |inputs| async {
                    let mut set = JoinSet::new();
                    for (id, raw) in inputs {
                        let store = Arc::clone(store);
                        set.spawn(async move {
                            store
                                .publish(&id, PublishOptions::default(), raw)
                                .await
                                .expect("publish should succeed in bench");
                        });
                    }
                    drain(set).await;
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Benchmark creating N new aggregates concurrently, all in the same
/// partition (same aggregate type).
pub fn bench_create_concentrated<S: EventStore + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &Arc<S>,
    prefix: &str,
    levels: &[usize],
) {
    let raw_template = prebuilt_raw_events(1);
    let mut group = c.benchmark_group(format!("{prefix}/creation/concentrated"));
    for &n in levels {
        group.bench_function(format!("{n}"), |b| {
            b.to_async(rt).iter_batched(
                || -> Vec<(AggregateId, Vec<RawEvent>)> {
                    (0..n)
                        .map(|_| (make_concentrated_id(), raw_template.clone()))
                        .collect()
                },
                |inputs| async {
                    let mut set = JoinSet::new();
                    for (id, raw) in inputs {
                        let store = Arc::clone(store);
                        set.spawn(async move {
                            store
                                .publish(&id, PublishOptions::default(), raw)
                                .await
                                .expect("publish should succeed in bench");
                        });
                    }
                    drain(set).await;
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ===========================================================================
// Group: steady_state — writes to pre-existing aggregates
// ===========================================================================

/// Benchmark publishing varying batch sizes to an existing aggregate.
pub fn bench_publish_batch<S: EventStore>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &S,
    prefix: &str,
) {
    let seed_template = prebuilt_raw_events(1);
    let mut group = c.benchmark_group(format!("{prefix}/steady_state/publish_batch"));
    for &batch_size in BATCH_SIZES {
        let raw_template = prebuilt_raw_events(batch_size);
        group.bench_function(format!("{batch_size}"), |b| {
            // `iter_custom` so async setup is `.await`ed instead of
            // `rt.block_on`'d (which would deadlock — the whole closure
            // already runs inside Criterion's `rt.block_on`). Fresh
            // aggregate per measurement iteration kills the monotonic
            // growth that the old single-aggregate `iter` form had.
            let seed_template = &seed_template;
            let raw_template = &raw_template;
            b.to_async(rt).iter_custom(move |iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let id = make_test_id();
                    store
                        .publish(&id, PublishOptions::default(), seed_template.clone())
                        .await
                        .expect("seed should succeed in bench");

                    let raw = raw_template.clone();
                    let start = Instant::now();
                    store
                        .publish(&id, PublishOptions::default(), raw)
                        .await
                        .expect("publish should succeed in bench");
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// NOTE: `raw_template.clone()` above happens *before* `Instant::now()`, so the
// Vec/payload allocation is excluded from the measurement. Same convention
// applies in every `iter_custom` body below.

/// Benchmark publish with optimistic concurrency check on an existing aggregate.
pub fn bench_publish_with_revision<S: EventStore>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &S,
    prefix: &str,
) {
    let seed_template = prebuilt_raw_events(1);
    let raw_template = prebuilt_raw_events(1);
    c.bench_function(
        &format!("{prefix}/steady_state/publish_with_revision"),
        |b| {
            // `iter_custom` — see `bench_publish_batch` for the rationale.
            // Fresh aggregate per iter so the `load` step doesn't walk an
            // ever-longer event stream.
            let seed_template = &seed_template;
            let raw_template = &raw_template;
            b.to_async(rt).iter_custom(move |iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let id = make_test_id();
                    store
                        .publish(&id, PublishOptions::default(), seed_template.clone())
                        .await
                        .expect("seed should succeed in bench");
                    let raw = raw_template.clone();

                    let start = Instant::now();
                    let agg = store.load(&id).await.expect("load should succeed in bench");
                    let opts = PublishOptions {
                        expected_revision: Some(agg.revision().clone()),
                        ..Default::default()
                    };
                    store
                        .publish(&id, opts, raw)
                        .await
                        .expect("publish should succeed in bench");
                    total += start.elapsed();
                }
                total
            });
        },
    );
}

/// Benchmark appending to a growing aggregate stream.
pub fn bench_publish_append<S: EventStore>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &S,
    prefix: &str,
) {
    let seed_template = prebuilt_raw_events(1);
    let raw_template = prebuilt_raw_events(1);
    c.bench_function(&format!("{prefix}/steady_state/append"), |b| {
        // `iter_custom` — see `bench_publish_batch` for the rationale.
        // Fresh aggregate per iter so the timed publish always lands on an
        // aggregate of length 1.
        let seed_template = &seed_template;
        let raw_template = &raw_template;
        b.to_async(rt).iter_custom(move |iters| async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let id = make_test_id();
                store
                    .publish(&id, PublishOptions::default(), seed_template.clone())
                    .await
                    .expect("seed should succeed in bench");

                let raw = raw_template.clone();
                let start = Instant::now();
                store
                    .publish(&id, PublishOptions::default(), raw)
                    .await
                    .expect("publish should succeed in bench");
                total += start.elapsed();
            }
            total
        });
    });
}

// ===========================================================================
// Group: load_scaling — read performance by event count
// ===========================================================================

/// Benchmark loading aggregates with varying event counts.
pub fn bench_load_scaling<S: EventStore>(c: &mut Criterion, rt: &Runtime, store: &S, prefix: &str) {
    let mut group = c.benchmark_group(format!("{prefix}/load_scaling"));

    // Empty aggregate (non-existent). Fresh ID per iter so the store's
    // negative-lookup cache (if any) doesn't get a free hit. ID built outside
    // the timed region via `iter_batched` setup.
    group.bench_function("0", |b| {
        b.to_async(rt).iter_batched(
            make_test_id,
            |id| async move {
                store.load(&id).await.unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    // Aggregates with events. One pre-seeded ID per count, captured by
    // reference inside the timed routine — no per-iter clone.
    for &count in LOAD_EVENT_COUNTS {
        let id = make_test_id();
        seed_aggregate(rt, store, &id, count);

        group.bench_function(format!("{count}"), |b| {
            let id = &id;
            b.to_async(rt).iter(|| async move {
                store.load(id).await.unwrap();
            });
        });
    }
    group.finish();
}

// ===========================================================================
// Group: partition_write — concurrent writes spread vs concentrated
// ===========================================================================

/// Concurrent writes to pre-existing aggregates spread across different
/// partitions (each aggregate has a unique type).
pub fn bench_write_spread<S: EventStore + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &Arc<S>,
    prefix: &str,
    levels: &[usize],
) {
    let seed_template = prebuilt_raw_events(10);
    let raw_template = prebuilt_raw_events(1);
    let mut group = c.benchmark_group(format!("{prefix}/partition_write/spread"));
    for &n in levels {
        group.bench_function(format!("{n}"), |b| {
            // setup (untimed)  : build N fresh aggregates, seed each with 10
            //                    events, then prepare N pre-cloned raw event
            //                    vecs for the timed spawn loop.
            // timed region     : `tokio::spawn` one publish per aggregate.
            let seed_template = &seed_template;
            let raw_template = &raw_template;
            b.to_async(rt).iter_custom(move |iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let ids: Vec<AggregateId> = (0..n).map(make_spread_id).collect();
                    for id in &ids {
                        store
                            .publish(id, PublishOptions::default(), seed_template.clone())
                            .await
                            .expect("seed should succeed in bench");
                    }
                    let pre_cloned: Vec<(AggregateId, Vec<RawEvent>)> = ids
                        .into_iter()
                        .map(|id| (id, raw_template.clone()))
                        .collect();

                    let start = Instant::now();
                    let mut set = JoinSet::new();
                    for (id, raw) in pre_cloned {
                        let store = Arc::clone(store);
                        set.spawn(async move {
                            store
                                .publish(&id, PublishOptions::default(), raw)
                                .await
                                .expect("publish should succeed in bench");
                        });
                    }
                    drain(set).await;
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

/// Concurrent writes to pre-existing aggregates all within the same
/// partition (same aggregate type, different keys).
pub fn bench_write_concentrated<S: EventStore + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &Arc<S>,
    prefix: &str,
    levels: &[usize],
) {
    let seed_template = prebuilt_raw_events(10);
    let raw_template = prebuilt_raw_events(1);
    let mut group = c.benchmark_group(format!("{prefix}/partition_write/concentrated"));
    for &n in levels {
        group.bench_function(format!("{n}"), |b| {
            // Same `iter_custom` shape as `bench_write_spread`, except all
            // aggregates share the "concentrated" type.
            let seed_template = &seed_template;
            let raw_template = &raw_template;
            b.to_async(rt).iter_custom(move |iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let ids: Vec<AggregateId> = (0..n).map(|_| make_concentrated_id()).collect();
                    for id in &ids {
                        store
                            .publish(id, PublishOptions::default(), seed_template.clone())
                            .await
                            .expect("seed should succeed in bench");
                    }
                    let pre_cloned: Vec<(AggregateId, Vec<RawEvent>)> = ids
                        .into_iter()
                        .map(|id| (id, raw_template.clone()))
                        .collect();

                    let start = Instant::now();
                    let mut set = JoinSet::new();
                    for (id, raw) in pre_cloned {
                        let store = Arc::clone(store);
                        set.spawn(async move {
                            store
                                .publish(&id, PublishOptions::default(), raw)
                                .await
                                .expect("publish should succeed in bench");
                        });
                    }
                    drain(set).await;
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

/// Concurrent writes to the same aggregate (maximum contention).
pub fn bench_write_contention<S: EventStore + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &Arc<S>,
    prefix: &str,
    levels: &[usize],
) {
    let seed_template = prebuilt_raw_events(1);
    let raw_template = prebuilt_raw_events(1);
    let mut group = c.benchmark_group(format!("{prefix}/partition_write/contention"));
    for &n in levels {
        group.bench_function(format!("{n}"), |b| {
            // setup (untimed)  : fresh aggregate seeded with 1 event; the N
            //                    per-writer raw event vecs are pre-cloned.
            // timed region     : N writers spawn, all `.wait()` on a barrier,
            //                    then publish simultaneously. Real contention
            //                    on one aggregate.
            let seed_template = &seed_template;
            let raw_template = &raw_template;
            b.to_async(rt).iter_custom(move |iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let id = make_test_id();
                    store
                        .publish(&id, PublishOptions::default(), seed_template.clone())
                        .await
                        .expect("seed should succeed in bench");
                    let pre_cloned: Vec<Vec<RawEvent>> =
                        (0..n).map(|_| raw_template.clone()).collect();

                    let start = Instant::now();
                    let barrier = Arc::new(Barrier::new(n));
                    let mut set = JoinSet::new();
                    for raw in pre_cloned {
                        let store = Arc::clone(store);
                        let id = id.clone();
                        let barrier = Arc::clone(&barrier);
                        set.spawn(async move {
                            barrier.wait().await;
                            // No `expected_revision`: every writer can
                            // commit; this measures pure publish-path
                            // contention on the store's locks.
                            store
                                .publish(&id, PublishOptions::default(), raw)
                                .await
                                .expect("publish should succeed in bench");
                        });
                    }
                    drain(set).await;
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// ===========================================================================
// Group: partition_read — concurrent reads spread vs concentrated
// ===========================================================================

/// Concurrent reads from pre-existing aggregates spread across different
/// partitions.
pub fn bench_read_spread<S: EventStore + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &Arc<S>,
    prefix: &str,
    levels: &[usize],
) {
    let mut group = c.benchmark_group(format!("{prefix}/partition_read/spread"));
    for &n in levels {
        let ids: Arc<[AggregateId]> = (0..n)
            .map(make_spread_id)
            .collect::<Vec<_>>()
            .into_boxed_slice()
            .into();
        for id in ids.iter() {
            seed_aggregate(rt, &**store, id, 50);
        }

        group.bench_function(format!("{n}"), |b| {
            // Capture `ids` and `store` by reference — only the per-spawn
            // Arc<str> id clone and the per-task `Arc::clone(store)` happen
            // inside the timed region.
            let ids = &ids;
            b.to_async(rt).iter(|| async move {
                let mut set = JoinSet::new();
                for id in ids.iter() {
                    let store = Arc::clone(store);
                    let id = id.clone();
                    set.spawn(async move {
                        store.load(&id).await.expect("load should succeed in bench");
                    });
                }
                drain(set).await;
            });
        });
    }
    group.finish();
}

/// Concurrent reads from pre-existing aggregates all within the same
/// partition.
pub fn bench_read_concentrated<S: EventStore + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &Arc<S>,
    prefix: &str,
    levels: &[usize],
) {
    let mut group = c.benchmark_group(format!("{prefix}/partition_read/concentrated"));
    for &n in levels {
        let ids: Arc<[AggregateId]> = (0..n)
            .map(|_| make_concentrated_id())
            .collect::<Vec<_>>()
            .into_boxed_slice()
            .into();
        for id in ids.iter() {
            seed_aggregate(rt, &**store, id, 50);
        }

        group.bench_function(format!("{n}"), |b| {
            let ids = &ids;
            b.to_async(rt).iter(|| async move {
                let mut set = JoinSet::new();
                for id in ids.iter() {
                    let store = Arc::clone(store);
                    let id = id.clone();
                    set.spawn(async move {
                        store.load(&id).await.expect("load should succeed in bench");
                    });
                }
                drain(set).await;
            });
        });
    }
    group.finish();
}

// ===========================================================================
// Group: mixed — concurrent reads and writes across partitions
// ===========================================================================

/// Half the tasks read from spread partitions, half write to spread
/// partitions. Measures interference between readers and writers.
pub fn bench_mixed_read_write<S: EventStore + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    store: &Arc<S>,
    prefix: &str,
    levels: &[usize],
) {
    let seed_template = prebuilt_raw_events(50);
    let raw_template = prebuilt_raw_events(1);
    let mut group = c.benchmark_group(format!("{prefix}/mixed/read_write_spread"));
    for &n in levels {
        group.bench_function(format!("{n}r_{n}w"), |b| {
            // setup (untimed)  : 2N fresh aggregates each seeded with 50
            //                    events. Even indices are read targets, odd
            //                    are write targets. N raw event vecs are
            //                    pre-cloned for the writers.
            // timed region     : spawn 2N tasks; readers `load`, writers
            //                    `publish`; drain.
            let seed_template = &seed_template;
            let raw_template = &raw_template;
            b.to_async(rt).iter_custom(move |iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let all_ids: Vec<AggregateId> = (0..2 * n).map(make_spread_id).collect();
                    for id in &all_ids {
                        store
                            .publish(id, PublishOptions::default(), seed_template.clone())
                            .await
                            .expect("seed should succeed in bench");
                    }
                    // Pre-clone one raw vec per writer (odd index).
                    let writer_raws: Vec<Vec<RawEvent>> =
                        (0..n).map(|_| raw_template.clone()).collect();
                    let mut writer_raws = writer_raws.into_iter();

                    let start = Instant::now();
                    let mut set = JoinSet::new();
                    for (i, id) in all_ids.into_iter().enumerate() {
                        let store = Arc::clone(store);
                        let is_reader = i % 2 == 0;
                        if is_reader {
                            set.spawn(async move {
                                store.load(&id).await.expect("load should succeed in bench");
                            });
                        } else {
                            let raw = writer_raws.next().expect("one raw per writer");
                            set.spawn(async move {
                                store
                                    .publish(&id, PublishOptions::default(), raw)
                                    .await
                                    .expect("publish should succeed in bench");
                            });
                        }
                    }
                    drain(set).await;
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// ===========================================================================
// Macro
// ===========================================================================

/// Generates a benchmark module that runs the full performance suite.
///
/// # Usage
///
/// ```text
/// // Default concurrency levels (2, 4, 8, 16, 32):
/// wee_events::testing::store_bench_suite!(memory_store, {
///     wee_events::memory::MemoryStore::new()
/// });
///
/// // Custom concurrency levels (for stores with expensive provisioning):
/// wee_events::testing::store_bench_suite!(heavy_store, &[2, 4, 8], {
///     HeavyStore::new().await
/// });
///
/// criterion_main!(memory_store::benches);
/// ```
#[macro_export]
macro_rules! store_bench_suite {
    ($mod_name:ident, $factory:expr) => {
        $crate::store_bench_suite!($mod_name, $crate::testing::CONCURRENCY_LEVELS, $factory);
    };
    ($mod_name:ident, $levels:expr, $factory:expr) => {
        mod $mod_name {
            use super::*;
            use criterion::{Criterion, criterion_group};
            use std::sync::Arc;

            fn store_benchmarks(c: &mut Criterion) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let prefix = stringify!($mod_name);
                let levels: &[usize] = $levels;

                // ─── Per-group fresh store ──────────────────────────────────
                // Each block constructs its own store from `$factory`, runs
                // one group of benches, then drops the store at the closing
                // brace. Without this, every bench writes into one shared
                // store; cumulative state grows into tens of millions of
                // events (multi-GB heap) by the time the last group runs,
                // and the post-suite deallocation can take many minutes —
                // observed as an apparent hang.
                //
                // Implicit contract: `$factory` must be safely re-evaluable.
                // `MemoryStore::new()` and similar are trivially fine; a
                // factory that allocates a real on-disk file (e.g. a SQLite
                // store at a fixed path) needs to use a unique path per call
                // or this will collide.
                // ────────────────────────────────────────────────────────────

                // Creation
                {
                    let store_arc = Arc::new(rt.block_on(async { $factory }));
                    let store_ref = &*store_arc;
                    $crate::testing::bench_create_aggregate(c, &rt, store_ref, prefix);
                    $crate::testing::bench_create_spread(c, &rt, &store_arc, prefix, levels);
                    $crate::testing::bench_create_concentrated(c, &rt, &store_arc, prefix, levels);
                }

                // Steady-state writes
                {
                    let store_arc = Arc::new(rt.block_on(async { $factory }));
                    let store_ref = &*store_arc;
                    $crate::testing::bench_publish_batch(c, &rt, store_ref, prefix);
                    $crate::testing::bench_publish_with_revision(c, &rt, store_ref, prefix);
                    $crate::testing::bench_publish_append(c, &rt, store_ref, prefix);
                }

                // Load scaling
                {
                    let store_arc = Arc::new(rt.block_on(async { $factory }));
                    let store_ref = &*store_arc;
                    $crate::testing::bench_load_scaling(c, &rt, store_ref, prefix);
                }

                // Partition write patterns
                {
                    let store_arc = Arc::new(rt.block_on(async { $factory }));
                    $crate::testing::bench_write_spread(c, &rt, &store_arc, prefix, levels);
                    $crate::testing::bench_write_concentrated(c, &rt, &store_arc, prefix, levels);
                    $crate::testing::bench_write_contention(c, &rt, &store_arc, prefix, levels);
                }

                // Partition read patterns
                {
                    let store_arc = Arc::new(rt.block_on(async { $factory }));
                    $crate::testing::bench_read_spread(c, &rt, &store_arc, prefix, levels);
                    $crate::testing::bench_read_concentrated(c, &rt, &store_arc, prefix, levels);
                }

                // Mixed workload
                {
                    let store_arc = Arc::new(rt.block_on(async { $factory }));
                    $crate::testing::bench_mixed_read_write(c, &rt, &store_arc, prefix, levels);
                }

                // Bounded runtime shutdown — see the original commit for
                // rationale. With per-group store drops above, the heap at
                // this point should be tiny.
                rt.shutdown_timeout(::core::time::Duration::from_secs(1));
            }

            // Bounded cost: 20 samples per bench, 1s warm-up + 5s
            // measurement, so a full suite caps at ~35 benches × ~7 s ≈ 4
            // min on a quiet box. Criterion's defaults (100 samples / 5 s
            // measurement) regularly blow past 30 min for the contention
            // and mixed groups.
            criterion_group! {
                name = benches;
                config = Criterion::default()
                    .sample_size(20)
                    .measurement_time(::core::time::Duration::from_secs(5))
                    .warm_up_time(::core::time::Duration::from_secs(1));
                targets = store_benchmarks
            }
        }
    };
}
