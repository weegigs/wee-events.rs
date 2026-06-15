use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use wee_events::{
    AggregateId, EventData, EventStore, EventType, PublishOptions, RawEvent, memory::MemoryStore,
};

#[derive(Serialize, Deserialize, Clone)]
struct FatProfileUpdate {
    user_id: String,
    event_seq: u64,
    timestamp_ms: u64,
    session_id: String,
    correlation_id: String,
    source_ip: String,
    user_agent: String,
    locale: String,
    region: String,
    schema_version: u32,
    blob: String,
}

fn make_fat(blob_size: usize, seq: u64) -> FatProfileUpdate {
    let blob: String = std::iter::repeat_n('x', blob_size).collect();
    FatProfileUpdate {
        user_id: format!("user-{seq:08}"),
        event_seq: seq,
        timestamp_ms: 1_700_000_000_000 + seq,
        session_id: format!("session-{seq:016x}"),
        correlation_id: format!("corr-{seq:016x}"),
        source_ip: "203.0.113.42".into(),
        user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0)".into(),
        locale: "en-US".into(),
        region: "us-west-2".into(),
        schema_version: 7,
        blob,
    }
}

fn make_raw(blob_size: usize, seq: u64) -> RawEvent {
    let event = make_fat(blob_size, seq);
    RawEvent {
        event_type: EventType::new("user:profile-updated"),
        data: EventData::json(&event).expect("json encode should succeed"),
    }
}

fn bench_publish(c: &mut Criterion, rt: &Runtime, size_label: &str, blob_size: usize) {
    let mut group = c.benchmark_group(format!("fat_payload/{size_label}/publish"));
    group.throughput(Throughput::Bytes(blob_size as u64));
    group.sample_size(20);

    group.bench_function("single", |b| {
        b.to_async(rt).iter_batched(
            || {
                let id = AggregateId::new("user", ulid::Ulid::new().to_string());
                let raw = make_raw(blob_size, 1);
                let store = MemoryStore::new();
                (store, id, raw)
            },
            |(store, id, raw)| async move {
                store
                    .publish(&id, PublishOptions::default(), vec![raw])
                    .await
                    .expect("publish should succeed");
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_load(c: &mut Criterion, rt: &Runtime, size_label: &str, blob_size: usize) {
    let mut group = c.benchmark_group(format!("fat_payload/{size_label}/load"));
    group.throughput(Throughput::Bytes(blob_size as u64));
    group.sample_size(20);

    let store = rt.block_on(async {
        let store = MemoryStore::new();
        let id = AggregateId::new("user", "load-target");
        store
            .publish(&id, PublishOptions::default(), vec![make_raw(blob_size, 1)])
            .await
            .expect("seed should succeed");
        Arc::new(store)
    });
    let id = AggregateId::new("user", "load-target");

    group.bench_function("load_decode", |b| {
        let store = Arc::clone(&store);
        let id = id.clone();
        b.to_async(rt).iter(|| {
            let store = Arc::clone(&store);
            let id = id.clone();
            async move {
                let agg = store.load(&id).await.expect("load should succeed");
                let event = &agg.events()[0];
                let _decoded: FatProfileUpdate = event
                    .data
                    .deserialize_json()
                    .expect("decode should succeed");
            }
        });
    });
    group.finish();
}

fn bench_encode(c: &mut Criterion, size_label: &str, blob_size: usize) {
    let mut group = c.benchmark_group(format!("fat_payload/{size_label}/encode_only"));
    group.throughput(Throughput::Bytes(blob_size as u64));
    group.sample_size(20);

    group.bench_function("event_data_json", |b| {
        b.iter_batched(
            || make_fat(blob_size, 1),
            |event| {
                let _data = EventData::json(&event).expect("encode");
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_decode(c: &mut Criterion, size_label: &str, blob_size: usize) {
    let mut group = c.benchmark_group(format!("fat_payload/{size_label}/decode_only"));
    group.throughput(Throughput::Bytes(blob_size as u64));
    group.sample_size(20);

    let data = EventData::json(&make_fat(blob_size, 1)).expect("encode");

    group.bench_function("deserialize_json", |b| {
        b.iter(|| {
            let _decoded: FatProfileUpdate = data.deserialize_json().expect("decode");
        });
    });
    group.finish();
}

fn bench_all(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    bench_encode(c, "1mb", 1024 * 1024);
    bench_decode(c, "1mb", 1024 * 1024);
    bench_publish(c, &rt, "1mb", 1024 * 1024);
    bench_load(c, &rt, "1mb", 1024 * 1024);

    bench_encode(c, "5mb", 5 * 1024 * 1024);
    bench_decode(c, "5mb", 5 * 1024 * 1024);
    bench_publish(c, &rt, "5mb", 5 * 1024 * 1024);
    bench_load(c, &rt, "5mb", 5 * 1024 * 1024);
}

criterion_group!(benches, bench_all);
criterion_main!(benches);
