use serde::Serialize;
use wee_events::{AggregateType, ChangeSet, EventStore, Renderer};

use crate::{Error, SqliteStore};

/// Applies a projection for a single aggregate after publish. Loads the full
/// aggregate, renders to projected state, and upserts the result to the
/// document store.
///
/// Called inline by the owning actor after each `publish()`. The full
/// load+render is correct and simple — for aggregates with 5-30 events,
/// incremental application is premature optimization.
///
/// Each collection should map to exactly one aggregate type. If two
/// different aggregate types share the same `aggregate_key`, they would
/// collide within the same collection.
pub async fn apply_projection<S: Default + Serialize>(
    renderer: &Renderer<S>,
    store: &SqliteStore,
    changeset: &ChangeSet,
    collection: &str,
) -> Result<(), Error> {
    let aggregate = store.events().load(&changeset.aggregate_id).await?;
    let entity = renderer.render(&aggregate)?;
    let doc = serde_json::to_value(&entity.state).map_err(|e| Error::Data(e.to_string()))?;
    store.documents().upsert(
        collection,
        &changeset.aggregate_id.aggregate_key,
        &entity.revision,
        &doc,
    )?;
    Ok(())
}

/// Rebuilds all projections for a given aggregate type. Called once on actor
/// startup to catch up any stale or missing documents.
///
/// For each aggregate of the given type:
///   - If the document exists and its revision matches the aggregate's latest,
///     skip (already current).
///   - Otherwise, load the full event stream, render, and upsert.
///
/// The revision-aware upsert provides an additional safety net: even if the
/// comparison is somehow stale, the SQL WHERE clause prevents overwriting a
/// newer document with an older projection.
///
/// Revisions are ULID strings (lexicographically monotonic). The stale-write
/// prevention in `DocumentStore::upsert` depends on this ordering property.
pub async fn rebuild_projection<S: Default + Serialize>(
    renderer: &Renderer<S>,
    store: &SqliteStore,
    collection: &str,
    aggregate_type: &AggregateType,
) -> Result<(), Error> {
    let ids = store
        .events()
        .enumerate_aggregates_by_type(aggregate_type)?;

    for id in ids {
        let aggregate = store.events().load(&id).await?;

        // Skip if document is already current
        if let Some(doc) = store.documents().get(collection, &id.aggregate_key)? {
            if doc.revision == *aggregate.revision() {
                continue;
            }
        }

        let entity = renderer.render(&aggregate)?;
        let doc = serde_json::to_value(&entity.state).map_err(|e| Error::Data(e.to_string()))?;
        store
            .documents()
            .upsert(collection, &id.aggregate_key, &entity.revision, &doc)?;
    }

    Ok(())
}
