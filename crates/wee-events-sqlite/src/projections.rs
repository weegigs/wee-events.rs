use serde::Serialize;
use wee_events::{AggregateType, ChangeSet, EventStore as _, Renderer};

use crate::{DocumentStore, Error, SqliteEventStore};

/// Applies a projection for a single aggregate after publish.
pub async fn apply_projection<S: Default + Serialize>(
    renderer: &Renderer<S>,
    event_store: &SqliteEventStore,
    document_store: &DocumentStore,
    changeset: &ChangeSet,
    collection: &str,
) -> Result<(), Error> {
    let aggregate = event_store.load(&changeset.aggregate_id).await?;
    let entity = renderer.render(&aggregate)?;
    let document = serde_json::to_value(&entity.state)?;

    document_store
        .upsert(
            collection,
            &changeset.aggregate_id.aggregate_key,
            &entity.revision,
            &document,
        )
        .await?;

    Ok(())
}

/// Rebuilds all projections for a given aggregate type.
pub async fn rebuild_projection<S: Default + Serialize>(
    renderer: &Renderer<S>,
    event_store: &SqliteEventStore,
    document_store: &DocumentStore,
    collection: &str,
    aggregate_type: &AggregateType,
) -> Result<(), Error> {
    let aggregate_ids = event_store
        .enumerate_aggregates_by_type(aggregate_type)
        .await?;

    for aggregate_id in aggregate_ids {
        let aggregate = event_store.load(&aggregate_id).await?;

        if let Some(document) = document_store
            .get(collection, &aggregate_id.aggregate_key)
            .await?
        {
            if document.revision == *aggregate.revision() {
                continue;
            }
        }

        let entity = renderer.render(&aggregate)?;
        let document = serde_json::to_value(&entity.state)?;

        document_store
            .upsert(
                collection,
                &aggregate_id.aggregate_key,
                &entity.revision,
                &document,
            )
            .await?;
    }

    Ok(())
}
