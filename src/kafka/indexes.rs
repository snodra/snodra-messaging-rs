//! The outbox relay index and the TTL indexes of `outbox` and `processed_events`. Idempotent.

use std::time::Duration;

use mongodb::bson::{Document, doc};
use mongodb::options::IndexOptions;
use mongodb::{Database, IndexModel};

use super::{OUTBOX_COLLECTION, PROCESSED_EVENTS_COLLECTION};
use crate::error::MessagingError;

pub async fn ensure_indexes(
    db: &Database,
    outbox_retention: Duration,
    idempotency_retention: Duration,
) -> Result<(), MessagingError> {
    let outbox = db.collection::<Document>(OUTBOX_COLLECTION);
    outbox
        .create_index(
            IndexModel::builder()
                .keys(doc! { "sentAt": 1, "createdAt": 1 })
                .build(),
        )
        .await?;
    // Documents with sentAt == null are never expired by the TTL monitor.
    outbox
        .create_index(
            IndexModel::builder()
                .keys(doc! { "sentAt": 1 })
                .options(
                    IndexOptions::builder()
                        .name("sentAt_ttl".to_string())
                        .expire_after(outbox_retention)
                        .build(),
                )
                .build(),
        )
        .await?;
    db.collection::<Document>(PROCESSED_EVENTS_COLLECTION)
        .create_index(
            IndexModel::builder()
                .keys(doc! { "processedAt": 1 })
                .options(
                    IndexOptions::builder()
                        .name("processedAt_ttl".to_string())
                        .expire_after(idempotency_retention)
                        .build(),
                )
                .build(),
        )
        .await?;
    Ok(())
}
