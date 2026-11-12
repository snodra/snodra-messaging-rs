//! The indexes of the saga collections (ADR 0021's storage contract) and of `external_calls`.
//! Idempotent; the names are the Kotlin ones, so a database indexed by either side is accepted by
//! the other.

use std::time::Duration;

use mongodb::bson::{Document, doc};
use mongodb::options::IndexOptions;
use mongodb::{Database, IndexModel};

use super::store::{
    DEADLINES_COLLECTION, EXTERNAL_CALLS_COLLECTION, SAGA_DISPATCH_COLLECTION,
    SAGA_DISPATCH_LEASES_COLLECTION, SAGA_FAILURES_COLLECTION, SAGAS_COLLECTION,
};
use crate::error::MessagingError;

fn index(keys: Document, name: &str, configure: impl FnOnce(&mut IndexOptions)) -> IndexModel {
    let mut options = IndexOptions::builder().name(name.to_string()).build();
    configure(&mut options);
    IndexModel::builder().keys(keys).options(options).build()
}

/// Ensures the indexes of `sagas`, `saga_dispatch`, `saga_dispatch_leases`, `deadlines` and
/// `saga_failures`. The association and start-key indexes are partial on `ended: false`, so ended
/// instances neither route events nor hold a start key; the TTL indexes expire ended sagas
/// (`ended_retention`, on `endedAt`), dispatched entries (`dispatch_retention`, on
/// `dispatchedAt`), what failed for good (`failed_retention`: entries given up on, on
/// `failedAt`, and parked events, on `lastFailedAt`), fired or cancelled deadlines
/// (`deadline_retention`, on `finishedAt`) and, at once, expired dispatch leases. A document whose
/// date is absent is never expired.
pub async fn ensure_saga_indexes(
    db: &Database,
    ended_retention: Duration,
    dispatch_retention: Duration,
    deadline_retention: Duration,
    failed_retention: Duration,
) -> Result<(), MessagingError> {
    let sagas = db.collection::<Document>(SAGAS_COLLECTION);
    sagas
        .create_indexes([
            index(
                doc! { "type": 1, "associations.k": 1, "associations.v": 1 },
                "type_associations_live",
                |o| o.partial_filter_expression = Some(doc! { "ended": false }),
            ),
            index(doc! { "startKey": 1 }, "startKey_live_unique", |o| {
                o.unique = Some(true);
                o.partial_filter_expression =
                    Some(doc! { "ended": false, "startKey": { "$exists": true } });
            }),
            index(doc! { "endedAt": 1 }, "endedAt_ttl", |o| {
                o.expire_after = Some(ended_retention)
            }),
        ])
        .await?;

    let dispatch = db.collection::<Document>(SAGA_DISPATCH_COLLECTION);
    dispatch
        .create_indexes([
            index(doc! { "sagaId": 1, "seq": 1 }, "sagaId_1_seq_1", |_| {}),
            // What the relay's due-saga aggregation matches (dispatchedAt and failedAt unset) and
            // sorts by (sagaId, seq).
            index(
                doc! { "dispatchedAt": 1, "failedAt": 1, "sagaId": 1, "seq": 1 },
                "dispatchedAt_1_failedAt_1_sagaId_1_seq_1",
                |_| {},
            ),
            index(doc! { "dispatchedAt": 1 }, "dispatchedAt_ttl", |o| {
                o.expire_after = Some(dispatch_retention)
            }),
            index(doc! { "failedAt": 1 }, "failedAt_ttl", |o| {
                o.expire_after = Some(failed_retention)
            }),
            index(
                doc! { "noticeNextAt": 1 },
                "noticePending_noticeNextAt",
                |o| o.partial_filter_expression = Some(doc! { "noticePending": true }),
            ),
        ])
        .await?;

    db.collection::<Document>(SAGA_DISPATCH_LEASES_COLLECTION)
        .create_index(index(doc! { "until": 1 }, "until_ttl", |o| {
            o.expire_after = Some(Duration::ZERO)
        }))
        .await?;

    db.collection::<Document>(DEADLINES_COLLECTION)
        .create_indexes([
            index(doc! { "status": 1, "dueAt": 1 }, "status_1_dueAt_1", |_| {}),
            index(
                doc! { "sagaId": 1, "status": 1 },
                "sagaId_1_status_1",
                |_| {},
            ),
            index(doc! { "finishedAt": 1 }, "finishedAt_ttl", |o| {
                o.expire_after = Some(deadline_retention)
            }),
        ])
        .await?;

    db.collection::<Document>(SAGA_FAILURES_COLLECTION)
        .create_indexes([
            index(
                doc! { "sagaType": 1, "lastFailedAt": 1 },
                "sagaType_1_lastFailedAt_1",
                |_| {},
            ),
            index(doc! { "lastFailedAt": 1 }, "lastFailedAt_ttl", |o| {
                o.expire_after = Some(failed_retention)
            }),
        ])
        .await?;
    Ok(())
}

/// Ensures the TTL index of `external_calls` (on `updatedAt`, after `retention`).
pub async fn ensure_external_call_indexes(
    db: &Database,
    retention: Duration,
) -> Result<(), MessagingError> {
    db.collection::<Document>(EXTERNAL_CALLS_COLLECTION)
        .create_index(index(doc! { "updatedAt": 1 }, "updatedAt_ttl", |o| {
            o.expire_after = Some(retention)
        }))
        .await?;
    Ok(())
}
