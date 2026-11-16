//! The indexes of every collection of the store (idempotent). The names are the ones the library
//! has always given them, so a database indexed by either library is accepted by the other.

use std::time::Duration;

use kraken_messaging::Config;
use kraken_messaging::store::StoreError;
use mongodb::bson::{Document, doc};
use mongodb::options::IndexOptions;
use mongodb::{Database, IndexModel};

use crate::OrStoreError;
use crate::dead_letters::DEAD_LETTERS_COLLECTION;
use crate::external::EXTERNAL_CALLS_COLLECTION;
use crate::outbox::OUTBOX_COLLECTION;
use crate::processed::PROCESSED_EVENTS_COLLECTION;
use crate::sagas::{
    DEADLINES_COLLECTION, SAGA_DISPATCH_COLLECTION, SAGA_DISPATCH_LEASES_COLLECTION,
    SAGA_FAILURES_COLLECTION, SAGAS_COLLECTION,
};

/// How long the TTL indexes keep what is done with. A document whose date is absent is never
/// expired (an unsent outbox entry, a live saga, a scheduled deadline).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongoIndexSettings {
    /// Sent outbox entries (`sentAt`); default 7 days.
    pub outbox_retention: Duration,
    /// Processed-event markers (`processedAt`); default 30 days.
    pub idempotency_retention: Duration,
    /// External call records (`updatedAt`); default 30 days.
    pub external_calls_retention: Duration,
    /// Ended sagas (`endedAt`); default 30 days.
    pub sagas_ended_retention: Duration,
    /// Dispatched saga entries (`dispatchedAt`); default 7 days.
    pub sagas_dispatch_retention: Duration,
    /// Saga entries given up on (`failedAt`) and parked saga events (`lastFailedAt`); default 30
    /// days.
    pub sagas_failed_retention: Duration,
    /// Fired or cancelled deadlines (`finishedAt`); default 7 days.
    pub deadlines_retention: Duration,
}

const DAY: Duration = Duration::from_secs(86_400);

/// The retentions of the service's [`Config`] (`KRAKEN_*_RETENTION_MS`).
impl From<&Config> for MongoIndexSettings {
    fn from(config: &Config) -> Self {
        MongoIndexSettings {
            outbox_retention: config.outbox_retention,
            idempotency_retention: config.idempotency_retention,
            external_calls_retention: config.external_calls_retention,
            sagas_ended_retention: config.sagas_ended_retention,
            sagas_dispatch_retention: config.sagas_dispatch_retention,
            sagas_failed_retention: config.sagas_failed_retention,
            deadlines_retention: config.deadlines_retention,
        }
    }
}

impl Default for MongoIndexSettings {
    fn default() -> Self {
        MongoIndexSettings {
            outbox_retention: DAY * 7,
            idempotency_retention: DAY * 30,
            external_calls_retention: DAY * 30,
            sagas_ended_retention: DAY * 30,
            sagas_dispatch_retention: DAY * 7,
            sagas_failed_retention: DAY * 30,
            deadlines_retention: DAY * 7,
        }
    }
}

fn index(keys: Document, name: &str, configure: impl FnOnce(&mut IndexOptions)) -> IndexModel {
    let mut options = IndexOptions::builder().name(name.to_string()).build();
    configure(&mut options);
    IndexModel::builder().keys(keys).options(options).build()
}

fn ttl(after: Duration) -> impl FnOnce(&mut IndexOptions) {
    move |o| o.expire_after = Some(after)
}

pub(crate) async fn ensure_indexes(
    db: &Database,
    settings: &MongoIndexSettings,
) -> Result<(), StoreError> {
    let create = |collection: &str, indexes: Vec<IndexModel>| {
        let collection = db.collection::<Document>(collection);
        async move { collection.create_indexes(indexes).await.store() }
    };

    // What the relay's claim matches (sentAt unset) and sorts by.
    create(
        OUTBOX_COLLECTION,
        vec![
            index(
                doc! { "sentAt": 1, "createdAt": 1, "seq": 1 },
                "sentAt_1_createdAt_1_seq_1",
                |_| {},
            ),
            index(
                doc! { "sentAt": 1 },
                "sentAt_ttl",
                ttl(settings.outbox_retention),
            ),
        ],
    )
    .await?;
    create(
        PROCESSED_EVENTS_COLLECTION,
        vec![index(
            doc! { "processedAt": 1 },
            "processedAt_ttl",
            ttl(settings.idempotency_retention),
        )],
    )
    .await?;
    create(
        EXTERNAL_CALLS_COLLECTION,
        vec![index(
            doc! { "updatedAt": 1 },
            "updatedAt_ttl",
            ttl(settings.external_calls_retention),
        )],
    )
    .await?;

    // Sequencing and retry order, listing, and the TTL on `expireAt` (an expired letter
    // disappears and unblocks its key).
    create(
        DEAD_LETTERS_COLLECTION,
        vec![
            index(
                doc! { "processor": 1, "key": 1, "offset": 1 },
                "dead_letters_processor_key_offset",
                |_| {},
            ),
            index(
                doc! { "processor": 1, "parkedAt": 1 },
                "dead_letters_processor_parked",
                |_| {},
            ),
            index(
                doc! { "expireAt": 1 },
                "dead_letters_expire",
                ttl(Duration::ZERO),
            ),
        ],
    )
    .await?;

    // The association and start-key indexes are partial on `ended: false`, so ended instances
    // neither route events nor hold a start key.
    create(
        SAGAS_COLLECTION,
        vec![
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
            index(
                doc! { "endedAt": 1 },
                "endedAt_ttl",
                ttl(settings.sagas_ended_retention),
            ),
        ],
    )
    .await?;
    create(
        SAGA_DISPATCH_COLLECTION,
        vec![
            index(doc! { "sagaId": 1, "seq": 1 }, "sagaId_1_seq_1", |_| {}),
            // What the relay's due-saga aggregation matches (dispatchedAt and failedAt unset) and
            // sorts by (sagaId, seq).
            index(
                doc! { "dispatchedAt": 1, "failedAt": 1, "sagaId": 1, "seq": 1 },
                "dispatchedAt_1_failedAt_1_sagaId_1_seq_1",
                |_| {},
            ),
            index(
                doc! { "dispatchedAt": 1 },
                "dispatchedAt_ttl",
                ttl(settings.sagas_dispatch_retention),
            ),
            index(
                doc! { "failedAt": 1 },
                "failedAt_ttl",
                ttl(settings.sagas_failed_retention),
            ),
            index(
                doc! { "noticeNextAt": 1 },
                "noticePending_noticeNextAt",
                |o| o.partial_filter_expression = Some(doc! { "noticePending": true }),
            ),
        ],
    )
    .await?;
    create(
        SAGA_DISPATCH_LEASES_COLLECTION,
        vec![index(doc! { "until": 1 }, "until_ttl", ttl(Duration::ZERO))],
    )
    .await?;
    create(
        DEADLINES_COLLECTION,
        vec![
            index(doc! { "status": 1, "dueAt": 1 }, "status_1_dueAt_1", |_| {}),
            index(
                doc! { "sagaId": 1, "status": 1 },
                "sagaId_1_status_1",
                |_| {},
            ),
            index(
                doc! { "finishedAt": 1 },
                "finishedAt_ttl",
                ttl(settings.deadlines_retention),
            ),
        ],
    )
    .await?;
    create(
        SAGA_FAILURES_COLLECTION,
        vec![
            index(
                doc! { "sagaType": 1, "lastFailedAt": 1 },
                "sagaType_1_lastFailedAt_1",
                |_| {},
            ),
            index(
                doc! { "lastFailedAt": 1 },
                "lastFailedAt_ttl",
                ttl(settings.sagas_failed_retention),
            ),
        ],
    )
    .await?;
    Ok(())
}
