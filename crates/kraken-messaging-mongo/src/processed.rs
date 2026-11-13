//! The `processed_events` collection: `{_id, type, processedAt}`, plus `processor`, `topic`,
//! `partition` (an Int32) and `offset` (an Int64) on a stage-1 marker of a consumed record.

use futures::future::BoxFuture;
use kraken_messaging::store::{ProcessedEventStore, ProcessedMarker, ReplayRange, StoreError, Tx};
use mongodb::bson::{Document, doc};
use mongodb::{Collection, Database};

use crate::{OrStoreError, date, is_duplicate_key, session, store_error};

pub(crate) const PROCESSED_EVENTS_COLLECTION: &str = "processed_events";

pub(crate) struct MongoProcessedEventStore {
    markers: Collection<Document>,
}

impl MongoProcessedEventStore {
    pub(crate) fn new(db: &Database) -> MongoProcessedEventStore {
        MongoProcessedEventStore {
            markers: db.collection(PROCESSED_EVENTS_COLLECTION),
        }
    }
}

fn marker_document(marker: &ProcessedMarker) -> Document {
    let mut doc = doc! {
        "_id": &marker.id,
        "type": &marker.type_name,
        "processedAt": date(marker.processed_at_ms),
    };
    if let Some(processor) = &marker.processor {
        doc.insert("processor", processor);
    }
    if let Some(topic) = &marker.topic {
        doc.insert("topic", topic);
    }
    if let Some(partition) = marker.partition {
        doc.insert("partition", partition);
    }
    if let Some(offset) = marker.offset {
        doc.insert("offset", offset);
    }
    doc
}

impl ProcessedEventStore for MongoProcessedEventStore {
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        marker: &'a ProcessedMarker,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            match self
                .markers
                .insert_one(marker_document(marker))
                .session(session(tx)?)
                .await
            {
                Ok(_) => Ok(true),
                Err(e) if is_duplicate_key(&e) => Ok(false),
                Err(e) => Err(store_error(e)),
            }
        })
    }

    fn exists<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(self
                .markers
                .find_one(doc! { "_id": id })
                .projection(doc! { "_id": 1 })
                .await
                .store()?
                .is_some())
        })
    }

    fn delete_markers<'a>(
        &'a self,
        processor: &'a str,
        ranges: &'a [ReplayRange],
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            let mut deleted = 0;
            for range in ranges {
                deleted += self
                    .markers
                    .delete_many(doc! {
                        "processor": processor,
                        "topic": &range.topic,
                        "partition": range.partition,
                        "offset": { "$gte": range.from, "$lt": range.until },
                    })
                    .await
                    .store()?
                    .deleted_count;
            }
            Ok(deleted)
        })
    }

    fn purge(&self, before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            Ok(self
                .markers
                .delete_many(doc! { "processedAt": { "$lt": date(before_ms) } })
                .await
                .store()?
                .deleted_count)
        })
    }
}
