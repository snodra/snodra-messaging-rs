//! The `processed_events` collection: `{_id, type, processedAt}`, plus `processor`, `topic` (the
//! source), `partition` (the segment) and `offset` (the position) on a stage-1 marker of a
//! delivered record, the segment and position as [`positions`](crate::positions) stores them (an
//! Int32 and an Int64 for decimal ones, as always: a marker written before the event store became
//! an add-on is deleted by a replay over its offset like a new one).

use futures::future::BoxFuture;
use mongodb::bson::{Document, doc};
use mongodb::{Collection, Database};
use snodra_messaging::event_store::ReplayRange;
use snodra_messaging::store::{ProcessedEventStore, ProcessedMarker, StoreError, Tx};

use crate::{OrStoreError, date, is_duplicate_key, positions, session, store_error};

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
    if let Some(source) = &marker.source {
        doc.insert("topic", source);
    }
    if let Some(segment) = &marker.segment {
        doc.insert("partition", positions::segment(segment));
    }
    if let Some(position) = &marker.position {
        doc.insert("offset", positions::position(position));
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
                        "topic": &range.source,
                        "partition": positions::segment(&range.segment),
                        "offset": {
                            "$gte": positions::position(&range.from),
                            "$lt": positions::position(&range.until),
                        },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_decimal_marker_keeps_the_layout_another_is_stored_as_text() {
        let marker = |segment: &str, position: &str| ProcessedMarker {
            id: "processor:default:e-1".into(),
            type_name: "t.E".into(),
            processed_at_ms: 0,
            processor: Some("default".into()),
            source: Some("snodra.events".into()),
            segment: Some(segment.into()),
            position: Some(position.into()),
        };
        let stored = marker_document(&marker("2", "41"));
        assert_eq!(stored.get_str("topic"), Ok("snodra.events"));
        assert_eq!(stored.get_i32("partition"), Ok(2));
        assert_eq!(stored.get_i64("offset"), Ok(41));
        let stored = marker_document(&marker("p-0", "x7"));
        assert_eq!(stored.get_str("partition"), Ok("p-0"));
        assert_eq!(stored.get_str("offset"), Ok("x7"));
        let local = marker_document(&ProcessedMarker {
            processor: None,
            source: None,
            segment: None,
            position: None,
            ..marker("0", "0")
        });
        assert_eq!(
            local.keys().collect::<Vec<_>>(),
            ["_id", "type", "processedAt"]
        );
    }
}
