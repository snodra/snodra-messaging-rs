//! The processed events in memory.

use std::sync::Arc;

use futures::future::BoxFuture;

use super::{Inner, Position, ready};
use crate::event_store::ReplayRange;
use crate::store::{ProcessedEventStore, ProcessedMarker, StoreError, Tx};

pub(super) struct MemoryProcessed(pub(super) Arc<Inner>);

impl ProcessedEventStore for MemoryProcessed {
    /// False when the id is recorded already, committed or in this transaction.
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        marker: &'a ProcessedMarker,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(self.0.with_tx(tx, |data, tx| {
            let overlay = tx.map(|t| &mut t.processed);
            if data.processed.get(overlay.as_deref(), &marker.id).is_some() {
                return Ok(false);
            }
            data.processed
                .set(overlay, &mut data.version, &marker.id, Some(marker.clone()));
            Ok(true)
        }))
    }

    fn exists<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.0.data().processed.get(None, id).is_some()))
    }

    fn delete_markers<'a>(
        &'a self,
        processor: &'a str,
        ranges: &'a [ReplayRange],
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        let mut data = self.0.data();
        let data = &mut *data;
        let mut deleted = 0;
        for range in ranges {
            let (from, until) = (Position::of(&range.from), Position::of(&range.until));
            let gone: Vec<String> = data
                .processed
                .values(None)
                .into_iter()
                .filter(|m| {
                    m.processor.as_deref() == Some(processor)
                        && m.source.as_deref() == Some(range.source.as_str())
                        && m.segment.as_deref() == Some(range.segment.as_str())
                        && m.position
                            .as_deref()
                            .is_some_and(|p| Position::of(p).within(&from, &until))
                })
                .map(|m| m.id.clone())
                .collect();
            for id in &gone {
                data.processed.set(None, &mut data.version, id, None);
            }
            deleted += gone.len() as u64;
        }
        ready(Ok(deleted))
    }

    fn purge(&self, before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        let mut data = self.0.data();
        let data = &mut *data;
        let gone: Vec<String> = data
            .processed
            .values(None)
            .into_iter()
            .filter(|m| m.processed_at_ms < before_ms)
            .map(|m| m.id.clone())
            .collect();
        for id in &gone {
            data.processed.set(None, &mut data.version, id, None);
        }
        ready(Ok(gone.len() as u64))
    }
}
