//! The dead letters in memory. A letter whose `expire_at_ms` has passed (on the store's own
//! clock, as a database's TTL) is removed before any call reads the letters.

use std::collections::HashSet;
use std::sync::{Arc, MutexGuard};

use futures::future::BoxFuture;

use super::{Data, Inner, Position, ready};
use crate::store::{
    DeadLetter, DeadLetterError, DeadLetterReason, DeadLetterStore, DeadLetterSummary, Parked,
    StoreError, Tx,
};

pub(super) struct MemoryDeadLetters(pub(super) Arc<Inner>);

impl MemoryDeadLetters {
    /// The data, without the expired letters.
    fn data(&self) -> MutexGuard<'_, Data> {
        let now = self.0.ttl_clock.now_ms();
        let mut data = self.0.data();
        let expired: Vec<String> = data
            .dead_letters
            .values(None)
            .into_iter()
            .filter(|l| l.expire_at_ms < now)
            .map(|l| l.id.clone())
            .collect();
        for id in &expired {
            let data = &mut *data;
            data.dead_letters.set(None, &mut data.version, id, None);
        }
        data
    }

    /// The id of the first letter (by position, then id) that is not INVALID among those of
    /// `processor` and `key` that `keep` keeps.
    fn first(
        &self,
        processor: &str,
        key: &str,
        keep: impl Fn(&Position) -> bool,
    ) -> Option<String> {
        let data = self.data();
        data.dead_letters
            .values(None)
            .into_iter()
            .filter(|l| {
                l.processor == processor
                    && l.key.as_deref() == Some(key)
                    && l.reason != DeadLetterReason::Invalid
            })
            .map(|l| (Position::of(&l.position), l.id.clone()))
            .filter(|(p, _)| keep(p))
            .min()
            .map(|(_, id)| id)
    }
}

impl DeadLetterStore for MemoryDeadLetters {
    fn park<'a>(&'a self, letter: &'a DeadLetter) -> BoxFuture<'a, Result<Parked, StoreError>> {
        let mut data = self.data();
        let data = &mut *data;
        if data.dead_letters.get(None, &letter.id).is_some() {
            return ready(Ok(Parked::Already));
        }
        data.dead_letters
            .set(None, &mut data.version, &letter.id, Some(letter.clone()));
        ready(Ok(Parked::New))
    }

    fn head<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
        ready(Ok(self.first(processor, key, |_| true)))
    }

    fn earlier<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
        position: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
        let bound = Position::of(position);
        ready(Ok(self.first(processor, key, |p| p.before(&bound))))
    }

    fn parked_keys<'a>(
        &'a self,
        processor: &'a str,
        source: &'a str,
        segments: &'a [String],
    ) -> BoxFuture<'a, Result<HashSet<String>, StoreError>> {
        let data = self.data();
        let keys = data
            .dead_letters
            .values(None)
            .into_iter()
            .filter(|l| {
                l.processor == processor
                    && l.source == source
                    && segments.contains(&l.segment)
                    && l.reason != DeadLetterReason::Invalid
            })
            .filter_map(|l| l.key.clone())
            .collect();
        ready(Ok(keys))
    }

    fn get<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<DeadLetter>, StoreError>> {
        ready(Ok(self.data().dead_letters.get(None, id).cloned()))
    }

    fn list<'a>(
        &'a self,
        processor: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxFuture<'a, Result<Vec<DeadLetter>, StoreError>> {
        let data = self.data();
        let mut letters: Vec<DeadLetter> = data
            .dead_letters
            .values(None)
            .into_iter()
            .filter(|l| processor.is_none_or(|p| l.processor == p))
            .cloned()
            .collect();
        letters.sort_by(|a, b| (a.parked_at_ms, &a.id).cmp(&(b.parked_at_ms, &b.id)));
        letters.truncate(limit.unwrap_or(usize::MAX));
        ready(Ok(letters))
    }

    fn summaries<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<Vec<DeadLetterSummary>, StoreError>> {
        let data = self.data();
        let mut letters: Vec<&DeadLetter> = data
            .dead_letters
            .values(None)
            .into_iter()
            .filter(|l| l.processor == processor)
            .collect();
        letters.sort_by_cached_key(|l| (Position::of(&l.position), l.id.clone()));
        let summaries = letters
            .into_iter()
            .map(|l| DeadLetterSummary {
                id: l.id.clone(),
                key: l.key.clone(),
                position: l.position.clone(),
                reason: l.reason,
                parked_at_ms: l.parked_at_ms,
            })
            .collect();
        ready(Ok(summaries))
    }

    fn count<'a>(&'a self, processor: Option<&'a str>) -> BoxFuture<'a, Result<u64, StoreError>> {
        let data = self.data();
        let n = data
            .dead_letters
            .values(None)
            .into_iter()
            .filter(|l| processor.is_none_or(|p| l.processor == p))
            .count();
        ready(Ok(n as u64))
    }

    fn delete<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.data();
        let data = &mut *data;
        let found = data.dead_letters.get(None, id).is_some();
        data.dead_letters.set(None, &mut data.version, id, None);
        ready(Ok(found))
    }

    fn delete_in<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        drop(self.data());
        ready(self.0.with_tx(tx, |data, tx| {
            let overlay = tx.map(|t| &mut t.dead_letters);
            let found = data.dead_letters.get(overlay.as_deref(), id).is_some();
            if found {
                data.dead_letters.set(overlay, &mut data.version, id, None);
            }
            Ok(found)
        }))
    }

    fn record_failure<'a>(
        &'a self,
        id: &'a str,
        error: &'a DeadLetterError,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.data();
        let data = &mut *data;
        let found = data.dead_letters.update(None, &mut data.version, id, |l| {
            l.attempts += 1;
            l.reason = DeadLetterReason::HandlerFailed;
            l.error = error.clone();
            l.last_attempt_at_ms = now_ms;
            l.blocked_by = None;
        });
        ready(Ok(found))
    }
}
