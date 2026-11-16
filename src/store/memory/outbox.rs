//! The outbox in memory.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::future::BoxFuture;

use super::{Data, Inner, OutboxRow, conflict, ready};
use crate::store::{AppendListener, OutboxEntry, OutboxStore, StoreError, Tx};
use crate::time::after;

pub(super) struct MemoryOutbox(pub(super) Arc<Inner>);

impl OutboxRow {
    fn pending(&self) -> bool {
        self.sent_at_ms.is_none() && self.failed_at_ms.is_none()
    }
}

/// Appends `entry` to the outbox as `tx` sees it (committed at once without one).
fn append(
    data: &mut Data,
    tx: Option<&mut super::MemoryTx>,
    entry: OutboxEntry,
) -> Result<(), StoreError> {
    crate::events::check_payload_text(&entry.type_name, &entry.payload)
        .map_err(StoreError::Failed)?;
    let overlay = tx.map(|t| &mut t.outbox);
    if data.outbox.get(overlay.as_deref(), &entry.id).is_some() {
        return Err(conflict(format!(
            "duplicate key: outbox entry {} exists",
            entry.id
        )));
    }
    let id = entry.id.clone();
    let row = OutboxRow {
        entry,
        sent_at_ms: None,
        failed_at_ms: None,
        owner: None,
        lease_until_ms: None,
    };
    data.outbox.set(overlay, &mut data.version, &id, Some(row));
    Ok(())
}

impl MemoryOutbox {
    /// Changes the committed entry `id`, if any.
    fn change(&self, id: &str, change: impl FnOnce(&mut OutboxRow)) {
        let mut data = self.0.data();
        let data = &mut *data;
        data.outbox.update(None, &mut data.version, id, change);
    }
}

impl OutboxStore for MemoryOutbox {
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx,
        entry: OutboxEntry,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.0.with_tx(tx, |data, tx| append(data, tx, entry)))
    }

    fn append_now(&self, entry: OutboxEntry) -> BoxFuture<'_, Result<(), StoreError>> {
        let appended = append(&mut self.0.data(), None, entry);
        if appended.is_ok() {
            self.0.wake();
        }
        ready(appended)
    }

    /// Called after every committed append: [`append_now`](OutboxStore::append_now) at once,
    /// [`append`](OutboxStore::append) when its transaction commits.
    fn on_append(&self, wake: Arc<dyn Fn() + Send + Sync>) -> AppendListener {
        let id = self.0.next_waker.fetch_add(1, Ordering::Relaxed);
        self.0
            .wakers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, wake);
        let inner = Arc::downgrade(&self.0);
        AppendListener::new(move || {
            if let Some(inner) = inner.upgrade() {
                inner
                    .wakers
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&id);
            }
        })
    }

    fn claim<'a>(
        &'a self,
        owner: &'a str,
        lease: Duration,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<OutboxEntry>, StoreError>> {
        let now = self.0.now_ms();
        let until = after(now, lease);
        let mut data = self.0.data();
        let data = &mut *data;
        let mut claimable: Vec<(i64, i64, String)> = data
            .outbox
            .values(None)
            .into_iter()
            .filter(|row| {
                row.pending()
                    && (row.owner.as_deref().is_none_or(|o| o == owner)
                        || row.lease_until_ms.is_some_and(|until| until <= now))
            })
            .map(|row| (row.entry.created_at_ms, row.entry.seq, row.entry.id.clone()))
            .collect();
        claimable.sort();
        let mut claimed = Vec::new();
        for (_, _, id) in claimable.into_iter().take(limit) {
            data.outbox.update(None, &mut data.version, &id, |row| {
                row.owner = Some(owner.to_string());
                row.lease_until_ms = Some(until);
                claimed.push(row.entry.clone());
            });
        }
        ready(Ok(claimed))
    }

    fn mark_sent<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<(), StoreError>> {
        let now = self.0.now_ms();
        for id in ids {
            self.change(id, |row| row.sent_at_ms = Some(now));
        }
        ready(Ok(()))
    }

    fn mark_attempt_failed<'a>(
        &'a self,
        id: &'a str,
        _error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.change(id, |row| row.entry.attempts += 1);
        ready(Ok(()))
    }

    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        _error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let now = self.0.now_ms();
        self.change(id, |row| {
            row.entry.attempts += 1;
            row.failed_at_ms = Some(now);
        });
        ready(Ok(()))
    }

    fn purge(&self, sent_before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        let mut data = self.0.data();
        let data = &mut *data;
        let gone: Vec<String> = data
            .outbox
            .values(None)
            .into_iter()
            .filter(|row| row.sent_at_ms.is_some_and(|at| at < sent_before_ms))
            .map(|row| row.entry.id.clone())
            .collect();
        for id in &gone {
            data.outbox.set(None, &mut data.version, id, None);
        }
        ready(Ok(gone.len() as u64))
    }

    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        let data = self.0.data();
        let n = data
            .outbox
            .values(None)
            .into_iter()
            .filter(|row| row.pending())
            .count();
        ready(Ok(n as u64))
    }

    fn oldest_pending(&self) -> BoxFuture<'_, Result<Option<i64>, StoreError>> {
        let data = self.0.data();
        let oldest = data
            .outbox
            .values(None)
            .into_iter()
            .filter(|row| row.pending())
            .map(|row| (row.entry.created_at_ms, row.entry.seq))
            .min()
            .map(|(at, _)| at);
        ready(Ok(oldest))
    }
}
