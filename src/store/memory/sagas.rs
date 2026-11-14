//! The saga ports in memory: the sagas, their dispatch queue and leases, the deadlines, the
//! parked failures and the saga stage's markers (kept with the processed events).

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;

use super::{Data, Inner, MemoryTx, conflict, ready};
use crate::saga::store::{
    DeadlineDocument, DeadlineStatus, DispatchKind, SagaDispatchDocument, SagaDocument,
    SagaFailureDocument,
};
use crate::store::{
    DeadlineStore, DispatchFailure, FailureStore, MarkerStore, ProcessedMarker, SagaDispatchStore,
    SagaRepository, StoreError, Tx,
};

pub(super) struct MemorySagas(pub(super) Arc<Inner>);

impl MemorySagas {
    /// Runs `f` on the data and `tx`'s writes (none: committed at once).
    fn tx<R>(
        &self,
        tx: &mut Tx,
        f: impl FnOnce(&mut Data, Option<&mut MemoryTx>) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        self.0.with_tx(tx, f)
    }

    /// Changes the committed dispatch entry `id`, if any.
    fn change_entry(&self, id: &str, change: impl FnOnce(&mut SagaDispatchDocument)) {
        let mut data = self.0.data();
        let data = &mut *data;
        data.dispatch.update(None, &mut data.version, id, change);
    }

    /// Cancels the SCHEDULED deadlines `matches` picks, as `tx` sees them.
    fn cancel_where(
        &self,
        tx: &mut Tx,
        now_ms: i64,
        matches: impl Fn(&DeadlineDocument) -> bool,
    ) -> Result<(), StoreError> {
        self.tx(tx, |data, tx| {
            let mut overlay = tx.map(|t| &mut t.deadlines);
            let ids: Vec<String> = data
                .deadlines
                .values(overlay.as_deref())
                .into_iter()
                .filter(|d| d.status == DeadlineStatus::Scheduled && matches(d))
                .map(|d| d.id.clone())
                .collect();
            for id in ids {
                data.deadlines
                    .update(overlay.as_deref_mut(), &mut data.version, &id, |d| {
                        d.status = DeadlineStatus::Cancelled;
                        d.finished_at_ms = Some(now_ms);
                    });
            }
            Ok(())
        })
    }
}

/// Neither dispatched nor failed.
fn pending(e: &SagaDispatchDocument) -> bool {
    e.dispatched_at_ms.is_none() && e.failed_at_ms.is_none()
}

/// A conflict when a live saga other than `saga` holds its start key, as `overlay` sees them.
fn check_start_key(
    data: &Data,
    overlay: Option<&super::Overlay<SagaDocument>>,
    saga: &SagaDocument,
) -> Result<(), StoreError> {
    let Some(key) = saga.start_key.as_ref().filter(|_| !saga.ended) else {
        return Ok(());
    };
    let held = data
        .sagas
        .values(overlay)
        .into_iter()
        .any(|s| s.id != saga.id && !s.ended && s.start_key.as_ref() == Some(key));
    if held {
        return Err(conflict(format!(
            "duplicate key: a live saga holds the start key {key}"
        )));
    }
    Ok(())
}

impl SagaRepository for MemorySagas {
    fn find_by_id<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDocument>, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            Ok(data.sagas.get(tx.map(|t| &t.sagas), id).cloned())
        }))
    }

    fn find_live<'a>(
        &'a self,
        tx: &'a mut Tx,
        type_name: &'a str,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<Vec<SagaDocument>, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            Ok(data
                .sagas
                .values(tx.map(|t| &t.sagas))
                .into_iter()
                .filter(|s| {
                    s.type_name == type_name
                        && !s.ended
                        && s.associations.iter().any(|a| a.k == key && a.v == value)
                })
                .cloned()
                .collect())
        }))
    }

    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.tx(tx, |data, tx| {
            let overlay = tx.map(|t| &mut t.sagas);
            if data.sagas.get(overlay.as_deref(), &saga.id).is_some() {
                return Err(conflict(format!("duplicate key: saga {} exists", saga.id)));
            }
            check_start_key(data, overlay.as_deref(), saga)?;
            data.sagas
                .set(overlay, &mut data.version, &saga.id, Some(saga.clone()));
            Ok(())
        }))
    }

    fn update<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
        expected_version: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            let overlay = tx.map(|t| &mut t.sagas);
            let current = data.sagas.get(overlay.as_deref(), &saga.id);
            if current.is_none_or(|s| s.version != expected_version) {
                return Ok(false);
            }
            check_start_key(data, overlay.as_deref(), saga)?;
            data.sagas
                .set(overlay, &mut data.version, &saga.id, Some(saga.clone()));
            Ok(true)
        }))
    }

    fn count_live_by_type(&self) -> BoxFuture<'_, Result<HashMap<String, u64>, StoreError>> {
        let data = self.0.data();
        let mut counts = HashMap::new();
        for saga in data.sagas.values(None).into_iter().filter(|s| !s.ended) {
            *counts.entry(saga.type_name.clone()).or_insert(0) += 1;
        }
        ready(Ok(counts))
    }
}

impl DeadlineStore for MemorySagas {
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        deadline: &'a DeadlineDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.tx(tx, |data, tx| {
            let overlay = tx.map(|t| &mut t.deadlines);
            if data
                .deadlines
                .get(overlay.as_deref(), &deadline.id)
                .is_some()
            {
                return Err(conflict(format!(
                    "duplicate key: deadline {} exists",
                    deadline.id
                )));
            }
            data.deadlines.set(
                overlay,
                &mut data.version,
                &deadline.id,
                Some(deadline.clone()),
            );
            Ok(())
        }))
    }

    fn find_scheduled<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            Ok(data
                .deadlines
                .get(tx.map(|t| &t.deadlines), id)
                .filter(|d| d.status == DeadlineStatus::Scheduled)
                .cloned())
        }))
    }

    fn cancel<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        deadline_id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.cancel_where(tx, now_ms, |d| d.saga_id == saga_id && d.id == deadline_id))
    }

    fn cancel_by_name<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        name: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.cancel_where(tx, now_ms, |d| d.saga_id == saga_id && d.name == name))
    }

    fn cancel_all<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.cancel_where(tx, now_ms, |d| d.saga_id == saga_id))
    }

    fn mark_fired<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            let overlay = tx.map(|t| &mut t.deadlines);
            let scheduled = data
                .deadlines
                .get(overlay.as_deref(), id)
                .is_some_and(|d| d.status == DeadlineStatus::Scheduled);
            if scheduled {
                data.deadlines.update(overlay, &mut data.version, id, |d| {
                    d.status = DeadlineStatus::Fired;
                    d.finished_at_ms = Some(now_ms);
                });
            }
            Ok(scheduled)
        }))
    }

    fn claim_due<'a>(
        &'a self,
        owner: &'a str,
        now_ms: i64,
        claimed_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>> {
        let mut data = self.0.data();
        let data = &mut *data;
        let due = data
            .deadlines
            .values(None)
            .into_iter()
            .filter(|d| {
                d.status == DeadlineStatus::Scheduled
                    && d.due_at_ms <= now_ms
                    && d.claimed_until_ms.is_none_or(|until| until < now_ms)
            })
            .min_by(|a, b| (a.due_at_ms, &a.id).cmp(&(b.due_at_ms, &b.id)))
            .map(|d| d.id.clone());
        let mut claimed = None;
        if let Some(id) = due {
            data.deadlines.update(None, &mut data.version, &id, |d| {
                d.claimed_by = Some(owner.to_string());
                d.claimed_until_ms = Some(claimed_until_ms);
                claimed = Some(d.clone());
            });
        }
        ready(Ok(claimed))
    }

    fn count_overdue_by_name(
        &self,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<HashMap<String, u64>, StoreError>> {
        let data = self.0.data();
        let mut counts = HashMap::new();
        for d in data
            .deadlines
            .values(None)
            .into_iter()
            .filter(|d| d.status == DeadlineStatus::Scheduled && d.due_at_ms <= now_ms)
        {
            *counts.entry(d.name.clone()).or_insert(0) += 1;
        }
        ready(Ok(counts))
    }
}

impl FailureStore for MemorySagas {
    fn find<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaFailureDocument>, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            Ok(data.failures.get(tx.map(|t| &t.failures), id).cloned())
        }))
    }

    fn save<'a>(
        &'a self,
        tx: &'a mut Tx,
        failure: &'a SagaFailureDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.tx(tx, |data, tx| {
            data.failures.set(
                tx.map(|t| &mut t.failures),
                &mut data.version,
                &failure.id,
                Some(failure.clone()),
            );
            Ok(())
        }))
    }

    fn delete<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            let overlay = tx.map(|t| &mut t.failures);
            let found = data.failures.get(overlay.as_deref(), id).is_some();
            if found {
                data.failures.set(overlay, &mut data.version, id, None);
            }
            Ok(found)
        }))
    }

    fn failed_again<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        error: &'a str,
        attempts: i32,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            Ok(data
                .failures
                .update(tx.map(|t| &mut t.failures), &mut data.version, id, |f| {
                    f.attempts += attempts;
                    f.error = error.to_string();
                    f.last_failed_at_ms = now_ms;
                }))
        }))
    }

    fn list<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<SagaFailureDocument>, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            let mut found: Vec<SagaFailureDocument> = data
                .failures
                .values(tx.map(|t| &t.failures))
                .into_iter()
                .filter(|f| saga_type.is_none_or(|t| f.saga_type == t))
                .cloned()
                .collect();
            found.sort_by_key(|f| f.last_failed_at_ms);
            Ok(found)
        }))
    }
}

impl MarkerStore for MemorySagas {
    fn exists<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(self.tx(tx, |data, tx| {
            Ok(data.processed.get(tx.map(|t| &t.processed), id).is_some())
        }))
    }

    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        type_name: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.tx(tx, |data, tx| {
            let overlay = tx.map(|t| &mut t.processed);
            if data.processed.get(overlay.as_deref(), id).is_some() {
                return Err(conflict(format!("duplicate key: marker {id} exists")));
            }
            let marker = ProcessedMarker {
                id: id.to_string(),
                type_name: type_name.to_string(),
                processed_at_ms: now_ms,
                processor: None,
                source: None,
                segment: None,
                position: None,
            };
            data.processed
                .set(overlay, &mut data.version, id, Some(marker));
            Ok(())
        }))
    }
}

impl SagaDispatchStore for MemorySagas {
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        entries: &'a [SagaDispatchDocument],
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.tx(tx, |data, tx| {
            for e in entries.iter().filter(|e| e.kind == DispatchKind::Event) {
                crate::events::check_payload_text(&e.type_name, e.payload.as_bytes())
                    .map_err(StoreError::Failed)?;
            }
            let mut overlay = tx.map(|t| &mut t.dispatch);
            if let Some(e) = entries
                .iter()
                .find(|e| data.dispatch.get(overlay.as_deref(), &e.id).is_some())
            {
                return Err(conflict(format!(
                    "duplicate key: dispatch entry {} exists",
                    e.id
                )));
            }
            for e in entries {
                data.dispatch.set(
                    overlay.as_deref_mut(),
                    &mut data.version,
                    &e.id,
                    Some(e.clone()),
                );
            }
            Ok(())
        }))
    }

    fn due_sagas<'a>(
        &'a self,
        now_ms: i64,
        except: &'a [String],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<String>, StoreError>> {
        let data = self.0.data();
        let mut first: HashMap<&str, &SagaDispatchDocument> = HashMap::new();
        for e in data
            .dispatch
            .values(None)
            .into_iter()
            .filter(|e| pending(e) && !except.contains(&e.saga_id))
        {
            let slot = first.entry(e.saga_id.as_str()).or_insert(e);
            if e.seq < slot.seq {
                *slot = e;
            }
        }
        let mut due: Vec<&SagaDispatchDocument> = first
            .into_values()
            .filter(|e| e.next_attempt_at_ms.is_none_or(|next| next <= now_ms))
            .collect();
        due.sort_by(|a, b| (a.created_at_ms, &a.saga_id).cmp(&(b.created_at_ms, &b.saga_id)));
        ready(Ok(due
            .into_iter()
            .take(limit)
            .map(|e| e.saga_id.clone())
            .collect()))
    }

    fn next_pending<'a>(
        &'a self,
        saga_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>> {
        let data = self.0.data();
        ready(Ok(data
            .dispatch
            .values(None)
            .into_iter()
            .filter(|e| e.saga_id == saga_id && pending(e))
            .min_by_key(|e| e.seq)
            .cloned()))
    }

    fn mark_dispatched<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(self.tx(tx, |data, tx| {
            data.dispatch
                .update(tx.map(|t| &mut t.dispatch), &mut data.version, id, |e| {
                    e.dispatched_at_ms = Some(now_ms)
                });
            Ok(())
        }))
    }

    fn schedule_retry<'a>(
        &'a self,
        id: &'a str,
        attempts: i32,
        next_attempt_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.change_entry(id, |e| {
            e.attempts = attempts;
            e.next_attempt_at_ms = Some(next_attempt_at_ms);
        });
        ready(Ok(()))
    }

    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        failure: &'a DispatchFailure,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.change_entry(id, |e| {
            e.failed_at_ms = Some(failure.failed_at_ms);
            e.failure_reason = Some(failure.reason);
            e.failure_message = Some(failure.message.clone());
            if let Some(attempts) = failure.attempts {
                e.attempts = attempts;
            }
            if let Some(at) = failure.notice_at_ms {
                e.notice_pending = Some(true);
                e.notice_attempts = Some(0);
                e.notice_next_at_ms = Some(at);
            }
        });
        ready(Ok(()))
    }

    fn claim_due_notice<'a>(
        &'a self,
        now_ms: i64,
        reserve_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>> {
        let mut data = self.0.data();
        let data = &mut *data;
        let due = data
            .dispatch
            .values(None)
            .into_iter()
            .filter(|e| {
                e.notice_pending == Some(true) && e.notice_next_at_ms.is_none_or(|at| at <= now_ms)
            })
            .min_by(|a, b| (a.notice_next_at_ms, &a.id).cmp(&(b.notice_next_at_ms, &b.id)))
            .map(|e| e.id.clone());
        let mut claimed = None;
        if let Some(id) = due {
            data.dispatch.update(None, &mut data.version, &id, |e| {
                e.notice_next_at_ms = Some(reserve_until_ms);
                claimed = Some(e.clone());
            });
        }
        ready(Ok(claimed))
    }

    fn clear_notice<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        self.change_entry(id, |e| {
            e.notice_pending = None;
            e.notice_attempts = None;
            e.notice_next_at_ms = None;
        });
        ready(Ok(()))
    }

    fn schedule_notice_retry<'a>(
        &'a self,
        id: &'a str,
        notice_attempts: i32,
        next_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.change_entry(id, |e| {
            e.notice_attempts = Some(notice_attempts);
            e.notice_next_at_ms = Some(next_at_ms);
        });
        ready(Ok(()))
    }

    fn claim_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        now_ms: i64,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.0.data();
        let free = data
            .leases
            .get(saga_id)
            .is_none_or(|(_, until)| *until < now_ms);
        if free {
            data.leases
                .insert(saga_id.to_string(), (owner.to_string(), until_ms));
        }
        ready(Ok(free))
    }

    fn renew_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.0.data();
        let renewed = match data.leases.get_mut(saga_id) {
            Some((holder, until)) if holder == owner => {
                *until = until_ms;
                true
            }
            _ => false,
        };
        ready(Ok(renewed))
    }

    fn release_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let mut data = self.0.data();
        if data
            .leases
            .get(saga_id)
            .is_some_and(|(holder, _)| holder == owner)
        {
            data.leases.remove(saga_id);
        }
        ready(Ok(()))
    }

    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        let data = self.0.data();
        let n = data
            .dispatch
            .values(None)
            .into_iter()
            .filter(|e| pending(e))
            .count();
        ready(Ok(n as u64))
    }
}
