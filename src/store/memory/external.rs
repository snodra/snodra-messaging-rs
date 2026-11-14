//! The records of idempotent external calls in memory.

use std::sync::Arc;

use futures::future::BoxFuture;

use super::{Inner, ready};
use crate::store::{ExternalCall, ExternalCallStatus, ExternalCallStore, StoreError};

pub(super) struct MemoryExternalCalls(pub(super) Arc<Inner>);

impl MemoryExternalCalls {
    /// Changes the record of `key` when it `matches`; the record as written.
    fn update(
        &self,
        key: &str,
        matches: impl FnOnce(&ExternalCall) -> bool,
        change: impl FnOnce(&mut ExternalCall),
    ) -> Option<ExternalCall> {
        let mut data = self.0.data();
        let call = data.external.get_mut(key).filter(|c| matches(c))?;
        change(call);
        Some(call.clone())
    }

    /// Settles the record `owner` owns as `status`, without owner or lease.
    fn settle(
        &self,
        key: &str,
        owner: &str,
        status: ExternalCallStatus,
        now_ms: i64,
        change: impl FnOnce(&mut ExternalCall),
    ) -> bool {
        self.update(
            key,
            |c| owns(c, owner),
            |c| {
                c.status = status;
                c.updated_at_ms = now_ms;
                c.owner = None;
                c.lease_until_ms = None;
                change(c);
            },
        )
        .is_some()
    }
}

/// The record is in progress and `owner`'s.
fn owns(call: &ExternalCall, owner: &str) -> bool {
    call.status == ExternalCallStatus::InProgress && call.owner.as_deref() == Some(owner)
}

/// The record is still the in-progress one `seen` read.
fn as_read(call: &ExternalCall, seen: &ExternalCall) -> bool {
    call.status == ExternalCallStatus::InProgress
        && call.owner == seen.owner
        && call.lease_until_ms == seen.lease_until_ms
}

impl ExternalCallStore for MemoryExternalCalls {
    fn insert<'a>(&'a self, call: &'a ExternalCall) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.0.data();
        if data.external.contains_key(&call.key) {
            return ready(Ok(false));
        }
        data.external.insert(call.key.clone(), call.clone());
        ready(Ok(true))
    }

    fn find<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ExternalCall>, StoreError>> {
        ready(Ok(self.0.data().external.get(key).cloned()))
    }

    fn take_over<'a>(
        &'a self,
        seen: &'a ExternalCall,
        owner: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<Option<ExternalCall>, StoreError>> {
        ready(Ok(self.update(
            &seen.key,
            |c| as_read(c, seen),
            |c| {
                c.owner = Some(owner.to_string());
                c.lease_until_ms = Some(lease_until_ms);
                c.updated_at_ms = now_ms;
                c.attempts += 1;
            },
        )))
    }

    fn expire<'a>(
        &'a self,
        seen: &'a ExternalCall,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self
            .update(
                &seen.key,
                |c| as_read(c, seen),
                |c| {
                    c.status = ExternalCallStatus::Unknown;
                    c.updated_at_ms = now_ms;
                    c.owner = None;
                    c.lease_until_ms = None;
                },
            )
            .is_some()))
    }

    fn succeed<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        reply: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.settle(
            key,
            owner,
            ExternalCallStatus::Succeeded,
            now_ms,
            |c| {
                c.reply = Some(reply.to_string());
                c.error = None;
            },
        )))
    }

    fn fail<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        error: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.settle(
            key,
            owner,
            ExternalCallStatus::Failed,
            now_ms,
            |c| c.error = Some(error.to_string()),
        )))
    }

    fn mark_unknown<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.settle(
            key,
            owner,
            ExternalCallStatus::Unknown,
            now_ms,
            |_| {},
        )))
    }

    fn reset<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.0.data();
        let owned = data.external.get(key).is_some_and(|c| owns(c, owner));
        if owned {
            data.external.remove(key);
        }
        ready(Ok(owned))
    }
}
