//! The processor control state in memory.

use std::sync::Arc;

use futures::future::BoxFuture;

use super::{Inner, ready};
use crate::event_store::ReplayRange;
use crate::store::{
    ControlOperation, Locked, ProcessorControlState, ProcessorControlStore, RESET_KIND, StoreError,
};

pub(super) struct MemoryControl(pub(super) Arc<Inner>);

impl MemoryControl {
    /// Changes the state of `processor`, made when absent.
    fn upsert<T>(
        &self,
        processor: &str,
        change: impl FnOnce(&mut ProcessorControlState) -> T,
    ) -> T {
        let mut data = self.0.data();
        let state = data
            .control
            .entry(processor.to_string())
            .or_insert_with(|| ProcessorControlState {
                processor: processor.to_string(),
                ..ProcessorControlState::default()
            });
        change(state)
    }

    /// Changes the state of `processor` when it exists and `matches`; whether it did.
    fn update(
        &self,
        processor: &str,
        matches: impl FnOnce(&ProcessorControlState) -> bool,
        change: impl FnOnce(&mut ProcessorControlState),
    ) -> bool {
        let mut data = self.0.data();
        match data.control.get_mut(processor) {
            Some(state) if matches(state) => {
                change(state);
                true
            }
            _ => false,
        }
    }

    fn extend_lease(&self, processor: &str, operation: &str, until_ms: i64, now_ms: i64) -> bool {
        self.update(
            processor,
            |s| s.operation.as_ref().is_some_and(|op| op.id == operation),
            |s| {
                if let Some(op) = s.operation.as_mut() {
                    op.lease_until_ms = until_ms;
                }
                s.updated_at_ms = Some(now_ms);
            },
        )
    }
}

impl ProcessorControlStore for MemoryControl {
    fn get<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<ProcessorControlState, StoreError>> {
        let state = self.0.data().control.get(processor).cloned();
        ready(Ok(state.unwrap_or_else(|| ProcessorControlState {
            processor: processor.to_string(),
            ..ProcessorControlState::default()
        })))
    }

    fn set_paused<'a>(
        &'a self,
        processor: &'a str,
        paused: bool,
        by: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.upsert(processor, |s| {
            s.paused = paused;
            s.paused_at_ms = paused.then_some(now_ms);
            s.paused_by = paused.then(|| by.to_string());
            s.updated_at_ms = Some(now_ms);
        });
        ready(Ok(()))
    }

    fn lock<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<Locked, StoreError>> {
        let locked = self.upsert(processor, |s| match &mut s.operation {
            Some(held) if held.id == operation => {
                held.lease_until_ms = lease_until_ms;
                s.updated_at_ms = Some(now_ms);
                Locked::Taken
            }
            Some(held) if held.holds_at(now_ms) => Locked::Held(held.clone()),
            _ => {
                s.operation = Some(ControlOperation {
                    id: operation.to_string(),
                    kind: RESET_KIND.to_string(),
                    lease_until_ms,
                    prepared_at_ms: now_ms,
                });
                s.updated_at_ms = Some(now_ms);
                Locked::Taken
            }
        });
        ready(Ok(locked))
    }

    fn extend<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.extend_lease(
            processor,
            operation,
            lease_until_ms,
            now_ms,
        )))
    }

    fn release<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.update(
            processor,
            |s| s.operation.as_ref().is_some_and(|op| op.id == operation),
            |s| {
                s.operation = None;
                s.updated_at_ms = Some(now_ms);
            },
        )))
    }

    fn expire<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.update(
            processor,
            |s| {
                s.operation
                    .as_ref()
                    .is_some_and(|op| op.id == operation && !op.holds_at(now_ms))
            },
            |s| {
                s.operation = None;
                s.updated_at_ms = Some(now_ms);
            },
        )))
    }

    fn set_replay<'a>(
        &'a self,
        processor: &'a str,
        ranges: &'a [ReplayRange],
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.upsert(processor, |s| {
            s.replay = ranges.to_vec();
            s.updated_at_ms = Some(now_ms);
        });
        ready(Ok(()))
    }

    fn remove_range<'a>(
        &'a self,
        processor: &'a str,
        range: &'a ReplayRange,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.update(
            processor,
            |s| s.replay.contains(range),
            |s| {
                s.replay.retain(|r| r != range);
                s.updated_at_ms = Some(now_ms);
            },
        )))
    }
}
