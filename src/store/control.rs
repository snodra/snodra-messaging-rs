//! The processor control state (ADR 0023): per processor its pause, its reset lock and its
//! replay ranges, the source of truth of all three (the hub keeps none of them). Every operation
//! is a write of its own, outside any transaction; times are the caller's clock, epoch
//! milliseconds.

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use super::StoreError;

/// `ControlOperation::kind` of a reset, the only kind there is.
pub const RESET_KIND: &str = "reset";

/// One processor's state; a processor the store knows nothing of is not paused, not locked and
/// not replaying.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessorControlState {
    pub processor: String,
    pub paused: bool,
    pub paused_at_ms: Option<i64>,
    /// The `control_id` of the request that paused it, or `local`.
    pub paused_by: Option<String>,
    /// The reset holding the processor's lock.
    pub operation: Option<ControlOperation>,
    /// The open replay ranges.
    pub replay: Vec<ReplayRange>,
    pub updated_at_ms: Option<i64>,
}

/// The lock of a reset: held until `lease_until_ms` unless renewed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlOperation {
    pub id: String,
    pub kind: String,
    pub lease_until_ms: i64,
    /// The first prepare's time; a renewal keeps it.
    pub prepared_at_ms: i64,
}

impl ControlOperation {
    /// Whether the lease has not passed at `now_ms`.
    pub fn holds_at(&self, now_ms: i64) -> bool {
        self.lease_until_ms > now_ms
    }
}

/// Records of `partition` of `topic` with `from <= offset < until` are a replay.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplayRange {
    pub topic: String,
    pub partition: i32,
    pub from: i64,
    pub until: i64,
}

/// Whether [`ProcessorControlStore::lock`] took (or renewed) the lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Locked {
    /// The operation holds the lock now.
    Taken,
    /// Another operation holds it (unexpired).
    Held(ControlOperation),
}

pub trait ProcessorControlStore: Send + Sync {
    /// The state of `processor`; the default (not paused, nothing held) when the store has none.
    fn get<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<ProcessorControlState, StoreError>>;

    /// Pauses (`by` the control id, or `local`) or resumes `processor`; idempotent. A resume
    /// clears `paused_at_ms` and `paused_by`.
    fn set_paused<'a>(
        &'a self,
        processor: &'a str,
        paused: bool,
        by: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Takes the lock of `processor` for `operation` until `lease_until_ms`, atomically: when
    /// nothing holds it or the holder's lease has passed (`lease_until_ms <= now_ms`); a renewal
    /// (the operation holds it already) moves the lease only. [`Locked::Held`] names the holder.
    fn lock<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<Locked, StoreError>>;

    /// Moves the lease of `operation`; false when it does not hold the lock.
    fn extend<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Clears the lock when `operation` holds it (never another operation's); false when it did
    /// not.
    fn release<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Clears the lock when `operation` holds it and its lease has passed at `now_ms` (a renewal
    /// that came meanwhile keeps it).
    fn expire<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Replaces the replay ranges of `processor`.
    fn set_replay<'a>(
        &'a self,
        processor: &'a str,
        ranges: &'a [ReplayRange],
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Removes `range` (that exact range) from the replay ranges of `processor`; false when it
    /// was not there.
    fn remove_range<'a>(
        &'a self,
        processor: &'a str,
        range: &'a ReplayRange,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;
}
