//! The records of idempotent external calls (ADR 0021 rulings 12, 17, 38, 39, 41-43, 70), keyed
//! by their idempotency key. Every operation is a write of its own, outside any transaction (a
//! rolled-back handler never forgets a call that happened); times are the caller's clock, epoch
//! milliseconds.

use futures::future::BoxFuture;

use super::StoreError;

/// Where an external call stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExternalCallStatus {
    InProgress,
    Succeeded,
    Failed,
    Unknown,
}

/// The record of one call. `reply` is the JSON text of the handler's reply, `error` the JSON text
/// of a `Failed` call's error (`{"code","message"}`). An `InProgress` record holds `owner` (the
/// running call's random token) and `lease_until_ms`; a settled one holds neither.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalCall {
    pub key: String,
    pub status: ExternalCallStatus,
    pub reply: Option<String>,
    pub error: Option<String>,
    pub attempts: i32,
    pub updated_at_ms: i64,
    pub lease_until_ms: Option<i64>,
    pub owner: Option<String>,
}

/// Every write after [`insert`](ExternalCallStore::insert) matches the record a call owns (its
/// key, `InProgress` and its `owner`) or the record a call read (its key, `InProgress`, `owner`
/// and `lease_until_ms` as read), so a call whose lease ran out never overwrites the record of the
/// call that took it over; false (or `None`) when nothing matched.
pub trait ExternalCallStore: Send + Sync {
    /// Inserts `call`; false when a record of its key exists.
    fn insert<'a>(&'a self, call: &'a ExternalCall) -> BoxFuture<'a, Result<bool, StoreError>>;

    fn find<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ExternalCall>, StoreError>>;

    /// Takes the lapsed call `seen` over for `owner`: `owner`, `lease_until_ms`,
    /// `updated_at_ms = now_ms` and `attempts + 1`, when the record is still `seen`; the record
    /// as written.
    fn take_over<'a>(
        &'a self,
        seen: &'a ExternalCall,
        owner: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<Option<ExternalCall>, StoreError>>;

    /// Gives the lapsed call `seen` up as `Unknown` (no owner, no lease), when the record is still
    /// `seen`.
    fn expire<'a>(
        &'a self,
        seen: &'a ExternalCall,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// `Succeeded` with `reply` (and no error, owner or lease) on the record `owner` owns.
    fn succeed<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        reply: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// `Failed` with `error` (no owner or lease) on the record `owner` owns.
    fn fail<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        error: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// `Unknown` (no owner or lease) on the record `owner` owns.
    fn mark_unknown<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Deletes the record `owner` owns (a retryable failure: the next call begins afresh).
    fn reset<'a>(&'a self, key: &'a str, owner: &'a str)
    -> BoxFuture<'a, Result<bool, StoreError>>;
}
