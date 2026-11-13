//! The persistence the saga runtime needs, as small ports, provided by the message store
//! ([`crate::store::MessageStore::sagas`]); the test fixture supplies in-memory ones. Every method
//! of the runtime's ports takes the [`Tx`] of the attempt it runs in, so the runtime decides what
//! commits together; an in-memory implementation ignores it ([`Tx::none`]). [`Tx`],
//! [`TxProvider`] and [`StoreError`] are the message store's ([`crate::store`]). Times are epoch
//! milliseconds.
//!
//! A port reports a [`StoreError`]: a *conflict* (a duplicate key, a write conflict, a transient
//! transaction error: the kind a retry resolves) apart from any other failure.
//!
//! The background workers have ports of their own, outside any transaction: the deadline
//! scheduler claims due deadlines ([`DeadlineStore::claim_due`]), the dispatch relay works the
//! saga-side outbox ([`DispatchQueue`]), and the statistics count live sagas
//! ([`SagaRepository::count_live`]) and pending entries ([`DispatchQueue::pending`]).

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;

use super::failed::FailureReason;
use super::store::{DeadlineDocument, SagaDispatchDocument, SagaDocument, SagaFailureDocument};
use crate::store::{StoreError, Tx, TxProvider};

/// The `sagas` collection.
pub trait SagaRepository: Send + Sync {
    fn find_by_id<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDocument>, StoreError>>;

    /// The live (not ended) instances of `type_name` associated with `key = value`.
    fn find_live<'a>(
        &'a self,
        tx: &'a mut Tx,
        type_name: &'a str,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<Vec<SagaDocument>, StoreError>>;

    /// Inserts a new instance; a [`StoreError::Conflict`] when its `startKey` is held by a live
    /// instance.
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Replaces the stored instance by `saga` when the stored `version` is `expected_version`;
    /// false when it is not.
    fn update<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
        expected_version: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// How many live (not ended) instances there are per saga type, outside any transaction
    /// (the `kraken.sagas.active` gauge); a type without one is absent.
    fn count_live(&self) -> BoxFuture<'_, Result<HashMap<String, u64>, StoreError>>;
}

/// The saga-side outbox `saga_dispatch`: where the runtime records what a saga sends (the relay
/// reads it).
pub trait DispatchRecorder: Send + Sync {
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        entries: &'a [SagaDispatchDocument],
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}

/// The `deadlines` collection, as far as the runtime writes it.
pub trait DeadlineStore: Send + Sync {
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        deadline: &'a DeadlineDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// The deadline `id` when it is still SCHEDULED (not fired, not cancelled), else `None`.
    fn find_scheduled<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>>;

    /// Cancels the SCHEDULED deadline `deadline_id` of `saga_id`; no effect on any other.
    fn cancel<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        deadline_id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Cancels the SCHEDULED deadlines of `saga_id` named `name`.
    fn cancel_by_name<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        name: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Cancels every SCHEDULED deadline of `saga_id`.
    fn cancel_all<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Sets the SCHEDULED deadline `id` to FIRED; false when it was not SCHEDULED.
    fn mark_fired<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Claims the SCHEDULED deadline due first (`due_at_ms <= now_ms`) that nobody holds or
    /// whose claim has passed (`claimed_until_ms < now_ms`), for `owner` until `claimed_until_ms`,
    /// atomically: of two schedulers asking at once, one gets it. Outside any transaction; the
    /// deadline as claimed, `None` when nothing is due.
    fn claim_due<'a>(
        &'a self,
        owner: &'a str,
        now_ms: i64,
        claimed_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>>;

    /// How many SCHEDULED deadlines are due (`due_at_ms <= now_ms`), per name, claimed or not
    /// (the `kraken.deadlines.due` gauge); a name without one is absent.
    fn count_overdue(&self, now_ms: i64)
    -> BoxFuture<'_, Result<HashMap<String, u64>, StoreError>>;
}

/// The `saga_failures` collection: parked events.
pub trait FailureStore: Send + Sync {
    fn find<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaFailureDocument>, StoreError>>;

    /// Inserts or replaces the failure with that id.
    fn save<'a>(
        &'a self,
        tx: &'a mut Tx,
        failure: &'a SagaFailureDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Deletes the failure; false when there was none.
    fn delete<'a>(&'a self, tx: &'a mut Tx, id: &'a str)
    -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Records that a retry of the failure `id` failed again: `attempts` grows by `attempts`,
    /// `error` and `lastFailedAt` are replaced. **Never inserts**: false when the failure no longer
    /// exists (a `discard` or a concurrent retry got there first), so the bookkeeping of a failed
    /// retry cannot bring a discarded failure back (ruling 36).
    fn failed_again<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        error: &'a str,
        attempts: i32,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// The failures of `saga_type` (every type when `None`), oldest `lastFailedAt` first.
    fn list<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<SagaFailureDocument>, StoreError>>;
}

/// The exactly-once markers of the saga stage: `processed_events` ids `saga:<sagaType>:<eventId>`.
pub trait MarkerStore: Send + Sync {
    fn exists<'a>(&'a self, tx: &'a mut Tx, id: &'a str)
    -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Inserts the marker; a [`StoreError::Conflict`] when it exists.
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        type_name: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}

/// How the relay gives an entry up ([`DispatchQueue::fail`]): `failed_at_ms`, the reason and
/// the message, with `attempts` when given, and a pending failure notice due at `notice_at_ms`
/// when given (`notice_pending` true, `notice_attempts` 0): a command's failure has one, an
/// event's none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchFailure {
    pub attempts: Option<i32>,
    pub reason: FailureReason,
    pub message: String,
    pub failed_at_ms: i64,
    pub notice_at_ms: Option<i64>,
}

/// The dispatch relay's side of the saga-side outbox (`saga_dispatch`) and of the per-saga
/// dispatch leases (`saga_dispatch_leases`). An entry is *pending* until it is dispatched or
/// failed. Every operation is a write of its own, outside any transaction, but
/// [`mark_dispatched`](Self::mark_dispatched), which joins the transaction the relay appends the
/// event to the outbox in.
pub trait DispatchQueue: Send + Sync {
    /// The sagas whose first pending entry (lowest `seq`) is due (`next_attempt_at_ms` absent or
    /// `<= now_ms`), oldest first pending entry first (its `created_at_ms`, then the saga id), at
    /// most `limit`, leaving out the sagas in `except` (in flight on this instance).
    fn due_sagas<'a>(
        &'a self,
        now_ms: i64,
        except: &'a [String],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<String>, StoreError>>;

    /// The first pending entry of `saga_id` (lowest `seq`), due or not.
    fn first_pending<'a>(
        &'a self,
        saga_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>>;

    /// The entry was dispatched at `now_ms`, in `tx` (an event: with its outbox append).
    fn mark_dispatched<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// A transient failure: `attempts` and the next attempt at `next_attempt_at_ms`.
    fn retry_later<'a>(
        &'a self,
        id: &'a str,
        attempts: i32,
        next_attempt_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Gives the entry up: see [`DispatchFailure`].
    fn fail<'a>(
        &'a self,
        id: &'a str,
        failure: &'a DispatchFailure,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Claims the pending failure notice due first (`notice_next_at_ms` absent or `<= now_ms`),
    /// moving its `notice_next_at_ms` to `reserve_until_ms` (which reserves it for the caller),
    /// atomically; the entry as claimed, `None` when no notice is due.
    fn claim_due_notice<'a>(
        &'a self,
        now_ms: i64,
        reserve_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>>;

    /// The failure notice was delivered (or given up): no notice is pending any more
    /// (`notice_pending`, `notice_attempts` and `notice_next_at_ms` cleared).
    fn clear_notice<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), StoreError>>;

    /// A delivery of the notice failed: `notice_attempts` and its next try at `next_at_ms`.
    fn notice_failed<'a>(
        &'a self,
        id: &'a str,
        notice_attempts: i32,
        next_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Takes the dispatch lease of `saga_id` for `owner` until `until_ms` when nobody holds it or
    /// its lease has passed (`< now_ms`), atomically; false when another holder's lease runs.
    fn claim_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        now_ms: i64,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Moves the lease `owner` holds to `until_ms`; false when it holds it no longer.
    fn renew_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Lets go of the lease when `owner` holds it.
    fn release_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// How many entries are pending (the `kraken.saga.dispatch.pending` gauge).
    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>>;
}

/// The six ports together.
#[derive(Clone)]
pub struct SagaPorts {
    pub tx: Arc<dyn TxProvider>,
    pub sagas: Arc<dyn SagaRepository>,
    pub dispatch: Arc<dyn DispatchRecorder>,
    pub deadlines: Arc<dyn DeadlineStore>,
    pub failures: Arc<dyn FailureStore>,
    pub markers: Arc<dyn MarkerStore>,
}

impl SagaPorts {
    /// The ports of `store`, in its transactions.
    pub fn of(store: &dyn crate::store::MessageStore) -> SagaPorts {
        let stores = store.sagas();
        SagaPorts {
            tx: store.tx(),
            sagas: stores.sagas,
            dispatch: stores.dispatch,
            deadlines: stores.deadlines,
            failures: stores.failures,
            markers: stores.markers,
        }
    }
}
