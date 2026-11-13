//! The outbox (ADR 0005): events appended in the service's transaction, relayed to the event
//! store by whichever instance claims them.

use std::collections::HashMap;
use std::time::Duration;

use futures::future::BoxFuture;

use super::{StoreError, Tx};

/// One event in the outbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxEntry {
    /// The event id (the record's `id` header).
    pub id: String,
    /// The event's wire type name.
    pub type_name: String,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
    /// The event as JSON: always a JSON object whose integers fit in a signed 64-bit integer
    /// (what both libraries and every store hold). A store refuses any other payload (not JSON,
    /// JSON that is not an object, an integer above `i64::MAX`) with
    /// [`MessagingError::InvalidMessage`].
    ///
    /// [`MessagingError::InvalidMessage`]: crate::error::MessagingError::InvalidMessage
    pub payload: Vec<u8>,
    /// Epoch milliseconds.
    pub created_at_ms: i64,
    /// The appending process's counter: breaks `created_at_ms` ties, so per-key order holds.
    pub seq: i64,
    /// Failed sends so far.
    pub attempts: i32,
}

/// The outbox. An entry is *pending* until it is sent or failed; the relay claims pending entries
/// under a lease, so two instances relaying one outbox never both hold an entry, and a crashed
/// one's entries are claimed again once its lease has passed.
pub trait OutboxStore: Send + Sync {
    /// Appends `entry` in the caller's transaction: rolled back with it. A payload that is not a
    /// JSON object is refused ([`OutboxEntry::payload`]).
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx,
        entry: OutboxEntry,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Appends `entry` on its own, outside any transaction.
    fn append_now(&self, entry: OutboxEntry) -> BoxFuture<'_, Result<(), StoreError>>;

    /// At most `limit` pending entries, now held by `owner` until `lease` from now: those nobody
    /// holds, those whose holder's lease has passed, and those `owner` holds already (its lease
    /// renewed); never an entry another owner holds. `(created_at_ms, seq)` is the only order: an
    /// entry `owner` still holds comes back in its place, so an entry whose send failed is retried
    /// before the later ones (order per key).
    fn claim<'a>(
        &'a self,
        owner: &'a str,
        lease: Duration,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<OutboxEntry>, StoreError>>;

    /// The entries were sent: never claimed again, purged after the retention.
    fn mark_sent<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<(), StoreError>>;

    /// A send of the entry failed and will be retried: `attempts + 1`; the entry stays pending
    /// (and held by its owner until the lease passes). `error` is for diagnosis; a store may keep
    /// it or not.
    fn mark_attempt_failed<'a>(
        &'a self,
        id: &'a str,
        error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// The entry is given up (out of attempts, or a failure no retry cures): `attempts + 1`,
    /// never claimed again, never purged. `error` is for diagnosis; a store may keep it or not.
    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Removes the entries sent before `sent_before_ms` (epoch milliseconds); how many.
    fn purge(&self, sent_before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>>;

    /// How many entries are pending (neither sent nor failed), held or not: the outbox backlog
    /// gauge.
    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>>;

    /// `created_at_ms` of the oldest pending entry (neither sent nor failed, held or not); `None`
    /// when none is pending: the outbox age gauge.
    fn oldest_pending(&self) -> BoxFuture<'_, Result<Option<i64>, StoreError>>;
}
