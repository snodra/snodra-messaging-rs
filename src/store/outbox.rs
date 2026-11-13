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
    /// The event as JSON (an object).
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
    /// Appends `entry` in the caller's transaction: rolled back with it.
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx,
        entry: OutboxEntry,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Appends `entry` on its own, outside any transaction.
    fn append_now(&self, entry: OutboxEntry) -> BoxFuture<'_, Result<(), StoreError>>;

    /// At most `limit` pending entries in `(created_at_ms, seq)` order, now held by `owner` until
    /// `lease` from now: those nobody holds, those whose holder's lease has passed, and those
    /// `owner` holds already (its lease renewed). Never an entry another owner holds.
    fn claim<'a>(
        &'a self,
        owner: &'a str,
        lease: Duration,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<OutboxEntry>, StoreError>>;

    /// The entries were sent: never claimed again, purged after the retention.
    fn mark_sent<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<(), StoreError>>;

    /// A send of the entry failed and will be retried: `attempts + 1`; the entry stays pending
    /// (and held by its owner until the lease passes).
    fn mark_retry<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), StoreError>>;

    /// The entry is given up (out of attempts, or a failure no retry cures): `attempts + 1`,
    /// never claimed again, never purged.
    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Removes the entries sent before `sent_before_ms` (epoch milliseconds); how many.
    fn purge(&self, sent_before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>>;
}
