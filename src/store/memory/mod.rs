//! The in-memory message store: every store of [`crate::store`] in the process's
//! memory, for tests, local runs and single-process demos, with no database.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use snodra_messaging::event_store::memory::MemoryEventStore;
//! # use snodra_messaging::store::memory::MemoryStore;
//! # use snodra_messaging::{Config, SnodraMessaging, MessagingError};
//! # async fn run(config: Config) -> Result<(), MessagingError> {
//! let app = SnodraMessaging::builder(config)
//!     .await?
//!     .store(Arc::new(MemoryStore::new()))
//!     .event_store(Arc::new(MemoryEventStore::default()))
//!     // the service's handlers
//!     .start()
//!     .await?;
//! # app.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! It keeps the contract of a database add-on, which the store conformance kit checks:
//!
//! - **Transactions.** [`TxProvider::begin`] hands out a transaction ([`Tx`]) that collects the
//!   writes made in it; the other readers see them only once it commits, and an abort (or a
//!   transaction dropped without a commit) leaves nothing behind. Reads in a transaction see its
//!   own writes. A transaction reserves every record it writes: a second live transaction writing
//!   one of them fails at once, and a commit fails when a record it wrote was changed meanwhile (a
//!   write outside any transaction) or would leave a live saga's start key held twice; each is a
//!   [`StoreError::Conflict`] (a database's write conflict), so a retry of the whole attempt
//!   resolves it. What runs outside a transaction ([`TxProvider::plain`], [`Tx::none`], a
//!   transaction that ended, every store call that takes no transaction) commits at once.
//! - **Claims and leases** (the outbox relay, the deadline scheduler, the saga dispatch leases,
//!   the reset locks) are atomic: of two claimers, one wins. The times it stamps itself are the
//!   application's clock ([`MessageStore::use_clock`]); until the builder hands it over,
//!   [`Clock::tokio`], which follows paused time in tests.
//! - **Relay wake-up**: every committed outbox append calls the
//!   [`OutboxStore::on_append`] wakers, so the relay publishes at once rather than at its next
//!   poll.
//! - **Retention**: nothing is ever deleted but dead letters at their `expire_at_ms` (on the
//!   store's own clock, as a database's TTL) and what the explicit `purge` calls remove (the
//!   library makes none): it keeps everything else it is given until the process ends.
//! - **Not durable**: one process, and everything is lost when it ends. The builder warns at
//!   startup that it is not durable.
//!
//! Every clone of a [`MemoryStore`] is the same store.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};

use futures::future::BoxFuture;

use super::{
    DeadLetter, DeadLetterStore, ExternalCall, ExternalCallStore, MessageStore, OutboxEntry,
    OutboxStore, ProcessedEventStore, ProcessedMarker, ProcessorControlState,
    ProcessorControlStore, SagaStores, StoreError, Tx, TxProvider,
};
use crate::config::Config;
use crate::error::MessagingError;
use crate::saga::Clock;
use crate::saga::store::{
    DeadlineDocument, SagaDispatchDocument, SagaDocument, SagaFailureDocument,
};

mod control;
mod dead_letters;
mod external;
mod outbox;
mod processed;
mod sagas;

/// The store's name ([`MessageStore::name`]).
pub const NAME: &str = "memory";

/// The in-memory message store: see the module docs.
#[derive(Clone)]
pub struct MemoryStore {
    inner: Arc<Inner>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        MemoryStore::new()
    }
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> MemoryStore {
        static STORES: AtomicU64 = AtomicU64::new(0);
        MemoryStore {
            inner: Arc::new(Inner {
                id: STORES.fetch_add(1, AtomicOrdering::Relaxed),
                data: Mutex::default(),
                clock: RwLock::new(Clock::tokio()),
                ttl_clock: Clock::tokio(),
                next_tx: AtomicU64::new(1),
                wakers: Mutex::default(),
                next_waker: AtomicU64::new(1),
            }),
        }
    }
}

pub(super) struct Inner {
    /// Which store a transaction belongs to.
    id: u64,
    data: Mutex<Data>,
    /// The application's clock: what the store stamps its own times with.
    clock: RwLock<Clock>,
    /// The store's own clock, as a database's: when an expired dead letter disappears.
    ttl_clock: Clock,
    /// The id of the next transaction.
    next_tx: AtomicU64,
    /// Called after an outbox append commits ([`OutboxStore::on_append`]), by listener id.
    wakers: Mutex<BTreeMap<u64, Arc<dyn Fn() + Send + Sync>>>,
    /// The id of the next listener.
    next_waker: AtomicU64,
}

impl Inner {
    fn data(&self) -> MutexGuard<'_, Data> {
        self.data.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn now_ms(&self) -> i64 {
        self.clock
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .now_ms()
    }

    /// Calls every [`OutboxStore::on_append`] waker (never under the data lock).
    fn wake(&self) {
        let wakers: Vec<_> = self
            .wakers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect();
        for wake in wakers {
            wake();
        }
    }

    /// Runs `f` on the data and, when `tx` is one of this store's open transactions, on it, then
    /// reserves the records `f` wrote for that transaction: a record another live transaction
    /// reserved is a conflict at once (and the transaction can no longer commit). With
    /// [`Tx::none`] (or an ended transaction) the writes of `f` commit at once.
    fn with_tx<R>(
        &self,
        tx: &mut Tx,
        f: impl FnOnce(&mut Data, Option<&mut MemoryTx>) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        let open = self.open(tx)?;
        let mut data = self.data();
        let Some(open) = open else {
            return f(&mut data, None);
        };
        let done = f(&mut data, Some(&mut *open));
        if let Err(e) = data.reserve(open) {
            open.doomed = true;
            return Err(e);
        }
        done
    }

    /// This store's open transaction in `tx`; `None` for [`Tx::none`] and for a transaction that
    /// ended (what is written with it commits on its own); an error for another store's
    /// transaction.
    fn open<'t>(&self, tx: &'t mut Tx) -> Result<Option<&'t mut MemoryTx>, StoreError> {
        if tx.is_none() {
            return Ok(None);
        }
        match tx.get_mut::<MemoryTx>() {
            Some(open) if open.store != self.id => Err(failed(
                "the in-memory store was given another in-memory store's transaction",
            )),
            Some(open) if open.ended => Ok(None),
            Some(open) => Ok(Some(open)),
            None => Err(failed(
                "the in-memory store was given another store's transaction",
            )),
        }
    }
}

fn failed(why: &str) -> StoreError {
    StoreError::Failed(MessagingError::Storage(why.to_string()))
}

fn conflict(why: String) -> StoreError {
    StoreError::Conflict(why)
}

/// A boxed ready answer.
fn ready<'a, T: Send + 'a>(value: Result<T, StoreError>) -> BoxFuture<'a, Result<T, StoreError>> {
    Box::pin(std::future::ready(value))
}

// ---- tables and transactions -------------------------------------------------------------------

/// Records by id, each with the version of its last write.
pub(super) struct Table<T> {
    rows: BTreeMap<String, Row<T>>,
}

impl<T> Default for Table<T> {
    fn default() -> Self {
        Table {
            rows: BTreeMap::new(),
        }
    }
}

struct Row<T> {
    version: u64,
    value: T,
}

/// A transaction's writes to one table: per id, the version it saw (`None`: absent) and what
/// it wrote (`None`: deleted).
pub(super) struct Overlay<T> {
    writes: BTreeMap<String, Write<T>>,
}

impl<T> Default for Overlay<T> {
    fn default() -> Self {
        Overlay {
            writes: BTreeMap::new(),
        }
    }
}

struct Write<T> {
    base: Option<u64>,
    value: Option<T>,
}

impl<T: Clone> Table<T> {
    /// The record `id` as `overlay` sees it (the committed one without).
    pub(super) fn get<'s>(&'s self, overlay: Option<&'s Overlay<T>>, id: &str) -> Option<&'s T> {
        match overlay.and_then(|o| o.writes.get(id)) {
            Some(write) => write.value.as_ref(),
            None => self.rows.get(id).map(|row| &row.value),
        }
    }

    /// Every record as `overlay` sees it, by id.
    pub(super) fn values<'s>(&'s self, overlay: Option<&'s Overlay<T>>) -> Vec<&'s T> {
        let Some(overlay) = overlay.filter(|o| !o.writes.is_empty()) else {
            return self.rows.values().map(|row| &row.value).collect();
        };
        let mut merged: BTreeMap<&str, &T> = self
            .rows
            .iter()
            .filter(|(id, _)| !overlay.writes.contains_key(*id))
            .map(|(id, row)| (id.as_str(), &row.value))
            .collect();
        for (id, write) in &overlay.writes {
            if let Some(value) = &write.value {
                merged.insert(id, value);
            }
        }
        merged.into_values().collect()
    }

    /// Writes `value` (`None`: deletes) as `id`: into `overlay`, or committed at once (`version`
    /// counts every committed write).
    pub(super) fn set(
        &mut self,
        overlay: Option<&mut Overlay<T>>,
        version: &mut u64,
        id: &str,
        value: Option<T>,
    ) {
        match overlay {
            Some(overlay) => {
                let base = self.rows.get(id).map(|row| row.version);
                overlay
                    .writes
                    .entry(id.to_string())
                    .or_insert(Write { base, value: None })
                    .value = value;
            }
            None => {
                *version += 1;
                match value {
                    Some(value) => {
                        self.rows.insert(
                            id.to_string(),
                            Row {
                                version: *version,
                                value,
                            },
                        );
                    }
                    None => {
                        self.rows.remove(id);
                    }
                }
            }
        }
    }

    /// Changes the record `id` (as `overlay` sees it) with `change`; false when there is none.
    pub(super) fn update(
        &mut self,
        overlay: Option<&mut Overlay<T>>,
        version: &mut u64,
        id: &str,
        change: impl FnOnce(&mut T),
    ) -> bool {
        let Some(mut value) = self.get(overlay.as_deref(), id).cloned() else {
            return false;
        };
        change(&mut value);
        self.set(overlay, version, id, Some(value));
        true
    }

    /// The first write of `overlay` whose record changed since the transaction saw it.
    fn stale(&self, overlay: &Overlay<T>) -> Option<String> {
        overlay
            .writes
            .iter()
            .find(|(id, write)| self.rows.get(*id).map(|row| row.version) != write.base)
            .map(|(id, _)| id.clone())
    }

    /// Commits the writes of `overlay`.
    fn apply(&mut self, overlay: Overlay<T>, version: &mut u64) {
        for (id, write) in overlay.writes {
            self.set(None, version, &id, write.value);
        }
    }
}

/// One outbox entry and the relay's marks on it.
#[derive(Clone, Debug)]
pub(super) struct OutboxRow {
    entry: OutboxEntry,
    sent_at_ms: Option<i64>,
    failed_at_ms: Option<i64>,
    owner: Option<String>,
    lease_until_ms: Option<i64>,
}

/// Everything the store holds.
#[derive(Default)]
pub(super) struct Data {
    /// Counts every committed write: the version of a record is the count at its last write.
    version: u64,
    outbox: Table<OutboxRow>,
    /// The processed-event markers, the saga stage's included (one collection).
    processed: Table<ProcessedMarker>,
    dead_letters: Table<DeadLetter>,
    control: BTreeMap<String, ProcessorControlState>,
    external: BTreeMap<String, ExternalCall>,
    sagas: Table<SagaDocument>,
    dispatch: Table<SagaDispatchDocument>,
    deadlines: Table<DeadlineDocument>,
    failures: Table<SagaFailureDocument>,
    /// The saga dispatch leases: saga id to (owner, until epoch ms).
    leases: HashMap<String, (String, i64)>,
    /// The records a live transaction wrote: (table, id) to the transaction's id.
    reserved: HashMap<(&'static str, String), u64>,
}

/// A transaction of a [`MemoryStore`], held in a [`Tx`]: its writes, kept apart until the commit.
pub(super) struct MemoryTx {
    store: u64,
    id: u64,
    ended: bool,
    /// A write met a record another live transaction reserved: the commit fails.
    doomed: bool,
    /// The records this transaction reserved, as (table, id).
    reserved: Vec<(&'static str, String)>,
    /// To release the reservations of a transaction dropped before it ended.
    inner: Weak<Inner>,
    outbox: Overlay<OutboxRow>,
    processed: Overlay<ProcessedMarker>,
    dead_letters: Overlay<DeadLetter>,
    sagas: Overlay<SagaDocument>,
    dispatch: Overlay<SagaDispatchDocument>,
    deadlines: Overlay<DeadlineDocument>,
    failures: Overlay<SagaFailureDocument>,
}

impl MemoryTx {
    fn new(inner: &Arc<Inner>) -> MemoryTx {
        MemoryTx {
            store: inner.id,
            id: inner.next_tx.fetch_add(1, AtomicOrdering::Relaxed),
            ended: false,
            doomed: false,
            reserved: Vec::new(),
            inner: Arc::downgrade(inner),
            outbox: Overlay::default(),
            processed: Overlay::default(),
            dead_letters: Overlay::default(),
            sagas: Overlay::default(),
            dispatch: Overlay::default(),
            deadlines: Overlay::default(),
            failures: Overlay::default(),
        }
    }
}

impl Drop for MemoryTx {
    /// A transaction dropped before it ended rolls back: its reservations go.
    fn drop(&mut self) {
        if !self.reserved.is_empty()
            && let Some(inner) = self.inner.upgrade()
        {
            inner.data().release(self);
        }
    }
}

impl MemoryTx {
    /// The (table, id) of every record written.
    fn written(&self) -> Vec<(&'static str, String)> {
        fn ids<'o, T>(
            table: &'static str,
            o: &'o Overlay<T>,
        ) -> impl Iterator<Item = (&'static str, String)> + 'o {
            o.writes.keys().map(move |id| (table, id.clone()))
        }
        ids("outbox", &self.outbox)
            .chain(ids("processed_events", &self.processed))
            .chain(ids("dead_letters", &self.dead_letters))
            .chain(ids("sagas", &self.sagas))
            .chain(ids("saga_dispatch", &self.dispatch))
            .chain(ids("deadlines", &self.deadlines))
            .chain(ids("saga_failures", &self.failures))
            .collect()
    }

    /// Forgets the writes (an abort).
    fn clear(&mut self) {
        self.outbox = Overlay::default();
        self.processed = Overlay::default();
        self.dead_letters = Overlay::default();
        self.sagas = Overlay::default();
        self.dispatch = Overlay::default();
        self.deadlines = Overlay::default();
        self.failures = Overlay::default();
    }
}

impl Data {
    /// Reserves for `tx` every record it wrote; a conflict when another live transaction holds one.
    fn reserve(&mut self, tx: &mut MemoryTx) -> Result<(), StoreError> {
        for key in tx.written() {
            match self.reserved.get(&key) {
                Some(holder) if *holder == tx.id => {}
                Some(_) => {
                    return Err(conflict(format!(
                        "write conflict: {} record {} is written by another transaction",
                        key.0, key.1
                    )));
                }
                None => {
                    self.reserved.insert(key.clone(), tx.id);
                    tx.reserved.push(key);
                }
            }
        }
        Ok(())
    }

    /// Lets go of `tx`'s reservations.
    fn release(&mut self, tx: &mut MemoryTx) {
        for key in tx.reserved.drain(..) {
            if self.reserved.get(&key) == Some(&tx.id) {
                self.reserved.remove(&key);
            }
        }
    }

    /// Commits `tx`'s writes, all or nothing: a conflict when a record it wrote changed since it
    /// saw it, or when a live saga's start key would be held twice.
    fn commit(&mut self, tx: &mut MemoryTx) -> Result<(), StoreError> {
        let stale = [
            ("outbox", self.outbox.stale(&tx.outbox)),
            ("processed_events", self.processed.stale(&tx.processed)),
            ("dead_letters", self.dead_letters.stale(&tx.dead_letters)),
            ("sagas", self.sagas.stale(&tx.sagas)),
            ("saga_dispatch", self.dispatch.stale(&tx.dispatch)),
            ("deadlines", self.deadlines.stale(&tx.deadlines)),
            ("saga_failures", self.failures.stale(&tx.failures)),
        ];
        if let Some((table, id)) = stale
            .into_iter()
            .find_map(|(table, id)| id.map(|id| (table, id)))
        {
            return Err(conflict(format!(
                "write conflict: {table} record {id} changed since the transaction read it"
            )));
        }
        for (id, write) in &tx.sagas.writes {
            if let Some(key) = write
                .value
                .as_ref()
                .filter(|s| !s.ended)
                .and_then(|s| s.start_key.as_ref())
                && self.sagas.rows.iter().any(|(other, row)| {
                    other != id
                        && !tx.sagas.writes.contains_key(other)
                        && !row.value.ended
                        && row.value.start_key.as_ref() == Some(key)
                })
            {
                return Err(conflict(format!(
                    "a live saga holds the start key {key} already"
                )));
            }
        }
        let v = &mut self.version;
        self.outbox.apply(std::mem::take(&mut tx.outbox), v);
        self.processed.apply(std::mem::take(&mut tx.processed), v);
        self.dead_letters
            .apply(std::mem::take(&mut tx.dead_letters), v);
        self.sagas.apply(std::mem::take(&mut tx.sagas), v);
        self.dispatch.apply(std::mem::take(&mut tx.dispatch), v);
        self.deadlines.apply(std::mem::take(&mut tx.deadlines), v);
        self.failures.apply(std::mem::take(&mut tx.failures), v);
        Ok(())
    }
}

impl TxProvider for MemoryStore {
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        ready(Ok(Tx::new(MemoryTx::new(&self.inner))))
    }

    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        ready(Ok(Tx::none()))
    }

    /// Commits the transaction's writes; [`StoreError::Conflict`] (and nothing committed) when
    /// another write got to one of its records first. Ends the transaction either way; an ended
    /// transaction (or none) commits nothing.
    fn commit<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>> {
        let open = match self.inner.open(tx) {
            Ok(Some(open)) => open,
            Ok(None) => return ready(Ok(())),
            Err(e) => return ready(Err(e)),
        };
        open.ended = true;
        let appended = !open.outbox.writes.is_empty();
        let committed = {
            let mut data = self.inner.data();
            let committed = if open.doomed {
                Err(conflict(
                    "write conflict: the transaction met a record another one was writing".into(),
                ))
            } else {
                data.commit(open)
            };
            data.release(open);
            committed
        };
        if committed.is_ok() && appended {
            self.inner.wake();
        }
        ready(committed)
    }

    fn abort<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, ()> {
        if let Ok(Some(open)) = self.inner.open(tx) {
            open.ended = true;
            open.clear();
            self.inner.data().release(open);
        }
        Box::pin(std::future::ready(()))
    }
}

impl MessageStore for MemoryStore {
    fn name(&self) -> &'static str {
        NAME
    }

    /// Nothing to prepare.
    fn init_schema<'a>(&'a self, _config: &'a Config) -> BoxFuture<'a, Result<(), StoreError>> {
        ready(Ok(()))
    }

    fn use_clock(&self, clock: Clock) {
        *self.inner.clock.write().unwrap_or_else(|p| p.into_inner()) = clock;
    }

    fn tx(&self) -> Arc<dyn TxProvider> {
        Arc::new(self.clone())
    }

    fn outbox(&self) -> Arc<dyn OutboxStore> {
        Arc::new(outbox::MemoryOutbox(Arc::clone(&self.inner)))
    }

    fn processed_events(&self) -> Arc<dyn ProcessedEventStore> {
        Arc::new(processed::MemoryProcessed(Arc::clone(&self.inner)))
    }

    fn dead_letters(&self) -> Arc<dyn DeadLetterStore> {
        Arc::new(dead_letters::MemoryDeadLetters(Arc::clone(&self.inner)))
    }

    fn processor_control(&self) -> Arc<dyn ProcessorControlStore> {
        Arc::new(control::MemoryControl(Arc::clone(&self.inner)))
    }

    fn external_calls(&self) -> Arc<dyn ExternalCallStore> {
        Arc::new(external::MemoryExternalCalls(Arc::clone(&self.inner)))
    }

    fn sagas(&self) -> SagaStores {
        let sagas = Arc::new(sagas::MemorySagas(Arc::clone(&self.inner)));
        SagaStores {
            sagas: sagas.clone(),
            dispatch: sagas.clone(),
            deadlines: sagas.clone(),
            failures: sagas.clone(),
            markers: sagas,
        }
    }
}

// ---- what a test reads ------------------------------------------------------------------------

/// One outbox entry and the relay's marks on it, as [`MemoryContents`] shows it.
#[cfg(feature = "testing")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxRecord {
    pub entry: OutboxEntry,
    pub sent_at_ms: Option<i64>,
    pub failed_at_ms: Option<i64>,
    pub owner: Option<String>,
    pub lease_until_ms: Option<i64>,
}

/// Everything a [`MemoryStore`] holds, committed, each collection by id (feature `testing`): what
/// a test reads back ([`MemoryStore::contents`]) or changes behind the library's back
/// ([`MemoryStore::edit`]).
#[cfg(feature = "testing")]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryContents {
    pub outbox: Vec<OutboxRecord>,
    /// The processed-event markers, the saga stage's included.
    pub processed: Vec<ProcessedMarker>,
    pub dead_letters: Vec<DeadLetter>,
    pub control: Vec<ProcessorControlState>,
    pub external_calls: Vec<ExternalCall>,
    pub sagas: Vec<SagaDocument>,
    pub dispatch: Vec<SagaDispatchDocument>,
    pub deadlines: Vec<DeadlineDocument>,
    pub failures: Vec<SagaFailureDocument>,
    /// The saga dispatch leases: (saga id, owner, until epoch ms).
    pub dispatch_leases: Vec<(String, String, i64)>,
}

#[cfg(feature = "testing")]
impl MemoryStore {
    /// How many [`OutboxStore::on_append`] listeners are registered.
    pub fn append_listeners(&self) -> usize {
        self.inner
            .wakers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    /// A copy of what the store holds, committed.
    pub fn contents(&self) -> MemoryContents {
        let data = self.inner.data();
        let rows = |values: Vec<&OutboxRow>| -> Vec<OutboxRecord> {
            values
                .into_iter()
                .map(|r| OutboxRecord {
                    entry: r.entry.clone(),
                    sent_at_ms: r.sent_at_ms,
                    failed_at_ms: r.failed_at_ms,
                    owner: r.owner.clone(),
                    lease_until_ms: r.lease_until_ms,
                })
                .collect()
        };
        let mut leases: Vec<(String, String, i64)> = data
            .leases
            .iter()
            .map(|(saga, (owner, until))| (saga.clone(), owner.clone(), *until))
            .collect();
        leases.sort();
        MemoryContents {
            outbox: rows(data.outbox.values(None)),
            processed: data.processed.values(None).into_iter().cloned().collect(),
            dead_letters: data
                .dead_letters
                .values(None)
                .into_iter()
                .cloned()
                .collect(),
            control: data.control.values().cloned().collect(),
            external_calls: data.external.values().cloned().collect(),
            sagas: data.sagas.values(None).into_iter().cloned().collect(),
            dispatch: data.dispatch.values(None).into_iter().cloned().collect(),
            deadlines: data.deadlines.values(None).into_iter().cloned().collect(),
            failures: data.failures.values(None).into_iter().cloned().collect(),
            dispatch_leases: leases,
        }
    }

    /// Changes what the store holds with `change`, at once and outside any transaction (an open
    /// transaction that wrote a record it changes then conflicts at its commit).
    pub fn edit(&self, change: impl FnOnce(&mut MemoryContents)) {
        let mut contents = self.contents();
        change(&mut contents);
        let mut data = self.inner.data();
        let data = &mut *data;
        fn refill<T: Clone>(
            table: &mut Table<T>,
            version: &mut u64,
            values: Vec<T>,
            id: impl Fn(&T) -> String,
        ) {
            *table = Table::default();
            for value in values {
                table.set(None, version, &id(&value), Some(value));
            }
        }
        let v = &mut data.version;
        refill(
            &mut data.outbox,
            v,
            contents
                .outbox
                .into_iter()
                .map(|r| OutboxRow {
                    entry: r.entry,
                    sent_at_ms: r.sent_at_ms,
                    failed_at_ms: r.failed_at_ms,
                    owner: r.owner,
                    lease_until_ms: r.lease_until_ms,
                })
                .collect(),
            |r| r.entry.id.clone(),
        );
        refill(&mut data.processed, v, contents.processed, |m| m.id.clone());
        refill(&mut data.dead_letters, v, contents.dead_letters, |l| {
            l.id.clone()
        });
        refill(&mut data.sagas, v, contents.sagas, |s| s.id.clone());
        refill(&mut data.dispatch, v, contents.dispatch, |e| e.id.clone());
        refill(&mut data.deadlines, v, contents.deadlines, |d| d.id.clone());
        refill(&mut data.failures, v, contents.failures, |f| f.id.clone());
        data.control = contents
            .control
            .into_iter()
            .map(|c| (c.processor.clone(), c))
            .collect();
        data.external = contents
            .external_calls
            .into_iter()
            .map(|c| (c.key.clone(), c))
            .collect();
        data.leases = contents
            .dispatch_leases
            .into_iter()
            .map(|(saga, owner, until)| (saga, (owner, until)))
            .collect();
    }
}

// ---- positions ---------------------------------------------------------------------------------

/// A position as the document stores keep it: a number when it is a canonical decimal `i64`,
/// else its text; numbers order before texts, as a database orders mixed values.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Position {
    Number(i64),
    Text(String),
}

impl Position {
    pub(super) fn of(text: &str) -> Position {
        match text.parse::<i64>() {
            Ok(n) if n.to_string() == text => Position::Number(n),
            _ => Position::Text(text.to_string()),
        }
    }

    /// `self < other` when both are of one kind (a comparison across kinds holds for none).
    pub(super) fn before(&self, other: &Position) -> bool {
        match (self, other) {
            (Position::Number(a), Position::Number(b)) => a < b,
            (Position::Text(a), Position::Text(b)) => a < b,
            _ => false,
        }
    }

    /// `from <= self < until`, each comparison within one kind.
    pub(super) fn within(&self, from: &Position, until: &Position) -> bool {
        !self.before(from) && self.kind() == from.kind() && self.before(until)
    }

    fn kind(&self) -> u8 {
        match self {
            Position::Number(_) => 0,
            Position::Text(_) => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_compare_within_their_kind() {
        let (n2, n10, t) = (Position::of("2"), Position::of("10"), Position::of("x"));
        assert!(n2.before(&n10) && !n10.before(&n2));
        assert!(n2 < t && n10 < t, "numbers sort before texts");
        assert!(
            !n2.before(&t) && !t.before(&n2),
            "no comparison across kinds"
        );
        assert_eq!(Position::of("007"), Position::Text("007".into()));
        assert_eq!(Position::of("-5"), Position::Number(-5));
        assert!(n2.within(&Position::of("2"), &n10));
        assert!(!n10.within(&n2, &n10));
        assert!(!t.within(&Position::of("a"), &n10));
    }

    #[test]
    fn a_transaction_sees_its_own_writes_and_others_see_them_at_commit() {
        let mut table: Table<String> = Table::default();
        let mut version = 0;
        table.set(None, &mut version, "a", Some("committed".into()));
        let mut overlay = Overlay::default();
        table.set(Some(&mut overlay), &mut version, "a", Some("mine".into()));
        table.set(Some(&mut overlay), &mut version, "b", Some("new".into()));
        assert_eq!(
            table.get(Some(&overlay), "a").map(String::as_str),
            Some("mine")
        );
        assert_eq!(table.get(None, "a").map(String::as_str), Some("committed"));
        assert_eq!(table.get(None, "b"), None);
        assert_eq!(
            table.values(Some(&overlay)),
            [&"mine".to_string(), &"new".to_string()]
        );
        assert_eq!(table.stale(&overlay), None);
        table.set(None, &mut version, "a", Some("other".into()));
        assert_eq!(
            table.stale(&overlay),
            Some("a".to_string()),
            "changed meanwhile"
        );
    }
}
