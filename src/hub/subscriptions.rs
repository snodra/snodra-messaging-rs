//! The subscriptions this instance serves (ADR 0017, P4c) and the [`QueryUpdateEmitter`] that
//! feeds them. A `Request{mode = SUBSCRIPTION}` registers one (the decoded query, the request id
//! `<id>#n`, the hub stream it arrived on); every `emit` whose filter the query passes becomes a
//! `ReplyPart` on that stream (`seq` from 1 per subscription; the hub renumbers), `complete` and
//! `complete_exceptionally` a `ReplyEnd`. Each subscription has a pump task that writes its
//! messages in call order, so an emitter call never blocks on the hub stream's buffer.
//!
//! A subscription is registered when its request is read (see [`Subscriptions::register`]),
//! before its admission (identity, guard, limit) completes: a hub `Cancel` or a lost stream that
//! follows finds it, so none outlives the hub's own forgetting. Until admitted, its pump holds
//! back what matching emits queued; a refused admission drops them.

use std::any::Any;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::codec;
use crate::error::MessagingError;
use crate::proto::{self, ClientMessage, ErrorCode, client_message};
use crate::types::SubscriptionQuery;

use super::connection::{ReplyTo, StreamId};

/// `Config.hub_max_subscriptions` when nothing sets it: the hub's own default per-instance
/// limit (`KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE`).
pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 1024;

/// What a subscription's pump writes to its hub stream, in order.
enum Outgoing {
    Part {
        payload: Vec<u8>,
        seq: u64,
    },
    /// `parts`: the updates this subscription sent before the end (`ReplyEnd.parts`).
    End {
        error: Option<proto::Error>,
        parts: u64,
    },
}

/// One open subscription.
struct Open {
    /// Tells this entry from another registered under the same id later.
    serial: u64,
    type_name: String,
    /// The decoded query, as the registered handler's type, for the emitter's filters (shared
    /// with the filters' snapshot, which runs them outside the lock).
    query: Arc<dyn Any + Send + Sync>,
    /// The query as sent, decoded again when a filter is typed with another struct of the same
    /// wire name.
    payload: Arc<Vec<u8>>,
    /// The hub stream the request arrived on.
    stream: StreamId,
    /// Updates sent so far (the `seq` of the last part).
    seq: u64,
    tx: mpsc::UnboundedSender<Outgoing>,
    /// Stops the pump at once (a hub `Cancel`, a lost stream): what is queued is dropped.
    cancel: CancellationToken,
    pump: JoinHandle<()>,
}

/// What an emit reads of an open subscription under the lock, to run the filter on it outside.
struct Candidate {
    id: String,
    serial: u64,
    query: Arc<dyn Any + Send + Sync>,
    payload: Arc<Vec<u8>>,
}

impl Candidate {
    fn matches<Q: SubscriptionQuery>(&self, filter: &dyn Fn(&Q) -> bool) -> bool {
        match self.query.downcast_ref::<Q>() {
            Some(query) => filter(query),
            // Registered under another Rust type of the same wire name: decode for this one.
            None => codec::decode::<Q>(&self.payload)
                .map(|q| filter(&q))
                .unwrap_or(false),
        }
    }
}

/// The subscriptions open on this instance, by their handler-side request id (`<id>#n`).
pub(crate) struct Subscriptions {
    max: AtomicUsize,
    serials: AtomicU64,
    open: Mutex<HashMap<String, Open>>,
}

impl Subscriptions {
    pub(crate) fn new(max: usize) -> Subscriptions {
        Subscriptions {
            max: AtomicUsize::new(max.max(1)),
            serials: AtomicU64::new(0),
            open: Mutex::new(HashMap::new()),
        }
    }

    /// The per-instance limit: a subscription beyond it is refused.
    pub(crate) fn set_max(&self, max: usize) {
        self.max.store(max.max(1), Ordering::SeqCst);
    }

    pub(crate) fn max(&self) -> usize {
        self.max.load(Ordering::SeqCst)
    }

    /// The lock, whatever happened to a holder: nothing user-supplied runs under it, but a
    /// poisoned lock must never stop the inbound loop's `cancel` or `stream_lost`.
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Open>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// Registers subscription `id` of `type_name` with its decoded `query`, its parts and end to
    /// go to `reply_to`, pending its admission. `Err(max)` when the instance already serves
    /// `max` subscriptions (nothing registered). A second registration of the same id replaces
    /// (and stops) the first.
    ///
    /// The pump is started at once but writes nothing until the [`Admission`] is decided: what
    /// matching emits send meanwhile is queued, in order, and goes out first once it is
    /// [`admit`](Admission::admit)ted; [`refuse`](Admission::refuse) drops it and writes the
    /// refusal instead. A hub `Cancel` or a lost stream in between removes the entry, and
    /// nothing is written at all.
    pub(crate) fn register(
        self: &Arc<Self>,
        id: &str,
        type_name: &str,
        query: Box<dyn Any + Send + Sync>,
        payload: Vec<u8>,
        reply_to: ReplyTo,
    ) -> Result<Admission, usize> {
        let mut open = self.lock();
        let max = self.max();
        if open.len() >= max && !open.contains_key(id) {
            return Err(max);
        }
        let serial = self.serials.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        let (gate, verdict) = oneshot::channel();
        let cancel = CancellationToken::new();
        let pump = tokio::spawn(pump(
            Pump {
                id: id.to_string(),
                serial,
                subscriptions: Arc::downgrade(self),
                reply_to: reply_to.clone(),
                cancel: cancel.clone(),
            },
            rx,
            verdict,
        ));
        let replaced = open.insert(
            id.to_string(),
            Open {
                serial,
                type_name: type_name.to_string(),
                query: Arc::from(query),
                payload: Arc::new(payload),
                stream: reply_to.stream(),
                seq: 0,
                tx,
                cancel,
                pump,
            },
        );
        if let Some(old) = replaced {
            tracing::debug!(
                request = id,
                "subscription registered again: the first one is dropped"
            );
            old.cancel.cancel();
        }
        Ok(Admission {
            subscriptions: Arc::clone(self),
            id: id.to_string(),
            serial,
            gate,
        })
    }

    /// Removes the entry `id` if it is still the one registered as `serial`, without stopping
    /// its pump.
    fn remove_entry(&self, id: &str, serial: u64) -> Option<Open> {
        let mut open = self.lock();
        match open.get(id) {
            Some(o) if o.serial == serial => open.remove(id),
            _ => None,
        }
    }

    /// Removes subscription `id` (a hub `Cancel`): nothing more is sent for it. False when there
    /// is none.
    pub(crate) fn cancel(&self, id: &str) -> bool {
        match self.lock().remove(id) {
            Some(open) => {
                open.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Removes every subscription that arrived on hub stream `stream` (the hub forgot them with
    /// the session). Returns how many.
    pub(crate) fn stream_lost(&self, stream: StreamId) -> usize {
        let mut open = self.lock();
        let lost: Vec<String> = open
            .iter()
            .filter(|(_, o)| o.stream == stream)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &lost {
            if let Some(o) = open.remove(id) {
                o.cancel.cancel();
            }
        }
        lost.len()
    }

    /// Completes every subscription cleanly (shutdown): each gets a `ReplyEnd`, best effort.
    /// Returns the pumps, so the caller can wait (bounded) for the ends to be written.
    pub(crate) fn end_all(&self) -> Vec<JoinHandle<()>> {
        let mut open = self.lock();
        open.drain()
            .map(|(_, o)| {
                let _ = o.tx.send(Outgoing::End {
                    error: None,
                    parts: o.seq,
                });
                o.pump
            })
            .collect()
    }

    /// Sends `outgoing` to every subscription of `Q` whose query passes `filter`, in call order
    /// per subscription; with `remove`, the subscription is taken out first (an end). Returns
    /// how many matched.
    ///
    /// The filters (user code, and a decode when typed with another struct) run on a snapshot,
    /// outside the lock: a filter may call the emitter again, and one that panics skips its
    /// subscription (logged) instead of poisoning the lock the inbound loop needs.
    fn deliver<Q: SubscriptionQuery>(
        &self,
        filter: &dyn Fn(&Q) -> bool,
        outgoing: impl Fn(u64) -> Outgoing,
        remove: bool,
    ) -> usize {
        let candidates: Vec<Candidate> = self
            .lock()
            .iter()
            .filter(|(_, o)| o.type_name == Q::TYPE)
            .map(|(id, o)| Candidate {
                id: id.clone(),
                serial: o.serial,
                query: Arc::clone(&o.query),
                payload: Arc::clone(&o.payload),
            })
            .collect();
        let matching: Vec<Candidate> = candidates
            .into_iter()
            .filter(
                |c| match catch_unwind(AssertUnwindSafe(|| c.matches(filter))) {
                    Ok(matched) => matched,
                    Err(_) => {
                        tracing::warn!(
                            r#type = Q::TYPE,
                            request = %c.id,
                            "an emitter filter panicked: the subscription is skipped"
                        );
                        false
                    }
                },
            )
            .collect();
        let mut open = self.lock();
        let mut sent = 0;
        for candidate in &matching {
            // Cancelled, ended or replaced meanwhile: it is not this call's to send to.
            let Some(o) = open.get(&candidate.id) else {
                continue;
            };
            if o.serial != candidate.serial {
                continue;
            }
            sent += 1;
            if remove {
                if let Some(o) = open.remove(&candidate.id) {
                    let _ = o.tx.send(outgoing(o.seq));
                }
            } else if let Some(o) = open.get_mut(&candidate.id) {
                o.seq += 1;
                let _ = o.tx.send(outgoing(o.seq));
            }
        }
        sent
    }
}

/// How a pending subscription's admission ended: `Err` is the refusal to write.
type Verdict = Result<(), proto::Error>;

/// The decision on a registered, pending subscription. Dropped undecided (its task was aborted),
/// the subscription is removed: it must not wait for ever.
pub(crate) struct Admission {
    subscriptions: Arc<Subscriptions>,
    id: String,
    serial: u64,
    gate: oneshot::Sender<Verdict>,
}

impl Admission {
    /// The subscription is admitted: what was queued meanwhile goes out, then later emits.
    pub(crate) fn admit(self) {
        let _ = self.gate.send(Ok(()));
    }

    /// The subscription is refused: it is removed, what was queued is dropped and `error` is
    /// the only thing written (nothing, if the hub cancelled it meanwhile).
    pub(crate) fn refuse(self, error: proto::Error) {
        self.subscriptions.remove_entry(&self.id, self.serial);
        let _ = self.gate.send(Err(error));
    }
}

/// What one subscription's pump knows of its subscription.
struct Pump {
    id: String,
    serial: u64,
    subscriptions: Weak<Subscriptions>,
    reply_to: ReplyTo,
    cancel: CancellationToken,
}

impl Pump {
    /// This subscription is over without anyone having removed it (its stream is gone, or its
    /// admission was abandoned): it must not hold its slot.
    fn abandon(&self) {
        if let Some(subscriptions) = self.subscriptions.upgrade() {
            subscriptions.remove_entry(&self.id, self.serial);
        }
    }
}

/// Writes subscription `id`'s parts and end to its arrival stream in order, once admitted, until
/// the end, the cancel, or a write that fails (the stream is gone: the entry is removed).
async fn pump(
    pump: Pump,
    mut rx: mpsc::UnboundedReceiver<Outgoing>,
    verdict: oneshot::Receiver<Verdict>,
) {
    let Pump {
        id,
        reply_to,
        cancel,
        ..
    } = &pump;
    let mut refusal = None;
    let verdict = tokio::select! {
        _ = cancel.cancelled() => return,
        verdict = verdict => verdict,
    };
    match verdict {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            // What was queued is not sent: the subscription never started.
            rx.close();
            refusal = Some(Outgoing::End {
                error: Some(error),
                parts: 0,
            });
        }
        Err(_) => {
            pump.abandon();
            return;
        }
    }
    loop {
        let next = match refusal.take() {
            Some(end) => Some(end),
            None => tokio::select! {
                _ = cancel.cancelled() => return,
                next = rx.recv() => next,
            },
        };
        let Some(outgoing) = next else {
            return;
        };
        if cancel.is_cancelled() {
            return;
        }
        let (message, last) = match outgoing {
            Outgoing::Part { payload, seq } => (
                client_message::Message::Part(proto::ReplyPart {
                    request_id: id.clone(),
                    payload,
                    seq,
                    ..Default::default()
                }),
                false,
            ),
            Outgoing::End { error, parts } => (
                client_message::Message::End(proto::ReplyEnd {
                    request_id: id.clone(),
                    error: error.map(|mut e| {
                        e.request_id = id.clone();
                        e
                    }),
                    parts: u32::try_from(parts).unwrap_or(u32::MAX),
                    ..Default::default()
                }),
                true,
            ),
        };
        let msg = ClientMessage {
            message: Some(message),
        };
        if let Err(e) = reply_to.send(msg).await {
            tracing::debug!(request = %id, stream = reply_to.stream(), "subscription message dropped, subscription abandoned: {e}");
            pump.abandon();
            return;
        }
        if last {
            return;
        }
    }
}

/// Pushes updates to the subscriptions of this instance (ADR 0017, P4c). Obtained from
/// `KrakenMessaging::emitter()` (or the builder's, so handlers can capture it) or from
/// `Dispatcher::emitter()`; clones share the same subscriptions.
///
/// Every call reaches every open subscription of `Q` on this instance whose query passes the
/// filter, in call order: [`emit`](Self::emit) sends one `ReplyPart` per subscription (the update
/// serialized with the crate's codec), [`complete`](Self::complete) a clean `ReplyEnd` and
/// removes it, [`complete_exceptionally`](Self::complete_exceptionally) a `ReplyEnd{HANDLER_ERROR,
/// message}` and removes it. With no matching subscription a call is a no-op. The messages are
/// written by a task per subscription, so a call never waits for the hub stream.
///
/// There is no transaction to defer to here (the Kotlin library emits after the commit of the
/// surrounding Spring transaction automatically): a handler that writes in a Mongo transaction
/// emits after its commit, or the subscribers see an update of a write that may roll back.
#[derive(Clone)]
pub struct QueryUpdateEmitter {
    inner: Arc<Subscriptions>,
}

impl QueryUpdateEmitter {
    pub(crate) fn new(max: usize) -> QueryUpdateEmitter {
        QueryUpdateEmitter {
            inner: Arc::new(Subscriptions::new(max)),
        }
    }

    pub(crate) fn from_inner(inner: Arc<Subscriptions>) -> QueryUpdateEmitter {
        QueryUpdateEmitter { inner }
    }

    pub(crate) fn inner(&self) -> &Arc<Subscriptions> {
        &self.inner
    }

    /// One `ReplyPart{payload = update}` to every open subscription of `Q` whose query passes
    /// `filter`. An update that does not serialize is logged and sent to nobody.
    pub fn emit<Q: SubscriptionQuery>(&self, filter: impl Fn(&Q) -> bool, update: &Q::Update) {
        let payload = match codec::encode_reply(update) {
            Ok(payload) => payload,
            Err(e) => {
                tracing::error!(
                    r#type = Q::TYPE,
                    "update not emitted, it does not serialize: {e}"
                );
                return;
            }
        };
        let sent = self.inner.deliver::<Q>(
            &filter,
            |seq| Outgoing::Part {
                payload: payload.clone(),
                seq,
            },
            false,
        );
        tracing::trace!(r#type = Q::TYPE, subscriptions = sent, "update emitted");
    }

    /// A clean `ReplyEnd` to every open subscription of `Q` whose query passes `filter`; each is
    /// removed (nothing more can be emitted to it). At the hub a clean end removes this target
    /// from the subscription silently; the last target's clean end completes it for the
    /// subscriber.
    pub fn complete<Q: SubscriptionQuery>(&self, filter: impl Fn(&Q) -> bool) {
        let ended =
            self.inner
                .deliver::<Q>(&filter, |parts| Outgoing::End { error: None, parts }, true);
        tracing::trace!(
            r#type = Q::TYPE,
            subscriptions = ended,
            "subscriptions completed"
        );
    }

    /// A `ReplyEnd{error = HANDLER_ERROR, message}` to every open subscription of `Q` whose query
    /// passes `filter`; each is removed. At the hub an error end ends the whole subscription with
    /// that error for the subscriber (`MessagingError::RemoteHandler`) and cancels its other
    /// targets.
    pub fn complete_exceptionally<Q: SubscriptionQuery>(
        &self,
        filter: impl Fn(&Q) -> bool,
        error: &MessagingError,
    ) {
        let details = match error {
            MessagingError::RemoteHandler { details, .. } => details.clone(),
            _ => vec![],
        };
        let message = error.to_string();
        let ended = self.inner.deliver::<Q>(
            &filter,
            |parts| Outgoing::End {
                error: Some(proto::Error {
                    request_id: String::new(),
                    code: ErrorCode::HandlerError as i32,
                    message: message.clone(),
                    details: details.clone(),
                }),
                parts,
            },
            true,
        );
        tracing::trace!(
            r#type = Q::TYPE,
            subscriptions = ended,
            "subscriptions failed: {message}"
        );
    }

    /// Subscriptions open on this instance now (every type).
    pub fn open_subscriptions(&self) -> usize {
        self.inner.len()
    }
}
