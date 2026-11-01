//! Outgoing commands and queries: one `Request` per call, correlated reply, timeout, error mapping.
//!
//! Failure split: a request that was never accepted by a handshaken stream fails with
//! `HubUnavailable` (the hub never saw it, a retry is always safe). One whose stream broke while
//! it still waited in that stream's outbound buffer was never written either: it goes to the next
//! hub with the same id (and fails with `HubUnavailable` if there is none). One the transport had
//! taken from the buffer fails with `HubConnectionLost` when its stream breaks (outcome unknown,
//! the handler may have run).
//!
//! With several hubs ([`HubPool`]) a request goes to one of them (round-robin, or rendezvous by
//! routing key); one that could not be written to that hub's stream is written to the next hub
//! with the same id (it was never sent), so `HubUnavailable` means no hub took it.
//!
//! Scatter-gather (ADR 0017, [`Sender::scatter_gather`]): one `Request` with
//! `mode = SCATTER_GATHER` to one hub whose ack lists `scatter-gather` (unkeyed round-robin over
//! those), answered by `ReplyPart`s in arrival order and one `ReplyEnd`. Its pending entry collects
//! the parts until the end; the local guard covers the whole gather. If its stream breaks before
//! the end, the gather fails with `HubConnectionLost` (parts already received are not kept).
//!
//! Streaming queries (ADR 0017 P4b, [`Sender::stream`]): one `Request` with `mode = STREAMING`
//! and the initial credit to one hub whose ack lists `streaming`, answered by one handler's
//! `ReplyPart`s and one `ReplyEnd`. The pending entry lives until the end; the caller's stream
//! refills credit as it consumes (`Flow`), and dropping it before the end sends `Cancel`. The
//! local guard is an idle guard (reset by every part), like the hub's idle timeout.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::Stream;
use futures::stream::{BoxStream, StreamExt};
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};

use crate::codec;
use crate::context::{MessageContext, hub_headers};
use crate::error::MessagingError;
use crate::proto::{
    self, ClientMessage, ErrorCode, RequestKind, RequestMode, client_message, reply,
};
use crate::retry::{self, RetryPolicy};
use crate::types::{Command, Query, StreamingQuery};

use super::connection::{Handshake, HubConnection, Inbound, SCATTER_GATHER, STREAMING, StreamId};
use super::pool::{HubPool, weak_probe};

/// The timeout sent to a hub older than protocol v2 when the caller chose none (today's default).
pub const LEGACY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Added to the hub's own deadline for the local guard, which only catches a hub that never answers.
pub const LOCAL_GUARD_MARGIN: Duration = Duration::from_secs(1);
/// Used for the guard when a v2 ack carries no maximum (never sent by the hub; defensive).
const FALLBACK_MAX_TIMEOUT_MS: u32 = 60_000;
/// The least of a call's budget a retry must have left once its wait is over (or the policy's
/// `initial_backoff`, when larger); with less the call ends with the last error at once.
pub const RETRY_MIN_REMAINING: Duration = Duration::from_millis(250);
/// The smallest `timeout_ms` sent for a timeout that was chosen (0 would mean "the hub decides").
const MIN_TIMEOUT: Duration = Duration::from_millis(1);
/// The credit window of a streaming query when the caller chose none (what the hub reads 0 as).
pub const DEFAULT_CREDIT: u32 = 64;
/// How long the `Cancel` of a dropped stream may wait for room in the hub stream's buffer.
const CANCEL_WRITE_TIMEOUT: Duration = Duration::from_secs(1);

/// `Request.timeout_ms` and the local guard for a request of `type_name` on a stream with
/// `handshake`, when the caller chose `chosen` (per call, or the configured default).
///
/// - Protocol v2: send the chosen timeout or 0 (the hub then applies its per-type override or its
///   default); guard with the hub's upper bound for the request, i.e. `min(chosen, override or
///   max)` or, when 0 was sent, `override or max`, plus [`LOCAL_GUARD_MARGIN`].
/// - Legacy hub: send the chosen timeout or [`LEGACY_REQUEST_TIMEOUT`]; guard with that plus the margin.
///
/// A zero `chosen` counts as none; a chosen timeout under a millisecond is sent as 1 ms, never as 0.
pub fn request_timing(
    handshake: &Handshake,
    type_name: &str,
    chosen: Option<Duration>,
) -> (u32, Duration) {
    let ms = |d: Duration| d.as_millis().min(u32::MAX as u128) as u32;
    let chosen = chosen.filter(|d| !d.is_zero()).map(|d| d.max(MIN_TIMEOUT));
    match handshake {
        Handshake::V2(ack) => {
            let sent = chosen.map(ms).unwrap_or(0);
            let max = if ack.max_timeout_ms == 0 {
                FALLBACK_MAX_TIMEOUT_MS
            } else {
                ack.max_timeout_ms
            };
            // A zero override is meaningless (the hub rejects it); fall back to the maximum.
            let limit = ack
                .type_timeouts_ms
                .get(type_name)
                .copied()
                .filter(|&ms| ms > 0)
                .unwrap_or(max);
            let bound = if sent > 0 { sent.min(limit) } else { limit };
            (
                sent,
                Duration::from_millis(bound.into()) + LOCAL_GUARD_MARGIN,
            )
        }
        Handshake::Legacy => {
            let timeout = chosen.unwrap_or(LEGACY_REQUEST_TIMEOUT);
            (ms(timeout), timeout + LOCAL_GUARD_MARGIN)
        }
    }
}

/// The timeout the caller chose: the per-call one, else the configured default. A zero duration
/// counts as not chosen (it would mean an instant local guard).
pub(crate) fn chosen_timeout(
    per_call: Option<Duration>,
    default: Option<Duration>,
) -> Option<Duration> {
    let set = |d: Option<Duration>| d.filter(|d| !d.is_zero());
    set(per_call).or(set(default))
}

#[derive(Clone, Debug, Default)]
pub struct SendOptions {
    /// Same key, same owning instance (consistent hashing on the hub); typically the aggregate id.
    pub routing_key: Option<String>,
    /// The request timeout for this call; sent to the hub as `timeout_ms`. Without one (and
    /// without `Config.hub_request_timeout`) a protocol v2 hub applies its per-type override or
    /// its default.
    pub timeout: Option<Duration>,
    /// Full header value (`Bearer <jwt>`) to send instead of the one forwarded from the current handler context.
    pub authorization: Option<String>,
    /// `Request.priority` for this call (higher is served first while the target's queue at the
    /// hub is not empty). Without one, `Config.hub_priorities` for the type, else 0.
    pub priority: Option<i32>,
    /// Retry policy for this call, in force as written (`Some(RetryPolicy::NONE)` switches a
    /// configured policy off). Without one, `Config.hub_retry` applies (no retry unless set). A
    /// policy with `max_attempts == 0` fails the call with `InvalidMessage` before anything is sent.
    pub retry: Option<RetryPolicy>,
}

/// What a [`Sender`] applies to every call unless [`SendOptions`] says otherwise.
#[derive(Clone, Debug)]
pub struct SenderSettings {
    /// `Config.hub_request_timeout`.
    pub default_timeout: Option<Duration>,
    /// Forward the current handler's bearer token.
    pub forward_jwt: bool,
    /// Default priority per type (`Config.hub_priorities`).
    pub priorities: HashMap<String, i32>,
    /// Default retry policy (`Config.hub_retry`); `None` (the default): no retry.
    pub retry: Option<RetryPolicy>,
}

impl Default for SenderSettings {
    fn default() -> Self {
        SenderSettings {
            default_timeout: None,
            forward_jwt: true,
            priorities: HashMap::new(),
            retry: None,
        }
    }
}

/// Options of one scatter-gather ([`Sender::scatter_gather`]).
#[derive(Clone, Debug, Default)]
pub struct ScatterOptions {
    /// The deadline of the whole gather, sent as `timeout_ms` (the rules of
    /// [`SendOptions::timeout`]: without one, `Config.hub_request_timeout`, else the hub's per-type
    /// override or default). Targets that have not answered by then count as `timed_out`.
    pub timeout: Option<Duration>,
    /// End the gather after this many successful parts; 0 waits for every target.
    pub max_results: u32,
    /// `Request.priority` of every target's request. Without one, `Config.hub_priorities` for the
    /// type, else 0.
    pub priority: Option<i32>,
}

/// Options of one streaming query ([`Sender::stream`]).
#[derive(Clone, Debug)]
pub struct StreamOptions {
    /// The idle timeout: the longest the hub waits for the next part (from the forward, reset by
    /// every part), sent as `timeout_ms` under the rules of [`SendOptions::timeout`] (without
    /// one, `Config.hub_request_timeout`, else the hub's per-type override or default). The
    /// local guard (the hub's bound plus [`LOCAL_GUARD_MARGIN`]) is an idle guard too.
    pub timeout: Option<Duration>,
    /// End the stream after this many parts (the hub cancels the handler); 0 is no limit.
    pub max_results: u32,
    /// `Request.priority` of the handler's request. Without one, `Config.hub_priorities` for
    /// the type, else 0.
    pub priority: Option<i32>,
    /// The credit window: parts the handler may send before the first refill (`initial_credit`);
    /// the stream refills half of it every time half was consumed. 0 reads as
    /// [`DEFAULT_CREDIT`] (64), the default.
    pub initial_credit: u32,
}

impl Default for StreamOptions {
    fn default() -> Self {
        StreamOptions {
            timeout: None,
            max_results: 0,
            priority: None,
            initial_credit: DEFAULT_CREDIT,
        }
    }
}

/// One target's answer to a scatter-gather.
#[derive(Debug)]
pub struct ScatterPart<R> {
    /// The service and instance that answered.
    pub service: String,
    pub instance: String,
    /// The decoded reply, or the target's failure (its handler's error, or a hub-side failure of
    /// that target such as `HANDLER_DISCONNECTED` or `RESOURCE_EXHAUSTED`).
    pub outcome: Result<R, RemoteError>,
}

/// Everything a scatter-gather got by its end.
#[derive(Debug)]
pub struct ScatterResult<R> {
    /// One per answer, in arrival order (failed ones included).
    pub parts: Vec<ScatterPart<R>>,
    /// Parts whose outcome is an error.
    pub failed: u32,
    /// Targets that had not answered by the deadline (no part for them).
    pub timed_out: u32,
}

/// The failure of one scatter-gather target, as the hub relayed it (or a reply this client could
/// not decode, with code `UNSPECIFIED`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message} ({code:?})")]
pub struct RemoteError {
    pub code: ErrorCode,
    pub message: String,
    pub details: Vec<u8>,
}

impl RemoteError {
    /// The error a unary query of `type_name` would have failed with.
    pub fn into_error(self, type_name: &str) -> MessagingError {
        MessagingError::from_hub(type_name, self.code as i32, &self.message, &self.details)
    }
}

/// A `ReplyPart` as the caller sees it: the payload decoded with the unary reply codec.
fn decode_part<R: DeserializeOwned + 'static>(part: proto::ReplyPart) -> ScatterPart<R> {
    let outcome = match part.error {
        Some(e) => Err(RemoteError {
            code: ErrorCode::try_from(e.code).unwrap_or(ErrorCode::Unspecified),
            message: e.message,
            details: e.details,
        }),
        None => codec::decode_reply::<R>(&part.payload).map_err(|e| RemoteError {
            code: ErrorCode::Unspecified,
            message: format!(
                "cannot decode the answer of {}/{}: {e}",
                part.responder_service, part.responder_instance
            ),
            details: vec![],
        }),
    };
    ScatterPart {
        service: part.responder_service,
        instance: part.responder_instance,
        outcome,
    }
}

/// The result of a gather from its parts (in arrival order) and its end.
fn assemble<R: DeserializeOwned + 'static>(
    parts: Vec<proto::ReplyPart>,
    end: Option<&proto::ReplyEnd>,
) -> ScatterResult<R> {
    let parts: Vec<ScatterPart<R>> = parts.into_iter().map(decode_part).collect();
    let failed = parts.iter().filter(|p| p.outcome.is_err()).count() as u32;
    ScatterResult {
        parts,
        failed,
        timed_out: end.map_or(0, |e| e.timed_out),
    }
}

/// The time all attempts of one call share: the caller's chosen timeout; else, against a v2 hub,
/// its bound for the type (the per-type override, else its default); else (legacy hub, or not
/// connected yet) [`LEGACY_REQUEST_TIMEOUT`].
pub fn retry_budget(
    handshake: Option<&Handshake>,
    type_name: &str,
    chosen: Option<Duration>,
) -> Duration {
    if let Some(chosen) = chosen.filter(|d| !d.is_zero()) {
        return chosen;
    }
    handshake
        .and_then(Handshake::ack)
        .and_then(|ack| {
            ack.type_timeouts_ms
                .get(type_name)
                .copied()
                .filter(|&ms| ms > 0)
                .or(Some(ack.default_timeout_ms).filter(|&ms| ms > 0))
        })
        .map(|ms| Duration::from_millis(ms.into()))
        .unwrap_or(LEGACY_REQUEST_TIMEOUT)
}

/// The parts of a call that stay the same across its attempts.
struct Attempt<'a> {
    kind: RequestKind,
    type_name: &'a str,
    routing_key: &'a str,
    headers: &'a HashMap<String, String>,
    priority: i32,
}

/// How a pending request ended.
enum Outcome {
    Done(Result<Vec<u8>, MessagingError>),
    /// Its stream was lost while the request was still in the outbound buffer: never written, it
    /// may go to another hub with the same id.
    NotWritten,
}

/// Who waits for a pending request.
enum Waiter {
    /// A unary request: its one outcome.
    Unary(oneshot::Sender<Outcome>),
    /// A scatter-gather: its parts, then its end (or how it failed).
    Scatter(mpsc::UnboundedSender<GatherEvent>),
    /// A streaming query: its parts, then its end (or how it failed).
    Stream(mpsc::UnboundedSender<GatherEvent>),
}

impl Waiter {
    /// Ends the wait with `outcome`.
    fn finish(self, outcome: Outcome) {
        match self {
            Waiter::Unary(tx) => {
                let _ = tx.send(outcome);
            }
            Waiter::Scatter(tx) | Waiter::Stream(tx) => {
                let _ = tx.send(GatherEvent::Ended(outcome));
            }
        }
    }

    /// The channel of a multi-part request (parts and end).
    fn parts(&self) -> Option<&mpsc::UnboundedSender<GatherEvent>> {
        match self {
            Waiter::Unary(_) => None,
            Waiter::Scatter(tx) | Waiter::Stream(tx) => Some(tx),
        }
    }
}

enum GatherEvent {
    Part(proto::ReplyPart),
    End(proto::ReplyEnd),
    /// Failed as a whole (an `Error`, the stream lost), or not written ([`Outcome::NotWritten`]).
    Ended(Outcome),
}

/// How often one attempt moves to another hub because its stream was lost before the request was
/// written; past that it fails with `HubUnavailable` (nothing was written).
const MAX_UNWRITTEN_MOVES: usize = 8;

/// Removes a request's pending entry when the call ends, however it ends: a reply, an error, the
/// local guard, or the caller dropping the future (while the write is blocked or while it awaits
/// the answer). Removing an entry that was already taken is a no-op.
struct ForgetOnDrop<'a> {
    pending: &'a Mutex<HashMap<String, Entry>>,
    id: &'a str,
}

/// What stays the same across the hubs and attempts of one scatter-gather.
struct ScatterCall {
    type_name: &'static str,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
    priority: i32,
    max_results: u32,
}

/// One scatter-gather written (or being written) to a hub: its parts as they arrive, then the
/// end. Dropping it forgets the request, from before the write on (later parts are ignored).
struct Gathering<'a> {
    sender: &'a Sender,
    call: Arc<ScatterCall>,
    chosen: Option<Duration>,
    id: String,
    events: mpsc::UnboundedReceiver<GatherEvent>,
    /// The local guard: the hub's bound for the gather plus [`LOCAL_GUARD_MARGIN`].
    deadline: tokio::time::Instant,
    moves: usize,
    end: Option<proto::ReplyEnd>,
    done: bool,
}

impl Drop for Gathering<'_> {
    fn drop(&mut self) {
        self.sender.take(&self.id);
    }
}

impl Gathering<'_> {
    /// The next part; `None` once the end arrived. An error ends the gather.
    async fn next(&mut self) -> Option<Result<proto::ReplyPart, MessagingError>> {
        if self.done {
            return None;
        }
        loop {
            let event = match tokio::time::timeout_at(self.deadline, self.events.recv()).await {
                Ok(Some(event)) => event,
                Ok(None) => GatherEvent::Ended(Outcome::Done(Err(MessagingError::HubUnavailable(
                    "sender closed".into(),
                )))),
                // The hub ends a gather at its deadline itself; the guard covers a hub that never does.
                Err(_) => GatherEvent::Ended(Outcome::Done(Err(MessagingError::Timeout(
                    self.call.type_name.to_string(),
                )))),
            };
            match event {
                GatherEvent::Part(part) => return Some(Ok(part)),
                GatherEvent::End(end) => {
                    self.done = true;
                    self.end = Some(end);
                    return None;
                }
                GatherEvent::Ended(Outcome::NotWritten) => {
                    self.moves += 1;
                    if self.moves > MAX_UNWRITTEN_MOVES {
                        return self.fail(MessagingError::HubUnavailable(format!(
                            "'{}' not written: {} hub streams lost before it was",
                            self.call.type_name, self.moves
                        )));
                    }
                    tracing::debug!(r#type = self.call.type_name, request = %self.id, "stream lost before the scatter-gather was written; trying the next hub");
                    match self
                        .sender
                        .write_gather(&self.id, &self.call, self.chosen)
                        .await
                    {
                        Ok((events, guard)) => {
                            self.events = events;
                            self.deadline = tokio::time::Instant::now() + guard;
                        }
                        Err(e) => return self.fail(e),
                    }
                }
                GatherEvent::Ended(Outcome::Done(Err(e))) => return self.fail(e),
                GatherEvent::Ended(Outcome::Done(Ok(_))) => {
                    unreachable!("a scatter-gather never ends with a reply")
                }
            }
        }
    }

    fn fail(&mut self, e: MessagingError) -> Option<Result<proto::ReplyPart, MessagingError>> {
        self.done = true;
        Some(Err(e))
    }
}

/// What stays the same across the hubs and attempts of one streaming query.
struct StreamCall {
    type_name: &'static str,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
    priority: i32,
    max_results: u32,
    /// The credit window (`initial_credit`, never 0).
    window: u32,
}

impl StreamCall {
    /// The credit given back every time this many parts were consumed: half the window, at
    /// least one.
    fn refill(&self) -> u32 {
        (self.window / 2).max(1)
    }
}

/// One streaming query written (or being written) to a hub: its parts as they arrive, then the
/// end. Dropping it before the end cancels the stream at the hub (once written) and forgets the
/// request, from before the write on.
struct Streaming<'a> {
    sender: &'a Sender,
    call: Arc<StreamCall>,
    chosen: Option<Duration>,
    id: String,
    /// The hub stream the request was written to: `Flow` and `Cancel` go there.
    stream: StreamId,
    events: mpsc::UnboundedReceiver<GatherEvent>,
    /// The idle guard: the hub's bound for the request plus [`LOCAL_GUARD_MARGIN`].
    guard: Duration,
    moves: usize,
    written: bool,
    done: bool,
    /// Parts consumed since the last refill.
    consumed: u32,
}

impl Drop for Streaming<'_> {
    fn drop(&mut self) {
        if self.written && !self.done {
            self.sender
                .cancel_later(self.stream, &self.id, "dropped by the sender");
        }
        self.sender.take(&self.id);
    }
}

impl Streaming<'_> {
    /// The next part; `None` once the end arrived. An error ends the stream (the hub's
    /// `ReplyEnd{error}` as the unary error of its code).
    async fn next(&mut self) -> Option<Result<proto::ReplyPart, MessagingError>> {
        if self.done {
            return None;
        }
        loop {
            let event = match tokio::time::timeout(self.guard, self.events.recv()).await {
                Ok(Some(event)) => event,
                Ok(None) => GatherEvent::Ended(Outcome::Done(Err(MessagingError::HubUnavailable(
                    "sender closed".into(),
                )))),
                // The hub ends an idle stream itself; the guard covers a hub that never does.
                Err(_) => GatherEvent::Ended(Outcome::Done(Err(MessagingError::Timeout(
                    self.call.type_name.to_string(),
                )))),
            };
            match event {
                GatherEvent::Part(part) => return Some(Ok(part)),
                GatherEvent::End(end) => {
                    self.done = true;
                    return match end.error {
                        Some(e) => Some(Err(MessagingError::from_hub(
                            self.call.type_name,
                            e.code,
                            &e.message,
                            &e.details,
                        ))),
                        None => None,
                    };
                }
                GatherEvent::Ended(Outcome::NotWritten) => {
                    self.moves += 1;
                    if self.moves > MAX_UNWRITTEN_MOVES {
                        return self.fail(MessagingError::HubUnavailable(format!(
                            "'{}' not written: {} hub streams lost before it was",
                            self.call.type_name, self.moves
                        )));
                    }
                    self.written = false;
                    tracing::debug!(r#type = self.call.type_name, request = %self.id, "stream lost before the streaming query was written; trying the next hub");
                    match self
                        .sender
                        .write_stream(&self.id, &self.call, self.chosen)
                        .await
                    {
                        Ok((events, guard, stream)) => {
                            self.events = events;
                            self.guard = guard;
                            self.stream = stream;
                            self.written = true;
                        }
                        Err(e) => return self.fail(e),
                    }
                }
                GatherEvent::Ended(Outcome::Done(Err(e))) => return self.fail(e),
                GatherEvent::Ended(Outcome::Done(Ok(_))) => {
                    unreachable!("a streaming query never ends with a reply")
                }
            }
        }
    }

    /// Decodes `part` for the caller and counts it as consumed: once half the window was, the
    /// hub gets a `Flow` for that much. A part that does not decode ends the stream (the handler
    /// is cancelled).
    async fn item<R: DeserializeOwned + 'static>(
        &mut self,
        part: proto::ReplyPart,
    ) -> Result<R, MessagingError> {
        match codec::decode_reply::<R>(&part.payload) {
            Ok(item) => {
                self.consumed += 1;
                let refill = self.call.refill();
                if self.consumed >= refill {
                    self.consumed -= refill;
                    let flow = ClientMessage {
                        message: Some(client_message::Message::Flow(proto::Flow {
                            request_id: self.id.clone(),
                            credits: refill,
                        })),
                    };
                    if let Err(e) = self.sender.hubs.send_on(self.stream, flow).await {
                        tracing::debug!(request = %self.id, "flow not written: {e}");
                    }
                }
                Ok(item)
            }
            Err(e) => {
                tracing::debug!(request = %self.id, seq = part.seq, "part of '{}' from {}/{} cannot be decoded: {e}", self.call.type_name, part.responder_service, part.responder_instance);
                self.cancel("undecodable part").await;
                Err(e)
            }
        }
    }

    /// Ends the stream here and cancels it at the hub (the handler is told).
    async fn cancel(&mut self, reason: &str) {
        self.done = true;
        self.sender.take(&self.id);
        let cancel = ClientMessage {
            message: Some(client_message::Message::Cancel(proto::Cancel {
                request_id: self.id.clone(),
                reason: reason.to_string(),
            })),
        };
        if let Err(e) = self.sender.hubs.send_on(self.stream, cancel).await {
            tracing::debug!(request = %self.id, "cancel not written: {e}");
        }
    }

    fn fail(&mut self, e: MessagingError) -> Option<Result<proto::ReplyPart, MessagingError>> {
        self.done = true;
        Some(Err(e))
    }
}

/// Where a [`Sender::scatter_gather_stream`] is.
enum StreamState<'a> {
    Start(Result<Arc<ScatterCall>, MessagingError>),
    Open(Gathering<'a>),
    Done,
}

impl Drop for ForgetOnDrop<'_> {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(self.id);
        }
    }
}

struct Entry {
    type_name: String,
    /// The stream the request was written to.
    stream: StreamId,
    waiter: Waiter,
}

/// The `CommandBus`/`QueryBus` of the Rust side. Replies arrive through [`Sender::on_inbound`].
pub struct Sender {
    hubs: Arc<HubPool>,
    pending: Mutex<HashMap<String, Entry>>,
    default_timeout: Option<Duration>,
    forward_jwt: bool,
    priorities: HashMap<String, i32>,
    retry: Option<RetryPolicy>,
}

impl Sender {
    pub fn new(
        conn: Arc<HubConnection>,
        default_timeout: Option<Duration>,
        forward_jwt: bool,
    ) -> Arc<Sender> {
        Self::with_settings(
            conn,
            SenderSettings {
                default_timeout,
                forward_jwt,
                ..SenderSettings::default()
            },
        )
    }

    /// A sender over one connection the caller owns.
    pub fn with_settings(conn: Arc<HubConnection>, settings: SenderSettings) -> Arc<Sender> {
        Self::with_pool(HubPool::of(conn), settings)
    }

    /// A sender over every hub of `hubs`.
    pub fn with_pool(hubs: Arc<HubPool>, settings: SenderSettings) -> Arc<Sender> {
        let sender = Arc::new(Sender {
            hubs: Arc::clone(&hubs),
            pending: Mutex::new(HashMap::new()),
            default_timeout: settings.default_timeout,
            forward_jwt: settings.forward_jwt,
            priorities: settings.priorities,
            retry: settings.retry,
        });
        hubs.add_pending_probe(weak_probe(Arc::downgrade(&sender), Sender::pending_on));
        sender
    }

    /// True while a request written to `stream` is unanswered.
    fn pending_on(&self, stream: StreamId) -> bool {
        self.pending
            .lock()
            .unwrap()
            .values()
            .any(|e| e.stream == stream)
    }

    pub async fn send_command<C: Command>(
        &self,
        command: &C,
        opts: SendOptions,
    ) -> Result<C::Reply, MessagingError> {
        let bytes = self
            .request(RequestKind::Command, C::TYPE, codec::encode(command)?, opts)
            .await?;
        codec::decode_reply(&bytes)
    }

    pub async fn send_query<Q: Query>(
        &self,
        query: &Q,
        opts: SendOptions,
    ) -> Result<Q::Reply, MessagingError> {
        let bytes = self
            .request(RequestKind::Query, Q::TYPE, codec::encode(query)?, opts)
            .await?;
        codec::decode_reply(&bytes)
    }

    /// Completes the pending request a `Reply`/`Error` refers to; a `Disconnected` fails the requests
    /// written to that stream with `HubConnectionLost`.
    pub fn on_inbound(&self, msg: Inbound) {
        match msg {
            Inbound::Reply(r) => {
                // A scatter-gather is answered by parts and an end, never by a reply.
                let unary = self.take_if(&r.request_id, |w| matches!(w, Waiter::Unary(_)));
                if let Some(Entry {
                    type_name, waiter, ..
                }) = unary
                {
                    let result = match r.result {
                        Some(reply::Result::Payload(p)) => Ok(p),
                        Some(reply::Result::Error(e)) => Err(MessagingError::from_hub(
                            &type_name, e.code, &e.message, &e.details,
                        )),
                        None => Ok(b"{}".to_vec()),
                    };
                    waiter.finish(Outcome::Done(result));
                }
            }
            Inbound::Error(e) => {
                if let Some(Entry {
                    type_name, waiter, ..
                }) = self.take(&e.request_id)
                {
                    waiter.finish(Outcome::Done(Err(MessagingError::from_hub(
                        &type_name, e.code, &e.message, &e.details,
                    ))));
                }
            }
            // Parts and ends of an unknown request (or of a unary one) are ignored.
            Inbound::Part(p) => {
                let pending = self.pending.lock().unwrap();
                if let Some(tx) = pending.get(&p.request_id).and_then(|e| e.waiter.parts()) {
                    let _ = tx.send(GatherEvent::Part(p));
                }
            }
            Inbound::End(end) => {
                let multi = self.take_if(&end.request_id, |w| w.parts().is_some());
                if let Some(tx) = multi.as_ref().and_then(|e| e.waiter.parts()) {
                    let _ = tx.send(GatherEvent::End(end));
                }
            }
            Inbound::Disconnected { stream, unsent } => {
                let lost: Vec<(String, Entry)> = {
                    let mut pending = self.pending.lock().unwrap();
                    let ids: Vec<String> = pending
                        .iter()
                        .filter(|(_, e)| e.stream == stream)
                        .map(|(id, _)| id.clone())
                        .collect();
                    ids.into_iter()
                        .filter_map(|id| pending.remove(&id).map(|e| (id, e)))
                        .collect()
                };
                for (id, e) in lost {
                    let outcome = if unsent.contains(&id) {
                        Outcome::NotWritten
                    } else {
                        Outcome::Done(Err(MessagingError::HubConnectionLost(format!(
                            "hub stream closed before the reply to '{}'; the outcome is unknown",
                            e.type_name
                        ))))
                    };
                    e.waiter.finish(outcome);
                }
            }
            // The dispatcher's business.
            Inbound::Request { .. } | Inbound::Flow(_) | Inbound::Cancel(_) => {}
        }
    }

    /// One logical call: the first attempt, plus retries the policy and the classification allow,
    /// each with a fresh request id, all within one time budget ([`retry_budget`]). A retry starts
    /// only if, once its wait is over, at least `max(initial_backoff, RETRY_MIN_REMAINING)` of the
    /// budget is left; otherwise the last error is returned at once, without waiting. Each attempt
    /// sends what is left of the budget at its start (at least 1 ms) as `timeout_ms`.
    async fn request(
        &self,
        kind: RequestKind,
        type_name: &str,
        payload: Vec<u8>,
        opts: SendOptions,
    ) -> Result<Vec<u8>, MessagingError> {
        let policy = opts.retry.or_else(|| self.retry.clone());
        let headers = self.headers(opts.authorization.clone());
        let priority = self.priority(type_name, opts.priority);
        let chosen = chosen_timeout(opts.timeout, self.default_timeout);
        let routing_key = opts.routing_key.unwrap_or_default();
        let call = Attempt {
            kind,
            type_name,
            routing_key: &routing_key,
            headers: &headers,
            priority,
        };
        self.retrying(kind, type_name, policy, chosen, &routing_key, |timeout| {
            self.attempt(&call, &payload, timeout)
        })
        .await
    }

    /// `priority` for this call, else `Config.hub_priorities` for the type, else 0.
    fn priority(&self, type_name: &str, priority: Option<i32>) -> i32 {
        priority
            .or_else(|| self.priorities.get(type_name).copied())
            .unwrap_or(0)
    }

    /// Runs `attempt` under `policy` (see [`request`](Self::request)): without a policy (or with
    /// one attempt) once with the `chosen` timeout; otherwise each attempt gets what is left of the
    /// budget.
    async fn retrying<T, F, Fut>(
        &self,
        kind: RequestKind,
        type_name: &str,
        policy: Option<RetryPolicy>,
        chosen: Option<Duration>,
        routing_key: &str,
        mut attempt: F,
    ) -> Result<T, MessagingError>
    where
        F: FnMut(Option<Duration>) -> Fut,
        Fut: Future<Output = Result<T, MessagingError>>,
    {
        if policy.as_ref().is_some_and(|p| p.max_attempts == 0) {
            return Err(MessagingError::InvalidMessage(format!(
                "retry policy for '{type_name}' has max_attempts 0; it must be at least 1 (1 = no retry)"
            )));
        }
        let policy = match policy {
            Some(p) if p.max_attempts > 1 => p,
            _ => return attempt(chosen).await,
        };
        let attempts = policy.max_attempts;

        let budget = retry_budget(
            self.hubs.handshake_for(routing_key).as_ref(),
            type_name,
            chosen,
        );
        let started = tokio::time::Instant::now();
        let min_remaining = policy.initial_backoff.max(RETRY_MIN_REMAINING);
        // The first attempt is never gated and gets the whole budget.
        let mut remaining = budget.saturating_sub(started.elapsed());
        let mut n = 1;
        loop {
            let timeout = remaining.max(MIN_TIMEOUT);
            let error = match attempt(Some(timeout)).await {
                Ok(value) => return Ok(value),
                Err(e) => e,
            };
            if n >= attempts || !retry::is_retryable(kind, &error) {
                return Err(error);
            }
            let wait = policy.jittered_backoff(n);
            // What the next attempt would have, from the moment its wait is over.
            remaining = match budget.checked_sub(started.elapsed() + wait) {
                Some(left) if left >= min_remaining => left,
                _ => {
                    tracing::debug!(
                        r#type = type_name,
                        attempt = n,
                        "no room left in the budget for a retry: {error}"
                    );
                    return Err(error);
                }
            };
            tracing::debug!(r#type = type_name, attempt = n, retry_in = ?wait, "retrying: {error}");
            tokio::time::sleep(wait).await;
            n += 1;
        }
    }

    /// Asks every live instance of every owner of `Q` (ADR 0017) and collects their answers until
    /// the hub's `ReplyEnd`: one [`ScatterPart`] per answer in arrival order, with the counts of
    /// failed parts and of targets that timed out. Partial answers are answers: a gather where
    /// some targets failed or timed out still succeeds.
    ///
    /// Sent to one hub whose `RegisterAck` lists `scatter-gather` (unkeyed round-robin over
    /// those; the capability is checked again on the stream the gather is written to, so a hub
    /// re-handshaken without it in between is skipped). Fails at once, without sending anything,
    /// with [`MessagingError::UnsupportedByHub`] when no hub lists it (a P3 or older hub, or a
    /// legacy hub). Before any target: no live target is `NoHandler`, a command type `InvalidMessage`, a
    /// hub `UNSUPPORTED` answer `UnsupportedByHub`. A hub stream lost before the end fails the
    /// call with `HubConnectionLost` (the parts received are dropped), the local guard (the hub's
    /// bound plus [`LOCAL_GUARD_MARGIN`]) with `Timeout`.
    ///
    /// `Config.hub_retry` applies to the whole gather, for the same errors as a unary query: a
    /// retry sends a new gather with a fresh id and starts collecting from scratch.
    pub async fn scatter_gather<Q: Query>(
        &self,
        query: &Q,
        options: ScatterOptions,
    ) -> Result<ScatterResult<Q::Reply>, MessagingError> {
        let call = Arc::new(self.scatter_call(Q::TYPE, codec::encode(query)?, &options));
        let chosen = chosen_timeout(options.timeout, self.default_timeout);
        let call = &call;
        self.retrying(
            RequestKind::Query,
            Q::TYPE,
            self.retry.clone(),
            chosen,
            "",
            |timeout| async move {
                let mut gathering = self.open_gather(call, timeout).await?;
                let mut parts = Vec::new();
                while let Some(part) = gathering.next().await {
                    parts.push(part?);
                }
                Ok(assemble::<Q::Reply>(parts, gathering.end.as_ref()))
            },
        )
        .await
    }

    /// [`scatter_gather`](Self::scatter_gather) as a stream: each part as it arrives, completing
    /// on the hub's `ReplyEnd`. An error is the last item. `Config.hub_retry` applies until the
    /// first part arrives (a retry then starts a new gather); once a part was yielded, a failure
    /// ends the stream with that error. Dropping the stream abandons the gather (later parts are
    /// ignored).
    pub fn scatter_gather_stream<'a, Q: Query>(
        &'a self,
        query: &Q,
        options: ScatterOptions,
    ) -> impl Stream<Item = Result<ScatterPart<Q::Reply>, MessagingError>> + Send + use<'a, Q> {
        let call = codec::encode(query)
            .map(|payload| Arc::new(self.scatter_call(Q::TYPE, payload, &options)));
        let chosen = chosen_timeout(options.timeout, self.default_timeout);
        futures::stream::unfold(StreamState::Start(call), move |state| async move {
            match state {
                StreamState::Start(Err(e)) => Some((Err(e), StreamState::Done)),
                StreamState::Start(Ok(call)) => {
                    // Retried (whole new gathers) until the first part, the end or a final error.
                    let opened = self
                        .retrying(
                            RequestKind::Query,
                            Q::TYPE,
                            self.retry.clone(),
                            chosen,
                            "",
                            |timeout| {
                                let call = Arc::clone(&call);
                                async move {
                                    let mut gathering = self.open_gather(&call, timeout).await?;
                                    match gathering.next().await {
                                        Some(Ok(part)) => Ok(Some((gathering, part))),
                                        Some(Err(e)) => Err(e),
                                        None => Ok(None),
                                    }
                                }
                            },
                        )
                        .await;
                    match opened {
                        Ok(Some((gathering, part))) => {
                            Some((Ok(decode_part(part)), StreamState::Open(gathering)))
                        }
                        Ok(None) => None,
                        Err(e) => Some((Err(e), StreamState::Done)),
                    }
                }
                StreamState::Open(mut gathering) => match gathering.next().await {
                    Some(Ok(part)) => Some((Ok(decode_part(part)), StreamState::Open(gathering))),
                    Some(Err(e)) => Some((Err(e), StreamState::Done)),
                    None => None,
                },
                StreamState::Done => None,
            }
        })
    }

    /// Streams the result of `Q` from one capable instance of an owner (ADR 0017, P4b): one
    /// `Request` with `mode = STREAMING` to one hub whose `RegisterAck` lists `streaming` (unkeyed
    /// round-robin over those; the capability is checked again on the stream the request is
    /// written to), answered by the handler's parts under this caller's credits and one end.
    ///
    /// Opening waits for the first part (or the end, or a failure), so the errors before any part
    /// are the call's: [`MessagingError::UnsupportedByHub`] when no hub lists the capability
    /// (nothing is sent) or the hub answers `UNSUPPORTED` (no owner instance negotiated
    /// `streaming`), `NoHandler` without an owner, `InvalidMessage` for a command type, `Timeout`
    /// when the first part is not in time, the handler's error when it fails at once,
    /// `HubUnavailable` / `HubConnectionLost` as for a unary query. `Config.hub_retry` applies
    /// until then, for the same errors as a unary query (a retry is a new stream with a fresh
    /// id); after the first part nothing is retried.
    ///
    /// The returned stream yields the items in the handler's order and ends on the hub's
    /// `ReplyEnd`; a `ReplyEnd{error}` (the idle timeout, the handler's error or its leaving, the
    /// hub's limits: `Timeout`, `RemoteHandler`, `HandlerDisconnected`, `Cancelled`, ...), a lost
    /// hub stream (`HubConnectionLost`) or a part that does not decode (`Serialization`; the
    /// handler is cancelled) is its last item. It refills credit as it is consumed: half the
    /// window ([`StreamOptions::initial_credit`]) every time half of it was pulled, so a handler
    /// never runs more than a window (plus the one item it may hold) ahead of the consumer.
    /// Dropping it before the end cancels
    /// the stream at the hub (the handler is told) and forgets the request.
    pub async fn stream<'a, Q: StreamingQuery>(
        &'a self,
        query: &Q,
        options: StreamOptions,
    ) -> Result<BoxStream<'a, Result<Q::Item, MessagingError>>, MessagingError> {
        let call = Arc::new(StreamCall {
            type_name: Q::TYPE,
            payload: codec::encode(query)?,
            headers: self.headers(None),
            priority: self.priority(Q::TYPE, options.priority),
            max_results: options.max_results,
            window: if options.initial_credit == 0 {
                DEFAULT_CREDIT
            } else {
                options.initial_credit
            },
        });
        let chosen = chosen_timeout(options.timeout, self.default_timeout);
        // Retried (new streams, fresh ids) until the first part, the end or a final error. The
        // idle timeout sent is the chosen one on every attempt; the budget bounds the retries.
        let opened = self
            .retrying(
                RequestKind::Query,
                Q::TYPE,
                self.retry.clone(),
                chosen,
                "",
                |_| {
                    let call = Arc::clone(&call);
                    async move {
                        let mut streaming = self.open_stream(&call, chosen).await?;
                        match streaming.next().await {
                            Some(Ok(part)) => Ok(Some((streaming, part))),
                            Some(Err(e)) => Err(e),
                            None => Ok(None),
                        }
                    }
                },
            )
            .await?;
        let Some((streaming, first)) = opened else {
            return Ok(futures::stream::empty().boxed());
        };
        let items = futures::stream::unfold(Some((streaming, Some(first))), |state| async move {
            let (mut streaming, first) = state?;
            let part = match first {
                Some(part) => part,
                None => match streaming.next().await {
                    Some(Ok(part)) => part,
                    Some(Err(e)) => return Some((Err(e), None)),
                    None => return None,
                },
            };
            match streaming.item::<Q::Item>(part).await {
                Ok(item) => Some((Ok(item), Some((streaming, None)))),
                Err(e) => Some((Err(e), None)),
            }
        });
        Ok(items.boxed())
    }

    /// Writes a new streaming query of `call` (fresh id) to a hub that shares the capability.
    async fn open_stream(
        &self,
        call: &Arc<StreamCall>,
        chosen: Option<Duration>,
    ) -> Result<Streaming<'_>, MessagingError> {
        // The streaming exists before the write: its drop forgets the pending entry (recorded by
        // the build, before the write) when the caller drops the future while the write still
        // waits for room in the stream's outbound buffer; nothing was written, so no cancel.
        let (_, events) = mpsc::unbounded_channel();
        let mut streaming = Streaming {
            sender: self,
            call: Arc::clone(call),
            chosen,
            id: uuid::Uuid::new_v4().to_string(),
            stream: 0,
            events,
            guard: Duration::ZERO,
            moves: 0,
            written: false,
            done: false,
            consumed: 0,
        };
        let (events, guard, stream) = self.write_stream(&streaming.id, call, chosen).await?;
        streaming.events = events;
        streaming.guard = guard;
        streaming.stream = stream;
        streaming.written = true;
        Ok(streaming)
    }

    /// Writes streaming query `id` to a hub whose ack lists `streaming`, its pending entry
    /// recorded while the stream is current. Returns its events, the idle guard and the stream.
    async fn write_stream(
        &self,
        id: &str,
        call: &StreamCall,
        chosen: Option<Duration>,
    ) -> Result<(mpsc::UnboundedReceiver<GatherEvent>, Duration, StreamId), MessagingError> {
        let mut events = None;
        let sent = self
            .hubs
            .send_where("", Some(STREAMING), |stream, handshake| {
                // Ranked on a snapshot: the stream current now may belong to a re-handshake with
                // an older hub, which would answer UNSUPPORTED (a P4a hub) or serve the request
                // as a unary query (older). Refused, nothing written: the next capable hub is tried.
                if !handshake.streaming() {
                    return Err(MessagingError::UnsupportedByHub(format!(
                        "the hub stream {stream} no longer lists the \"{STREAMING}\" capability"
                    )));
                }
                let (timeout_ms, guard) = request_timing(handshake, call.type_name, chosen);
                let (tx, rx) = mpsc::unbounded_channel();
                events = Some(rx);
                self.pending.lock().unwrap().insert(
                    id.to_string(),
                    Entry {
                        type_name: call.type_name.to_string(),
                        stream,
                        waiter: Waiter::Stream(tx),
                    },
                );
                let request = proto::Request {
                    id: id.to_string(),
                    kind: RequestKind::Query as i32,
                    r#type: call.type_name.to_string(),
                    payload: call.payload.clone(),
                    headers: call.headers.clone(),
                    timeout_ms,
                    priority: call.priority,
                    mode: RequestMode::Streaming as i32,
                    initial_credit: call.window,
                    max_results: call.max_results,
                    ..proto::Request::default()
                };
                let msg = ClientMessage {
                    message: Some(client_message::Message::Request(request)),
                };
                Ok((msg, guard))
            })
            .await;
        match (sent, events) {
            (Ok((stream, guard)), Some(events)) => Ok((events, guard, stream)),
            (Err(e), _) => {
                self.take(id);
                Err(e)
            }
            (Ok(_), None) => unreachable!("a written request was built"),
        }
    }

    /// Writes `Cancel{id, reason}` to hub stream `stream` from a drop (no await possible):
    /// spawned on the current runtime, best effort, bounded by [`CANCEL_WRITE_TIMEOUT`].
    fn cancel_later(&self, stream: StreamId, id: &str, reason: &str) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let hubs = Arc::clone(&self.hubs);
        let cancel = ClientMessage {
            message: Some(client_message::Message::Cancel(proto::Cancel {
                request_id: id.to_string(),
                reason: reason.to_string(),
            })),
        };
        let id = id.to_string();
        runtime.spawn(async move {
            match tokio::time::timeout(CANCEL_WRITE_TIMEOUT, hubs.send_on(stream, cancel)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::debug!(request = %id, "cancel not written: {e}"),
                Err(_) => tracing::debug!(request = %id, "cancel not written in time"),
            }
        });
    }

    fn scatter_call(
        &self,
        type_name: &'static str,
        payload: Vec<u8>,
        options: &ScatterOptions,
    ) -> ScatterCall {
        ScatterCall {
            type_name,
            payload,
            headers: self.headers(None),
            priority: self.priority(type_name, options.priority),
            max_results: options.max_results,
        }
    }

    /// Writes a new scatter-gather of `call` (fresh id) to a hub that shares the capability.
    async fn open_gather(
        &self,
        call: &Arc<ScatterCall>,
        chosen: Option<Duration>,
    ) -> Result<Gathering<'_>, MessagingError> {
        // The gathering exists before the write: its drop forgets the pending entry (recorded by
        // the build, before the write) when the caller drops the future while the write still
        // waits for room in the stream's outbound buffer, as the unary path's guard does.
        let (_, events) = mpsc::unbounded_channel();
        let mut gathering = Gathering {
            sender: self,
            call: Arc::clone(call),
            chosen,
            id: uuid::Uuid::new_v4().to_string(),
            events,
            deadline: tokio::time::Instant::now(),
            moves: 0,
            end: None,
            done: false,
        };
        let (events, guard) = self.write_gather(&gathering.id, call, chosen).await?;
        gathering.events = events;
        gathering.deadline = tokio::time::Instant::now() + guard;
        Ok(gathering)
    }

    /// Writes scatter-gather `id` to a hub whose ack lists `scatter-gather`, its pending entry
    /// recorded while the stream is current. Returns its events and the local guard.
    async fn write_gather(
        &self,
        id: &str,
        call: &ScatterCall,
        chosen: Option<Duration>,
    ) -> Result<(mpsc::UnboundedReceiver<GatherEvent>, Duration), MessagingError> {
        let mut events = None;
        let sent = self
            .hubs
            .send_where("", Some(SCATTER_GATHER), |stream, handshake| {
                // The hubs were ranked on a snapshot: the stream current now may belong to a
                // re-handshake with an older hub, which would serve the gather as a unary query
                // (its reply ignored here, the local guard the only end). Refused, nothing
                // written: the next capable hub is tried.
                if !handshake.scatter_gather() {
                    return Err(MessagingError::UnsupportedByHub(format!(
                        "the hub stream {stream} no longer lists the \"{SCATTER_GATHER}\" capability"
                    )));
                }
                let (timeout_ms, guard) = request_timing(handshake, call.type_name, chosen);
                let (tx, rx) = mpsc::unbounded_channel();
                events = Some(rx);
                self.pending.lock().unwrap().insert(
                    id.to_string(),
                    Entry {
                        type_name: call.type_name.to_string(),
                        stream,
                        waiter: Waiter::Scatter(tx),
                    },
                );
                let request = proto::Request {
                    id: id.to_string(),
                    kind: RequestKind::Query as i32,
                    r#type: call.type_name.to_string(),
                    payload: call.payload.clone(),
                    headers: call.headers.clone(),
                    timeout_ms,
                    priority: call.priority,
                    mode: RequestMode::ScatterGather as i32,
                    max_results: call.max_results,
                    ..proto::Request::default()
                };
                let msg = ClientMessage {
                    message: Some(client_message::Message::Request(request)),
                };
                Ok((msg, guard))
            })
            .await;
        match (sent, events) {
            (Ok((_, guard)), Some(events)) => Ok((events, guard)),
            (Err(e), _) => {
                self.take(id);
                Err(e)
            }
            (Ok(_), None) => unreachable!("a written request was built"),
        }
    }

    /// Correlation id, forwarded bearer token and trace parent, shared by every attempt.
    fn headers(&self, authorization: Option<String>) -> HashMap<String, String> {
        let current = MessageContext::current();
        let mut headers = HashMap::new();
        headers.insert(
            hub_headers::CORRELATION_ID.to_string(),
            current
                .as_ref()
                .map(|c| c.correlation_id.clone())
                .filter(|c| !c.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        );
        let authorization = authorization.or_else(|| {
            if self.forward_jwt {
                current.as_ref().and_then(|c| c.authorization.clone())
            } else {
                None
            }
        });
        if let Some(auth) = authorization {
            headers.insert(hub_headers::AUTHORIZATION.to_string(), auth);
        }
        if let Some(tp) = current
            .as_ref()
            .and_then(|c| c.headers.get(hub_headers::TRACEPARENT))
        {
            headers.insert(hub_headers::TRACEPARENT.to_string(), tp.clone());
        }
        headers
    }

    /// One `Request` with a fresh id: written, then awaited until the reply, the hub's error or
    /// the local guard. A hub whose stream does not take the write is skipped for the next one
    /// (same id: nothing was written); so is one whose stream is lost while the request still
    /// waits in its outbound buffer ([`Inbound::Disconnected`]'s `unsent`).
    async fn attempt(
        &self,
        call: &Attempt<'_>,
        payload: &[u8],
        chosen: Option<Duration>,
    ) -> Result<Vec<u8>, MessagingError> {
        let id = uuid::Uuid::new_v4().to_string();
        let type_name = call.type_name;
        let _forget = ForgetOnDrop {
            pending: &self.pending,
            id: &id,
        };
        // The entry is recorded while the stream is still current, tagged with it, so that stream's
        // Disconnected always finds it. A failed write leaves it to the next hub's build, which
        // replaces it (new stream, new channel: a late Disconnected of the failed stream finds
        // nothing of it); when no hub takes the write it is removed (never written).
        let mut moves = 0;
        loop {
            let mut answer = None;
            let sent = self
                .hubs
                .send_with(call.routing_key, |stream, handshake| {
                    let (timeout_ms, guard) = request_timing(handshake, type_name, chosen);
                    let (tx, rx) = oneshot::channel();
                    answer = Some(rx);
                    self.pending.lock().unwrap().insert(
                        id.clone(),
                        Entry {
                            type_name: type_name.to_string(),
                            stream,
                            waiter: Waiter::Unary(tx),
                        },
                    );
                    let request = proto::Request {
                        id: id.clone(),
                        kind: call.kind as i32,
                        r#type: type_name.to_string(),
                        routing_key: call.routing_key.to_string(),
                        payload: payload.to_vec(),
                        headers: call.headers.clone(),
                        timeout_ms,
                        priority: call.priority,
                        ..proto::Request::default()
                    };
                    let msg = ClientMessage {
                        message: Some(client_message::Message::Request(request)),
                    };
                    Ok((msg, guard))
                })
                .await;
            let (guard, rx) = match (sent, answer) {
                (Ok((_, guard)), Some(rx)) => (guard, rx),
                (Err(e), _) => {
                    self.take(&id);
                    return Err(e);
                }
                (Ok(_), None) => unreachable!("a written request was built"),
            };
            // The hub emits TIMEOUT itself; the guard covers a hub that never answers at all.
            match tokio::time::timeout(guard, rx).await {
                Ok(Ok(Outcome::Done(result))) => return result,
                Ok(Ok(Outcome::NotWritten)) => {
                    moves += 1;
                    if moves > MAX_UNWRITTEN_MOVES {
                        return Err(MessagingError::HubUnavailable(format!(
                            "'{type_name}' not written: {moves} hub streams lost before it was"
                        )));
                    }
                    tracing::debug!(r#type = type_name, request = %id, "stream lost before the request was written; trying the next hub");
                }
                Ok(Err(_)) => return Err(MessagingError::HubUnavailable("sender closed".into())),
                Err(_) => {
                    self.take(&id);
                    return Err(MessagingError::Timeout(type_name.to_string()));
                }
            }
        }
    }

    /// Fails every pending request with `HubConnectionLost` (used on shutdown, after the stream is
    /// closed: every pending request was written to a stream that is now gone).
    pub fn fail_all_lost(&self, reason: &str) {
        let lost: Vec<Entry> = self
            .pending
            .lock()
            .unwrap()
            .drain()
            .map(|(_, e)| e)
            .collect();
        for e in lost {
            let error = MessagingError::HubConnectionLost(format!(
                "{reason} before the reply to '{}'; the outcome is unknown",
                e.type_name
            ));
            e.waiter.finish(Outcome::Done(Err(error)));
        }
    }

    /// Requests sent and not yet answered, failed or abandoned.
    pub fn in_flight(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    fn take(&self, request_id: &str) -> Option<Entry> {
        self.pending.lock().unwrap().remove(request_id)
    }

    /// Takes the entry of `request_id` if its waiter is the right kind.
    fn take_if(&self, request_id: &str, kind: impl Fn(&Waiter) -> bool) -> Option<Entry> {
        let mut pending = self.pending.lock().unwrap();
        if pending.get(request_id).is_some_and(|e| kind(&e.waiter)) {
            pending.remove(request_id)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::RegisterAck;

    fn ack(overrides: &[(&str, u32)]) -> Handshake {
        Handshake::V2(RegisterAck {
            hub_id: "h".into(),
            protocol_version: 2,
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 10_000,
            default_timeout_ms: 5_000,
            max_timeout_ms: 60_000,
            context: "default".into(),
            type_timeouts_ms: overrides
                .iter()
                .map(|(t, ms)| (t.to_string(), *ms))
                .collect(),
            ..Default::default()
        })
    }

    const S: fn(u64) -> Duration = Duration::from_secs;
    const MS: fn(u64) -> Duration = Duration::from_millis;

    #[test]
    fn v2_sends_zero_unless_chosen_and_guards_with_the_hub_bound() {
        let h = ack(&[("t.Slow", 120_000), ("t.Quick", 1_500)]);
        // Nothing chosen: 0 on the wire, guard = override or max, plus the margin.
        assert_eq!(request_timing(&h, "t.Other", None), (0, S(61)));
        assert_eq!(request_timing(&h, "t.Slow", None), (0, S(121)));
        assert_eq!(request_timing(&h, "t.Quick", None), (0, MS(2_500)));
        // Chosen: sent as is, guard = min(chosen, override or max) plus the margin.
        assert_eq!(request_timing(&h, "t.Other", Some(S(2))), (2_000, S(3)));
        assert_eq!(request_timing(&h, "t.Other", Some(S(90))), (90_000, S(61)));
        assert_eq!(request_timing(&h, "t.Slow", Some(S(90))), (90_000, S(91)));
        assert_eq!(
            request_timing(&h, "t.Quick", Some(S(4))),
            (4_000, MS(2_500))
        );
    }

    #[test]
    fn zero_chosen_timeout_means_unset() {
        assert_eq!(chosen_timeout(Some(MS(0)), Some(S(3))), Some(S(3)));
        assert_eq!(chosen_timeout(Some(MS(0)), Some(MS(0))), None);
        assert_eq!(chosen_timeout(None, Some(MS(0))), None);
        assert_eq!(chosen_timeout(Some(S(2)), Some(S(3))), Some(S(2)));
        // Defensive: a zero reaching the timing is treated as unset too.
        assert_eq!(
            request_timing(&Handshake::Legacy, "t.X", Some(MS(0))),
            (5_000, S(6))
        );
        assert_eq!(request_timing(&ack(&[]), "t.X", Some(MS(0))), (0, S(61)));
    }

    #[test]
    fn a_sub_millisecond_timeout_is_sent_as_one_millisecond() {
        let us = Duration::from_micros;
        assert_eq!(
            request_timing(&ack(&[]), "t.X", Some(us(400))),
            (1, MS(1) + LOCAL_GUARD_MARGIN)
        );
        assert_eq!(
            request_timing(&Handshake::Legacy, "t.X", Some(us(400))),
            (1, MS(1) + LOCAL_GUARD_MARGIN)
        );
    }

    #[test]
    fn zero_override_in_the_ack_is_ignored() {
        let h = ack(&[("t.Zero", 0)]);
        assert_eq!(request_timing(&h, "t.Zero", None), (0, S(61)));
        assert_eq!(request_timing(&h, "t.Zero", Some(S(2))), (2_000, S(3)));
    }

    #[test]
    fn retry_budget_is_the_chosen_timeout_else_the_hubs_bound() {
        let h = ack(&[("t.Slow", 120_000), ("t.Zero", 0)]);
        assert_eq!(retry_budget(Some(&h), "t.X", Some(S(2))), S(2));
        assert_eq!(retry_budget(Some(&h), "t.Slow", None), S(120));
        assert_eq!(
            retry_budget(Some(&h), "t.Zero", None),
            S(5),
            "a zero override is ignored"
        );
        assert_eq!(
            retry_budget(Some(&h), "t.X", None),
            S(5),
            "the ack's default"
        );
        assert_eq!(retry_budget(Some(&Handshake::Legacy), "t.X", None), S(5));
        assert_eq!(retry_budget(None, "t.X", None), S(5));
        assert_eq!(
            retry_budget(Some(&h), "t.X", Some(MS(0))),
            S(5),
            "zero is unset"
        );
    }

    fn part(service: &str, payload: &[u8], error: Option<ErrorCode>) -> proto::ReplyPart {
        proto::ReplyPart {
            request_id: "g".into(),
            payload: payload.to_vec(),
            error: error.map(|code| proto::Error {
                request_id: "g".into(),
                code: code as i32,
                message: "boom".into(),
                details: vec![],
            }),
            responder_service: service.into(),
            responder_instance: format!("{service}-1"),
            seq: 0,
        }
    }

    #[test]
    fn a_gather_assembles_its_parts_in_arrival_order_and_counts_the_failures() {
        let parts = vec![
            part("b", br#"{"n":2}"#, None),
            part("a", b"", Some(ErrorCode::HandlerDisconnected)),
            part("c", b"not json", None),
            part("d", br#"{"n":4}"#, None),
        ];
        let end = proto::ReplyEnd {
            request_id: "g".into(),
            parts: 4,
            failed: 1,
            timed_out: 2,
            ..Default::default()
        };
        let r: ScatterResult<serde_json::Value> = assemble(parts, Some(&end));
        let who: Vec<_> = r.parts.iter().map(|p| p.service.as_str()).collect();
        assert_eq!(who, ["b", "a", "c", "d"]);
        assert_eq!(r.parts[0].instance, "b-1");
        assert_eq!(r.parts[0].outcome.as_ref().unwrap()["n"], 2);
        let e = r.parts[1].outcome.as_ref().unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (ErrorCode::HandlerDisconnected, "boom")
        );
        assert!(matches!(
            e.clone().into_error("t.X"),
            MessagingError::HandlerDisconnected(t) if t == "t.X"
        ));
        let undecodable = r.parts[2].outcome.as_ref().unwrap_err();
        assert_eq!(undecodable.code, ErrorCode::Unspecified);
        assert!(undecodable.message.contains("c/c-1"), "{undecodable}");
        // The handler's failure plus the reply this client could not decode.
        assert_eq!((r.failed, r.timed_out), (2, 2));
        // A unit reply accepts the empty object, like a unary reply.
        let unit: ScatterResult<()> = assemble(vec![part("a", b"{}", None)], None);
        assert!(unit.parts[0].outcome.is_ok());
    }

    fn idle_sender() -> Arc<Sender> {
        let (tx, _rx) = mpsc::channel(1);
        let conn = HubConnection::new(
            super::super::HubSettings::default(),
            "svc".into(),
            "i".into(),
            tx,
        );
        Sender::with_settings(conn, SenderSettings::default())
    }

    fn pending(sender: &Sender, id: &str, waiter: Waiter) {
        sender.pending.lock().unwrap().insert(
            id.into(),
            Entry {
                type_name: "t.X".into(),
                stream: 7,
                waiter,
            },
        );
    }

    #[tokio::test]
    async fn parts_and_ends_reach_their_gather_only_and_the_end_completes_it() {
        let sender = idle_sender();
        let (tx, mut gather) = mpsc::unbounded_channel();
        pending(&sender, "g", Waiter::Scatter(tx));
        let (unary_tx, mut unary) = oneshot::channel();
        pending(&sender, "u", Waiter::Unary(unary_tx));

        for id in ["nobody", "u"] {
            sender.on_inbound(Inbound::Part(proto::ReplyPart {
                request_id: id.into(),
                ..Default::default()
            }));
            sender.on_inbound(Inbound::End(proto::ReplyEnd {
                request_id: id.into(),
                ..Default::default()
            }));
        }
        assert!(
            unary.try_recv().is_err(),
            "a unary request ignores parts and ends"
        );
        assert!(gather.try_recv().is_err());
        // A reply never answers a gather.
        sender.on_inbound(Inbound::Reply(proto::Reply {
            request_id: "g".into(),
            result: None,
        }));
        assert!(gather.try_recv().is_err());
        assert_eq!(sender.in_flight(), 2);

        sender.on_inbound(Inbound::Part(proto::ReplyPart {
            request_id: "g".into(),
            seq: 0,
            ..Default::default()
        }));
        sender.on_inbound(Inbound::End(proto::ReplyEnd {
            request_id: "g".into(),
            parts: 1,
            ..Default::default()
        }));
        assert!(matches!(gather.try_recv(), Ok(GatherEvent::Part(p)) if p.seq == 0));
        assert!(matches!(gather.try_recv(), Ok(GatherEvent::End(e)) if e.parts == 1));
        assert_eq!(sender.in_flight(), 1, "the end takes the gather's entry");
        // Parts after the end are ignored.
        sender.on_inbound(Inbound::Part(proto::ReplyPart {
            request_id: "g".into(),
            ..Default::default()
        }));
        assert!(gather.try_recv().is_err());
    }

    #[tokio::test]
    async fn parts_and_ends_reach_their_stream_and_a_reply_never_answers_one() {
        let sender = idle_sender();
        let (tx, mut stream) = mpsc::unbounded_channel();
        pending(&sender, "s", Waiter::Stream(tx));
        sender.on_inbound(Inbound::Reply(proto::Reply {
            request_id: "s".into(),
            result: None,
        }));
        assert!(stream.try_recv().is_err());
        assert_eq!(sender.in_flight(), 1);
        sender.on_inbound(Inbound::Part(proto::ReplyPart {
            request_id: "s".into(),
            seq: 1,
            ..Default::default()
        }));
        sender.on_inbound(Inbound::End(proto::ReplyEnd {
            request_id: "s".into(),
            parts: 1,
            ..Default::default()
        }));
        assert!(matches!(stream.try_recv(), Ok(GatherEvent::Part(p)) if p.seq == 1));
        assert!(matches!(stream.try_recv(), Ok(GatherEvent::End(e)) if e.parts == 1));
        assert_eq!(sender.in_flight(), 0, "the end takes the stream's entry");
        // Flow and Cancel are the dispatcher's: ignored here.
        sender.on_inbound(Inbound::Flow(proto::Flow::default()));
        sender.on_inbound(Inbound::Cancel(proto::Cancel::default()));
    }

    #[test]
    fn stream_options_default_to_a_window_of_64_and_refill_half_of_it() {
        let options = StreamOptions::default();
        assert_eq!(options.initial_credit, DEFAULT_CREDIT);
        assert_eq!(
            (options.timeout, options.max_results, options.priority),
            (None, 0, None)
        );
        let call = |window| StreamCall {
            type_name: "t.X",
            payload: vec![],
            headers: HashMap::new(),
            priority: 0,
            max_results: 0,
            window,
        };
        assert_eq!(call(64).refill(), 32);
        assert_eq!(call(3).refill(), 1);
        assert_eq!(call(1).refill(), 1, "never 0: the stream would stall");
    }

    #[tokio::test]
    async fn a_lost_stream_or_an_error_ends_a_gather() {
        let sender = idle_sender();
        let (tx, mut lost) = mpsc::unbounded_channel();
        pending(&sender, "g1", Waiter::Scatter(tx));
        let (tx, mut unwritten) = mpsc::unbounded_channel();
        pending(&sender, "g2", Waiter::Scatter(tx));
        let (tx, mut rejected) = mpsc::unbounded_channel();
        pending(&sender, "g3", Waiter::Scatter(tx));
        sender.on_inbound(Inbound::Error(proto::Error {
            request_id: "g3".into(),
            code: ErrorCode::NoHandler as i32,
            message: "no handler".into(),
            details: vec![],
        }));
        sender.on_inbound(Inbound::Disconnected {
            stream: 7,
            unsent: vec!["g2".into()],
        });
        assert!(matches!(
            lost.try_recv(),
            Ok(GatherEvent::Ended(Outcome::Done(Err(
                MessagingError::HubConnectionLost(_)
            ))))
        ));
        assert!(matches!(
            unwritten.try_recv(),
            Ok(GatherEvent::Ended(Outcome::NotWritten))
        ));
        assert!(matches!(
            rejected.try_recv(),
            Ok(GatherEvent::Ended(Outcome::Done(Err(
                MessagingError::NoHandler(_)
            ))))
        ));
        assert_eq!(sender.in_flight(), 0);
    }

    #[test]
    fn legacy_keeps_todays_timeout_and_guard() {
        let h = Handshake::Legacy;
        assert_eq!(request_timing(&h, "t.X", None), (5_000, S(6)));
        assert_eq!(request_timing(&h, "t.X", Some(MS(200))), (200, MS(1_200)));
    }
}
