//! Outgoing commands and queries: one `Request` per call, correlated reply, timeout, error mapping.
//!
//! Failure split: a request that was never accepted by a handshaken stream fails with
//! `HubUnavailable` (the hub never saw it, a retry is always safe); one that was accepted by a
//! stream's outbound queue and whose stream then broke fails with `HubConnectionLost` (outcome
//! unknown, the handler may have run).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::codec;
use crate::context::{MessageContext, hub_headers};
use crate::error::MessagingError;
use crate::proto::{self, ClientMessage, RequestKind, client_message, reply};
use crate::retry::{self, RetryPolicy};
use crate::types::{Command, Query};

use super::connection::{Handshake, HubConnection, Inbound, StreamId};

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

type Pending = oneshot::Sender<Result<Vec<u8>, MessagingError>>;

/// Removes a request's pending entry when the call ends, however it ends: a reply, an error, the
/// local guard, or the caller dropping the future (while the write is blocked or while it awaits
/// the answer). Removing an entry that was already taken is a no-op.
struct ForgetOnDrop<'a> {
    pending: &'a Mutex<HashMap<String, Entry>>,
    id: &'a str,
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
    tx: Pending,
}

/// The `CommandBus`/`QueryBus` of the Rust side. Replies arrive through [`Sender::on_inbound`].
pub struct Sender {
    conn: Arc<HubConnection>,
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

    pub fn with_settings(conn: Arc<HubConnection>, settings: SenderSettings) -> Arc<Sender> {
        Arc::new(Sender {
            conn,
            pending: Mutex::new(HashMap::new()),
            default_timeout: settings.default_timeout,
            forward_jwt: settings.forward_jwt,
            priorities: settings.priorities,
            retry: settings.retry,
        })
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
                if let Some(Entry { type_name, tx, .. }) = self.take(&r.request_id) {
                    let result = match r.result {
                        Some(reply::Result::Payload(p)) => Ok(p),
                        Some(reply::Result::Error(e)) => Err(MessagingError::from_hub(
                            &type_name, e.code, &e.message, &e.details,
                        )),
                        None => Ok(b"{}".to_vec()),
                    };
                    let _ = tx.send(result);
                }
            }
            Inbound::Error(e) => {
                if let Some(Entry { type_name, tx, .. }) = self.take(&e.request_id) {
                    let _ = tx.send(Err(MessagingError::from_hub(
                        &type_name, e.code, &e.message, &e.details,
                    )));
                }
            }
            Inbound::Disconnected { stream } => {
                let lost: Vec<Entry> = {
                    let mut pending = self.pending.lock().unwrap();
                    let ids: Vec<String> = pending
                        .iter()
                        .filter(|(_, e)| e.stream == stream)
                        .map(|(id, _)| id.clone())
                        .collect();
                    ids.iter().filter_map(|id| pending.remove(id)).collect()
                };
                for e in lost {
                    let _ = e.tx.send(Err(MessagingError::HubConnectionLost(format!(
                        "hub stream closed before the reply to '{}'; the outcome is unknown",
                        e.type_name
                    ))));
                }
            }
            Inbound::Request { .. } => {} // the dispatcher's business
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
        if policy.as_ref().is_some_and(|p| p.max_attempts == 0) {
            return Err(MessagingError::InvalidMessage(format!(
                "retry policy for '{type_name}' has max_attempts 0; it must be at least 1 (1 = no retry)"
            )));
        }
        let headers = self.headers(opts.authorization.clone());
        let priority = opts
            .priority
            .or_else(|| self.priorities.get(type_name).copied())
            .unwrap_or(0);
        let chosen = chosen_timeout(opts.timeout, self.default_timeout);
        let routing_key = opts.routing_key.unwrap_or_default();
        let call = Attempt {
            kind,
            type_name,
            routing_key: &routing_key,
            headers: &headers,
            priority,
        };
        let policy = match policy {
            Some(p) if p.max_attempts > 1 => p,
            _ => return self.attempt(&call, payload, chosen).await,
        };
        let attempts = policy.max_attempts;

        let budget = retry_budget(self.conn.handshake().as_ref(), type_name, chosen);
        let started = tokio::time::Instant::now();
        let min_remaining = policy.initial_backoff.max(RETRY_MIN_REMAINING);
        // The first attempt is never gated and gets the whole budget.
        let mut remaining = budget.saturating_sub(started.elapsed());
        let mut attempt = 1;
        loop {
            let timeout = remaining.max(MIN_TIMEOUT);
            let result = self.attempt(&call, payload.clone(), Some(timeout)).await;
            let error = match result {
                Ok(bytes) => return Ok(bytes),
                Err(e) => e,
            };
            if attempt >= attempts || !retry::is_retryable(kind, &error) {
                return Err(error);
            }
            let wait = policy.jittered_backoff(attempt);
            // What the next attempt would have, from the moment its wait is over.
            remaining = match budget.checked_sub(started.elapsed() + wait) {
                Some(left) if left >= min_remaining => left,
                _ => {
                    tracing::debug!(
                        r#type = type_name,
                        attempt,
                        "no room left in the budget for a retry: {error}"
                    );
                    return Err(error);
                }
            };
            tracing::debug!(r#type = type_name, attempt, retry_in = ?wait, "retrying: {error}");
            tokio::time::sleep(wait).await;
            attempt += 1;
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
    /// the local guard.
    async fn attempt(
        &self,
        call: &Attempt<'_>,
        payload: Vec<u8>,
        chosen: Option<Duration>,
    ) -> Result<Vec<u8>, MessagingError> {
        let id = uuid::Uuid::new_v4().to_string();
        let type_name = call.type_name;
        let (tx, rx) = oneshot::channel();
        let _forget = ForgetOnDrop {
            pending: &self.pending,
            id: &id,
        };
        // The entry is recorded while the stream is still current, tagged with it, so that stream's
        // Disconnected always finds it; a failed write removes it again (never written).
        let sent = self
            .conn
            .send_with(|stream, handshake| {
                let (timeout_ms, guard) = request_timing(handshake, type_name, chosen);
                self.pending.lock().unwrap().insert(
                    id.clone(),
                    Entry {
                        type_name: type_name.to_string(),
                        stream,
                        tx,
                    },
                );
                let request = proto::Request {
                    id: id.clone(),
                    kind: call.kind as i32,
                    r#type: type_name.to_string(),
                    routing_key: call.routing_key.to_string(),
                    payload,
                    headers: call.headers.clone(),
                    timeout_ms,
                    priority: call.priority,
                };
                let msg = ClientMessage {
                    message: Some(client_message::Message::Request(request)),
                };
                (msg, guard)
            })
            .await;
        let guard = match sent {
            Ok((_, guard)) => guard,
            Err(e) => {
                self.take(&id);
                return Err(e);
            }
        };
        // The hub emits TIMEOUT itself; the guard covers a hub that never answers at all.
        match tokio::time::timeout(guard, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(MessagingError::HubUnavailable("sender closed".into())),
            Err(_) => {
                self.take(&id);
                Err(MessagingError::Timeout(type_name.to_string()))
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
            let _ = e.tx.send(Err(MessagingError::HubConnectionLost(format!(
                "{reason} before the reply to '{}'; the outcome is unknown",
                e.type_name
            ))));
        }
    }

    /// Requests sent and not yet answered, failed or abandoned.
    pub fn in_flight(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    fn take(&self, request_id: &str) -> Option<Entry> {
        self.pending.lock().unwrap().remove(request_id)
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

    #[test]
    fn legacy_keeps_todays_timeout_and_guard() {
        let h = Handshake::Legacy;
        assert_eq!(request_timing(&h, "t.X", None), (5_000, S(6)));
        assert_eq!(request_timing(&h, "t.X", Some(MS(200))), (200, MS(1_200)));
    }
}
