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
use crate::types::{Command, Query};

use super::connection::{Handshake, HubConnection, Inbound, StreamId};

/// The timeout sent to a hub older than protocol v2 when the caller chose none (today's default).
pub const LEGACY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Added to the hub's own deadline for the local guard, which only catches a hub that never answers.
pub const LOCAL_GUARD_MARGIN: Duration = Duration::from_secs(1);
/// Used for the guard when a v2 ack carries no maximum (never sent by the hub; defensive).
const FALLBACK_MAX_TIMEOUT_MS: u32 = 60_000;

/// `Request.timeout_ms` and the local guard for a request of `type_name` on a stream with
/// `handshake`, when the caller chose `chosen` (per call, or the configured default).
///
/// - Protocol v2: send the chosen timeout or 0 (the hub then applies its per-type override or its
///   default); guard with the hub's upper bound for the request, i.e. `min(chosen, override or
///   max)` or, when 0 was sent, `override or max`, plus [`LOCAL_GUARD_MARGIN`].
/// - Legacy hub: send the chosen timeout or [`LEGACY_REQUEST_TIMEOUT`]; guard with that plus the margin.
pub fn request_timing(
    handshake: &Handshake,
    type_name: &str,
    chosen: Option<Duration>,
) -> (u32, Duration) {
    let ms = |d: Duration| d.as_millis().min(u32::MAX as u128) as u32;
    let chosen = chosen.filter(|d| ms(*d) > 0);
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
}

impl Sender {
    pub fn new(
        conn: Arc<HubConnection>,
        default_timeout: Option<Duration>,
        forward_jwt: bool,
    ) -> Arc<Sender> {
        Arc::new(Sender {
            conn,
            pending: Mutex::new(HashMap::new()),
            default_timeout,
            forward_jwt,
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

    async fn request(
        &self,
        kind: RequestKind,
        type_name: &str,
        payload: Vec<u8>,
        opts: SendOptions,
    ) -> Result<Vec<u8>, MessagingError> {
        let id = uuid::Uuid::new_v4().to_string();
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
        let authorization = opts.authorization.or_else(|| {
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
        let chosen = chosen_timeout(opts.timeout, self.default_timeout);
        let routing_key = opts.routing_key.unwrap_or_default();

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
                    kind: kind as i32,
                    r#type: type_name.to_string(),
                    routing_key,
                    payload,
                    headers,
                    timeout_ms,
                    priority: 0,
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
    fn zero_override_in_the_ack_is_ignored() {
        let h = ack(&[("t.Zero", 0)]);
        assert_eq!(request_timing(&h, "t.Zero", None), (0, S(61)));
        assert_eq!(request_timing(&h, "t.Zero", Some(S(2))), (2_000, S(3)));
    }

    #[test]
    fn legacy_keeps_todays_timeout_and_guard() {
        let h = Handshake::Legacy;
        assert_eq!(request_timing(&h, "t.X", None), (5_000, S(6)));
        assert_eq!(request_timing(&h, "t.X", Some(MS(200))), (200, MS(1_200)));
    }
}
