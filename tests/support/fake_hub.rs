//! In-process stand-in for kraken-hub: records registrations, answers a v2 `Register` with a
//! `RegisterAck` (new-hub behaviour, the default) or not at all (old-hub behaviour, `set_ack(None)`).
//! The ack lists "flow-control" when the client declared it, unless `set_flow_control(false)`
//! (the P0 hub), and "instance-status" likewise unless `set_instance_status(false)` (hubs before
//! P3). Permits and InstanceStatus messages are recorded per stream. It
//! echoes heartbeats, can drop streams, reject a registration, push requests to clients and answer
//! their requests.
//!
//! Like kraken-hub, a `Register` for an instance id that another open stream holds replaces that
//! stream's session: the older stream is ended (its forwarded requests can no longer be answered)
//! unless `set_replace_sessions(false)`. A stream that ends while it holds the session removes the
//! instance. Registrations and stream ends are logged in order ([`FakeHub::events`]).
//!
//! Permit accounting like kraken-hub (it never holds requests back, it only counts): a stream whose
//! ack shares "flow-control" and whose first `Register` offers `initial_permits > 0` holds that
//! many permits ([`FakeHub::permits_held`]); each request pushed to it spends one, each `Permits`
//! adds `count`, never past the stream's cap. The cap starts at `initial_permits` and a
//! `Permits.grant` above it raises it first (never lowers it). `set_permit_cap(false)` drops the
//! cap; `set_permit_grants(false)` emulates a hub that predates `Permits.grant`.
//!
//! Scatter-gather like kraken-hub (ADR 0017): the ack lists "scatter-gather" when the client
//! declared it, unless `set_scatter_gather(false)` (a P3 hub). A `Request` with
//! `mode = SCATTER_GATHER` on a stream whose ack never listed it is served like a unary request
//! (a P3 hub does not know `mode`: the responder answers, or nobody); on a stream whose ack listed
//! it while the flag is off now it is rejected with `UNSUPPORTED`. Otherwise it
//! goes to every stream whose `Register` lists the type in `query_types` or
//! `shared_query_types` (ordered by service, then instance id) as a unary request `<id>#<n>`; each
//! answer goes back to the sender as a `ReplyPart` in arrival order, then one `ReplyEnd` once every
//! target answered, after `max_results` successful parts, or at the deadline (`timeout_ms`, else
//! 5 s) with the unanswered targets as `timed_out`. A target whose stream ends is a failed part
//! (`HANDLER_DISCONNECTED`). No target: `NO_HANDLER`; a command: `INVALID_REQUEST`; a session
//! without the capability: `UNSUPPORTED`. Unary requests still go to the responder only. A gather
//! that ends early (`max_results`, the deadline, the sender gone) sends `Cancel{<id>#<n>, reason}`
//! to each still-outstanding target whose session negotiated "streaming".
//!
//! Streaming like kraken-hub (ADR 0017, P4b): the ack lists "streaming" when the client declared
//! it, unless `set_streaming(false)` (a P4a hub). A `Request` with `mode = STREAMING` on a session
//! without it is `UNSUPPORTED` (unary on a session that never saw `scatter-gather` either: a P3
//! hub does not know `mode`). Otherwise: a command `INVALID_REQUEST`, no owner `NO_HANDLER`, no
//! owner instance that negotiated "streaming" `UNSUPPORTED`, a duplicate id `INVALID_REQUEST`;
//! else one capable instance (round-robin over them, by service then instance id) gets
//! `Request{<id>#1, mode = STREAMING, initial_credit (0 reads as 64), max_results 0}`. Its
//! `ReplyPart`s are relayed under the credit (`request_id` rewritten, responder set, `seq` kept;
//! one beyond the credit ends the stream with `INVALID_REQUEST "part without credit"` and cancels
//! the handler; one carrying `error` ends it with that error), its `ReplyEnd` is relayed; after
//! `max_results` parts the stream ends and the handler gets `Cancel{"max results"}`; a sender
//! `Flow` adds credit and is relayed as `Flow{<id>#1}`; a sender `Cancel` cancels the handler and
//! ends the stream silently; no part within `timeout_ms` (else 5 s, from the forward, reset per
//! part; the clock does not run while the credit is 0 and starts over at the `Flow` that raises
//! it) ends it with `TIMEOUT` and `Cancel{"idle timeout"}`; the handler leaving ends it with
//! `HANDLER_DISCONNECTED`, the sender leaving cancels the handler (`"sender disconnected"`).
//! Client-sent parts, ends, flows and cancels are recorded per stream.
//!
//! Subscriptions like kraken-hub (ADR 0017, P4c): the ack lists "subscription" when the client
//! declared it, unless `set_subscription(false)` (a P4b hub). A `Request` with
//! `mode = SUBSCRIPTION` on a session without it is `UNSUPPORTED` (unary on a session that knows
//! no modes); then a command `INVALID_REQUEST`, no owner `NO_HANDLER`, no owner instance that
//! negotiated "subscription" `UNSUPPORTED`, a duplicate id `INVALID_REQUEST`, a target already
//! serving `set_max_subscriptions_per_instance` (1024) subscriptions `RESOURCE_EXHAUSTED`. Else
//! every capable instance listing the type (by service, then instance id) gets
//! `Request{<id>#n, mode = SUBSCRIPTION}` (`n` from 1; `initial_credit` and `max_results` 0) and
//! one of them (round-robin) `Request{<id>#0, mode = UNARY}` for the initial result, whose
//! `Reply{payload}` is `ReplyPart{seq 1}` to the sender (`Reply{error}`, no reply within
//! `timeout_ms` (else 5 s) or its handler leaving: `ReplyEnd{error}` and `Cancel{"initial result
//! failed"}` to the targets). A target's `ReplyPart` is an update: relayed in arrival order with
//! the hub's own `seq` (2, 3, ...; the handler's is dropped) under the sender's credit
//! (`initial_credit`, 0 reads as 64, plus every `Flow`; the initial part takes one), buffered
//! behind the initial part and beyond the credit up to `set_subscription_buffer` (256): one more
//! ends it with `RESOURCE_EXHAUSTED "subscriber buffer (N) exceeded"` and `Cancel{"subscriber
//! buffer exceeded"}`; a part carrying `error` or an error `ReplyEnd` from any target ends it with
//! that error and `Cancel{"target <instance> failed"}` to the others; a clean `ReplyEnd` removes
//! the target, the last one's ends the subscription with `ReplyEnd{parts}` once the buffer is
//! out; after `max_results` updates `ReplyEnd{parts}` and `Cancel{"max results"}`. A sender
//! `Cancel` (or the sender leaving: `"sender disconnected"`) is fanned out to the targets (and to
//! `<id>#0` while forwarded), nothing more to the sender; a target leaving is silent unless it
//! was the last (`HANDLER_DISCONNECTED`). `set_subscription_max_duration(Some(d))` ends one open
//! longer with `CANCELLED "max subscription duration (<d>) exceeded"`. No idle timeout.

//! Access control like kraken-hub (ADR 0018, P5a): `set_auth(mode, &[(token, Acl)])`. In `Enforce`
//! the first `Register` of a stream must carry a token the table knows (else the stream is closed
//! with `UNAUTHENTICATED`), the service name must be the entry's (else `PERMISSION_DENIED`) and
//! every command, query and shared query type must match its `handle` patterns (else
//! `PERMISSION_DENIED`, naming the type); a later `Register` is judged for its types against the
//! session's own token. A `Request` whose type is outside the entry's `send` patterns is answered
//! with an immediate `Error{PERMISSION_DENIED}` in every request mode and never routed. In
//! `Permissive` every violation is recorded ([`FakeHub::violations`]) and allowed; in `Off`
//! (default) nothing is checked. A refused `Register` is not in [`FakeHub::registers`]; it is in
//! [`FakeHub::rejections`] with the token it carried and when it arrived. A session without a
//! valid token (permissive) is not judged per request. Patterns as the hub parses them: exact,
//! `prefix.*` (a type longer than `prefix.`), `*`; `Acl::new` panics on any other wildcard.
//! JWT credentials (P5b, ADR 0019): `set_services(&[Acl])` is the `[[service]]` table. A `Register`
//! (or refresh) carrying `jwt` is decoded without verifying the signature: its `azp` must be the
//! `Register.service_name` (else `PERMISSION_DENIED` "token azp ... is not service ...") and name
//! an entry of that table (else `PERMISSION_DENIED` "no ACL entry for service ..."); until
//! `set_jwks_loaded(true)` (the default) the hub answers a JWT `Register` with `UNAVAILABLE` "jwks
//! not loaded yet". `set_credentials_refresh(true)` offers the capability `credentials-refresh` to
//! a client that declares it; a `ClientMessage.credentials` is recorded ([`FakeHub::refreshes`])
//! and, in `Enforce`, must resolve to the session's own service (else the stream ends with the
//! status, like a refused `Register`); the session then holds the new credential's entry.
//! Whatever the mode but `Off`, a `Register` whose instance id an open session of another service
//! holds is closed with `PERMISSION_DENIED "instance id <first 8 chars> belongs to another
//! service"` and nothing changes (the holder keeps its session); `Off` replaces it as before.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use kraken_messaging::proto::hub_server::{Hub, HubServer};
use kraken_messaging::proto::{self, ClientMessage, ServerMessage, client_message, server_message};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

type Responder = Box<dyn Fn(proto::Request) -> Option<ServerMessage> + Send + Sync>;
type Outbound = mpsc::Sender<Result<ServerMessage, Status>>;
/// What the hub says to a client that half-closed its stream, and how long it then keeps its own
/// side open.
type HalfClose = (
    std::time::Duration,
    Arc<dyn Fn() -> Vec<ServerMessage> + Send + Sync>,
);
/// A subscription target: (stream number, service, instance, outbound).
type Target = (usize, String, String, Outbound);

/// The ack a default kraken-hub sends.
pub fn default_ack() -> proto::RegisterAck {
    proto::RegisterAck {
        hub_id: "fake-hub".into(),
        protocol_version: 2,
        capabilities: vec![],
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 10_000,
        default_timeout_ms: 5_000,
        max_timeout_ms: 60_000,
        context: "default".into(),
        type_timeouts_ms: Default::default(),
    }
}

/// A stream's registration, for scatter-gather targeting.
struct Route {
    service: String,
    instance: String,
    commands: Vec<String>,
    /// Exclusive and shared query types.
    queries: Vec<String>,
    /// The session declared "scatter-gather" and the hub shares it.
    scatter: bool,
    /// The session declared "streaming" and the hub shares it.
    streaming: bool,
    /// The session declared "subscription" and the hub shares it.
    subscription: bool,
    tx: Outbound,
}

/// A subscription in progress (P4c).
struct SubState {
    sender: Outbound,
    sender_no: usize,
    /// The update targets still serving: target id → (stream number, service, instance, outbound).
    targets: HashMap<String, Target>,
    /// The initial request `<id>#0` while forwarded: (stream number, service, instance, outbound).
    initial: Option<Target>,
    initial_done: bool,
    credit: u32,
    /// Parts sent to the sender: the hub's `seq`.
    parts: u32,
    /// Updates relayed (for `max_results`).
    updates: u32,
    max_results: u32,
    /// Updates waiting for credit or for the initial part: (payload, service, instance).
    buffered: VecDeque<(Vec<u8>, String, String)>,
}

/// A streaming query in progress (P4b).
struct StreamState {
    sender: Outbound,
    sender_no: usize,
    target_no: usize,
    service: String,
    instance: String,
    target: Outbound,
    credit: u32,
    parts: u32,
    max_results: u32,
    idle: Duration,
    /// When the idle timeout fires; `None` while the credit is 0 (the clock is paused).
    deadline: Option<Instant>,
}

/// Why a gather closes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Closed {
    /// Every target answered.
    Complete,
    MaxResults,
    Deadline,
    /// The sender's stream ended: no end to send, the targets are cancelled.
    SenderGone,
}

/// A scatter-gather in progress.
struct Gather {
    sender: Outbound,
    /// target id → (stream number, service, instance).
    outstanding: HashMap<String, (usize, String, String)>,
    parts: u32,
    ok: u32,
    failed: u32,
    max_results: u32,
}

fn server(m: server_message::Message) -> ServerMessage {
    ServerMessage { message: Some(m) }
}

fn hub_error(id: &str, code: proto::ErrorCode, message: &str) -> ServerMessage {
    server(server_message::Message::Error(error(id, code, message)))
}

fn error(id: &str, code: proto::ErrorCode, message: &str) -> proto::Error {
    proto::Error {
        request_id: id.to_string(),
        code: code as i32,
        message: message.to_string(),
        details: vec![],
    }
}

fn cancel_msg(id: &str, reason: &str) -> ServerMessage {
    server(server_message::Message::Cancel(proto::Cancel {
        request_id: id.to_string(),
        reason: reason.to_string(),
    }))
}

/// Writes each message to its stream, in order.
async fn deliver(msgs: Vec<(Outbound, ServerMessage)>) {
    for (tx, m) in msgs {
        let _ = tx.send(Ok(m)).await;
    }
}

/// `KRAKEN_HUB_AUTH_MODE` of the fake hub.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Off,
    Permissive,
    Enforce,
}

/// What a token may do: the one service name, the types it may handle and the types it may send.
#[derive(Clone, Debug)]
pub struct Acl {
    pub service: String,
    pub handle: Vec<String>,
    pub send: Vec<String>,
}

impl Acl {
    /// Panics on a pattern the real hub's ACL parser refuses to load.
    pub fn new(service: &str, handle: &[&str], send: &[&str]) -> Acl {
        for pattern in handle.iter().chain(send) {
            if let Err(e) = parse_pattern(pattern) {
                panic!("{e}");
            }
        }
        Acl {
            service: service.to_string(),
            handle: handle.iter().map(|p| p.to_string()).collect(),
            send: send.iter().map(|p| p.to_string()).collect(),
        }
    }
}

/// A type pattern as the hub's ACL parser reads it: `*`, `prefix.*` or an exact name. `Err` for
/// what the hub refuses to load (a `*` anywhere else, or a `prefix` shorter than one character
/// before the dot).
fn parse_pattern(pattern: &str) -> Result<Pattern, String> {
    if pattern.is_empty() || pattern.chars().any(char::is_whitespace) {
        return Err(format!("pattern {pattern:?} is empty or has whitespace"));
    }
    if pattern == "*" {
        return Ok(Pattern::Any);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        if prefix.len() < 2 || !prefix.ends_with('.') || prefix.contains('*') {
            return Err(format!(
                "pattern {pattern:?}: a wildcard must be `*` or `prefix.*`"
            ));
        }
        return Ok(Pattern::Prefix(prefix.to_string()));
    }
    if pattern.contains('*') {
        return Err(format!(
            "pattern {pattern:?}: a wildcard must be `*` or `prefix.*`"
        ));
    }
    Ok(Pattern::Exact(pattern.to_string()))
}

enum Pattern {
    Any,
    /// With its trailing dot.
    Prefix(String),
    Exact(String),
}

fn pattern_matches(pattern: &str, ty: &str) -> bool {
    match parse_pattern(pattern) {
        Ok(Pattern::Any) => true,
        // Longer than the prefix: `orders.` is not an `orders.*` type.
        Ok(Pattern::Prefix(p)) => ty.len() > p.len() && ty.starts_with(&p),
        Ok(Pattern::Exact(e)) => e == ty,
        Err(_) => false,
    }
}

fn allowed(patterns: &[String], ty: &str) -> bool {
    patterns.iter().any(|p| pattern_matches(p, ty))
}

#[derive(Default)]
struct Auth {
    mode: Option<AuthMode>,
    tokens: HashMap<String, Acl>,
    /// The `[[service]]` table: azp → what that service may do.
    services: HashMap<String, Acl>,
}

/// A refused `Register`: the status it was closed with, the token it carried, when.
#[derive(Clone, Debug)]
pub struct Rejection {
    pub code: tonic::Code,
    pub message: String,
    pub token: Option<String>,
    pub at: std::time::Instant,
}

/// The identity of a stream, fixed by its first `Register`: `None` until then, `Some(None)` for a
/// session without a valid token (only possible in permissive mode).
type Principal = Option<Option<Acl>>;

pub struct State {
    auth: StdMutex<Auth>,
    rejections: StdMutex<Vec<Rejection>>,
    violations: StdMutex<Vec<String>>,
    pub registers: Mutex<Vec<proto::Register>>,
    /// Replies with the number of the stream (1-based, in accept order) they arrived on.
    pub replies: Mutex<Vec<(usize, proto::Reply)>>,
    pub requests: Mutex<Vec<proto::Request>>,
    heartbeats: AtomicUsize,
    reject_next: AtomicBool,
    reject_invalid_next: AtomicBool,
    echo_heartbeats: AtomicBool,
    drop_tx: broadcast::Sender<()>,
    /// Open streams: (stream number, outbound).
    clients: Mutex<Vec<(usize, Outbound)>>,
    responder: Mutex<Option<Responder>>,
    ack: StdMutex<Option<proto::RegisterAck>>,
    streams: AtomicUsize,
    flow_control: AtomicBool,
    /// Permits messages: (stream number, count, when).
    permits: StdMutex<Vec<(usize, u32, std::time::Instant)>>,
    /// Permits messages' grants: (stream number, grant).
    grants: StdMutex<Vec<(usize, u32)>>,
    /// Flow-controlled streams: stream number → (permits held, cap).
    held: StdMutex<HashMap<usize, (u32, u32)>>,
    permit_cap: AtomicBool,
    permit_grants: AtomicBool,
    instance_status: AtomicBool,
    /// InstanceStatus messages: (stream number, report).
    statuses: StdMutex<Vec<(usize, proto::InstanceStatus)>>,
    down: AtomicBool,
    replace_sessions: AtomicBool,
    stall_reads: AtomicBool,
    half_close: StdMutex<Option<HalfClose>>,
    /// instance id → (stream number, that stream's cancel token).
    sessions: StdMutex<HashMap<String, (usize, CancellationToken)>>,
    evictions: AtomicUsize,
    events: StdMutex<Vec<(Event, std::time::Instant)>>,
    scatter_gather: AtomicBool,
    streaming: AtomicBool,
    /// Whether idle streams end with TIMEOUT (`false`: a hub that never does, for local-guard tests).
    stream_timeouts: AtomicBool,
    /// stream number → its registration.
    routes: StdMutex<HashMap<usize, Route>>,
    /// gather id → the gather; target id → gather id.
    gathers: StdMutex<HashMap<String, Gather>>,
    target_of: StdMutex<HashMap<String, String>>,
    /// stream id → the stream; `<id>#1` → stream id.
    open_streams: StdMutex<HashMap<String, StreamState>>,
    stream_target_of: StdMutex<HashMap<String, String>>,
    /// Round-robin over the capable instances of a streaming query.
    rotation: AtomicUsize,
    subscription: AtomicBool,
    /// subscription id → the subscription; `<id>#n` / `<id>#0` → subscription id.
    subscriptions: StdMutex<HashMap<String, SubState>>,
    sub_target_of: StdMutex<HashMap<String, String>>,
    /// Round-robin over the targets for the initial result.
    sub_rotation: AtomicUsize,
    subscription_buffer: AtomicUsize,
    max_subscriptions_per_instance: AtomicUsize,
    subscription_max_duration: StdMutex<Option<Duration>>,
    /// Client-sent multi-part messages: (stream number, message).
    parts: StdMutex<Vec<(usize, proto::ReplyPart)>>,
    ends: StdMutex<Vec<(usize, proto::ReplyEnd)>>,
    flows: StdMutex<Vec<(usize, proto::Flow)>>,
    cancels: StdMutex<Vec<(usize, proto::Cancel)>>,
    /// The JWKS is loaded: JWT `Register`s are judged (else `UNAVAILABLE`).
    jwks_loaded: AtomicBool,
    /// How long the `RegisterAck` is held back after the `Register` was recorded (milliseconds).
    ack_delay_ms: AtomicU64,
    /// While set, every JWT `Register` and refresh is refused with this status (the hub judging the
    /// token: "token expired", "unknown signing key", ...).
    jwt_refusal: StdMutex<Option<(tonic::Code, String)>>,
    /// The status the next [`FakeHub::close_streams_with`] ends the open streams with.
    close_with: StdMutex<Option<(tonic::Code, String)>>,
    /// Offer `credentials-refresh` to clients that declare it.
    credentials_refresh: AtomicBool,
    /// `ClientMessage.credentials` (P5b): (stream number, the new credential).
    refreshes: StdMutex<Vec<(usize, proto::Credentials)>>,
    /// The leaf certificate each stream presented in the TLS handshake (DER), mutual TLS only.
    peers: StdMutex<Vec<(usize, Vec<u8>)>>,
}

impl State {
    fn mode(&self) -> AuthMode {
        self.auth.lock().unwrap().mode.unwrap_or(AuthMode::Off)
    }

    /// Judges a `Register`: `Err` closes the stream (enforce); violations of a permissive hub are
    /// recorded and allowed.
    fn admit(
        &self,
        stream_no: usize,
        r: &proto::Register,
        principal: &mut Principal,
    ) -> Result<(), tonic::Status> {
        let mode = self.mode();
        if mode == AuthMode::Off {
            return Ok(());
        }
        let mut found: Option<tonic::Status> = None;
        if principal.is_none() {
            let resolved = self.resolve(r.credentials.as_ref(), Some(&r.service_name));
            let acl = resolved.as_ref().ok().cloned();
            if let Err(status) = resolved {
                found = Some(status);
            }
            *principal = Some(acl);
        }
        if found.is_none()
            && let Some(Some(acl)) = principal
        {
            if acl.service != r.service_name {
                found = Some(tonic::Status::permission_denied(format!(
                    "token is not valid for service '{}'",
                    r.service_name
                )));
            } else if let Some(ty) = r
                .command_types
                .iter()
                .chain(&r.query_types)
                .chain(&r.shared_query_types)
                .find(|t| !allowed(&acl.handle, t))
            {
                found = Some(tonic::Status::permission_denied(format!(
                    "type '{ty}' is not allowed to be handled"
                )));
            }
        }
        // The hub refuses, in `permissive` too, a Register that would take over the instance id of
        // a session of another service (nothing changes: the holder keeps its session).
        if found.is_none() && self.held_by_another_service(stream_no, r) {
            return Err(tonic::Status::permission_denied(format!(
                "instance id {} belongs to another service",
                r.instance_id.chars().take(8).collect::<String>()
            )));
        }
        match found {
            Some(status) if mode == AuthMode::Enforce => Err(status),
            Some(status) => {
                self.violations.lock().unwrap().push(format!(
                    "{:?}: {}",
                    status.code(),
                    status.message()
                ));
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// The ACL entry a credential stands for: a token's entry, or (a JWT) the `[[service]]` entry
    /// its `azp` names, which must be `service` when that is known. A token's service-name check
    /// is the caller's.
    fn resolve(
        &self,
        credentials: Option<&proto::Credentials>,
        service: Option<&str>,
    ) -> Result<Acl, tonic::Status> {
        let auth = self.auth.lock().unwrap();
        match credentials.and_then(|c| c.kind.as_ref()) {
            Some(proto::credentials::Kind::Token(t)) if !t.is_empty() => auth
                .tokens
                .get(t)
                .cloned()
                .ok_or_else(|| tonic::Status::unauthenticated("unknown token")),
            Some(proto::credentials::Kind::Jwt(jwt)) if !jwt.is_empty() => {
                if !self.jwks_loaded.load(Ordering::SeqCst) {
                    return Err(tonic::Status::unavailable("jwks not loaded yet"));
                }
                if let Some((code, message)) = self.jwt_refusal.lock().unwrap().clone() {
                    return Err(tonic::Status::new(code, message));
                }
                let azp = jwt_claims(jwt)
                    .and_then(|c| c["azp"].as_str().map(str::to_string))
                    .ok_or_else(|| tonic::Status::unauthenticated("malformed jwt (no azp)"))?;
                if let Some(service) = service.filter(|s| *s != azp) {
                    return Err(tonic::Status::permission_denied(format!(
                        "token azp '{azp}' is not service '{service}'"
                    )));
                }
                auth.services.get(&azp).cloned().ok_or_else(|| {
                    tonic::Status::permission_denied(format!("no ACL entry for service '{azp}'"))
                })
            }
            _ => Err(tonic::Status::unauthenticated("missing token")),
        }
    }

    /// Judges a `ClientMessage.credentials`: it must stand for the session's own service. `Err`
    /// closes the stream (enforce); a permissive hub records it and keeps the old principal.
    fn admit_refresh(
        &self,
        credentials: &proto::Credentials,
        principal: &mut Principal,
    ) -> Result<(), tonic::Status> {
        let mode = self.mode();
        if mode == AuthMode::Off {
            return Ok(());
        }
        let session = principal.as_ref().and_then(Option::as_ref);
        let outcome = self
            .resolve(Some(credentials), session.map(|a| a.service.as_str()))
            .and_then(|acl| match session {
                Some(own) if own.service != acl.service => {
                    Err(tonic::Status::permission_denied(format!(
                        "credentials are for service '{}', not '{}'",
                        acl.service, own.service
                    )))
                }
                _ => Ok(acl),
            });
        match outcome {
            Ok(acl) => {
                *principal = Some(Some(acl));
                Ok(())
            }
            Err(status) if mode == AuthMode::Enforce => Err(status),
            Err(status) => {
                self.violations.lock().unwrap().push(format!(
                    "refresh {:?}: {}",
                    status.code(),
                    status.message()
                ));
                Ok(())
            }
        }
    }

    /// The instance id of `r` is held by an open session of another service.
    fn held_by_another_service(&self, stream_no: usize, r: &proto::Register) -> bool {
        let holder = match self.sessions.lock().unwrap().get(&r.instance_id) {
            Some((holder, _)) if *holder != stream_no => *holder,
            _ => return false,
        };
        self.routes
            .lock()
            .unwrap()
            .get(&holder)
            .is_some_and(|route| route.service != r.service_name)
    }

    /// An immediate `Error{PERMISSION_DENIED}` for a request outside the session's `send`.
    fn check_send(&self, principal: &Principal, req: &proto::Request) -> Option<ServerMessage> {
        let mode = self.mode();
        let Some(Some(acl)) = principal else {
            return None;
        };
        if mode == AuthMode::Off || allowed(&acl.send, &req.r#type) {
            return None;
        }
        if mode == AuthMode::Permissive {
            self.violations
                .lock()
                .unwrap()
                .push(format!("send {}", req.r#type));
            return None;
        }
        Some(hub_error(
            &req.id,
            proto::ErrorCode::PermissionDenied,
            &format!("type '{}' is not allowed to be sent", req.r#type),
        ))
    }
}

/// The token or JWT a credential carries, for [`Rejection::token`].
fn credential_text(credentials: Option<&proto::Credentials>) -> Option<String> {
    match credentials.and_then(|c| c.kind.as_ref()) {
        Some(proto::credentials::Kind::Token(t)) => Some(t.clone()),
        Some(proto::credentials::Kind::Jwt(j)) => Some(j.clone()),
        None => None,
    }
}

/// The claims of a JWT, read without verifying it (the fake hub trusts its tests).
pub fn jwt_claims(jwt: &str) -> Option<serde_json::Value> {
    use base64::Engine;
    let payload = jwt.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// What happened on the fake hub, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A `Register` on stream `stream` announcing `types` command and query types.
    Register { stream: usize, types: usize },
    /// Stream `stream` ended (either side).
    Closed { stream: usize },
    /// Stream `stream` lost its session to a newer stream of the same instance id.
    Evicted { stream: usize },
}

impl State {
    fn log(&self, event: Event) {
        self.events
            .lock()
            .unwrap()
            .push((event, std::time::Instant::now()));
    }

    /// A scatter-gather `Request` from stream `stream_no`: rejected, or fanned out. `false` when
    /// that stream's ack never listed the capability: the request is to be served as unary (an
    /// older hub does not know `mode`).
    async fn scatter(
        self: &Arc<Self>,
        stream_no: usize,
        req: &proto::Request,
        tx: &Outbound,
    ) -> bool {
        use proto::ErrorCode;
        let reject = |code, message: &str| hub_error(&req.id, code, message);
        let listed = self
            .routes
            .lock()
            .unwrap()
            .get(&stream_no)
            .is_some_and(|r| r.scatter);
        if !listed {
            return false;
        }
        if !self.scatter_gather.load(Ordering::SeqCst) {
            let _ = tx
                .send(Ok(reject(
                    ErrorCode::Unsupported,
                    "scatter-gather not supported",
                )))
                .await;
            return true;
        }
        let (is_command, mut targets) = {
            let routes = self.routes.lock().unwrap();
            let is_command = req.kind == proto::RequestKind::Command as i32
                || routes.values().any(|r| r.commands.contains(&req.r#type));
            let targets: Vec<(usize, String, String, Outbound)> = routes
                .iter()
                .filter(|(_, r)| r.queries.contains(&req.r#type))
                .map(|(n, r)| (*n, r.service.clone(), r.instance.clone(), r.tx.clone()))
                .collect();
            (is_command, targets)
        };
        if is_command {
            let _ = tx
                .send(Ok(reject(
                    ErrorCode::InvalidRequest,
                    "invalid request: a command",
                )))
                .await;
            return true;
        }
        if targets.is_empty() {
            let _ = tx
                .send(Ok(reject(ErrorCode::NoHandler, "no handler")))
                .await;
            return true;
        }
        targets.sort_by(|a, b| (&a.1, &a.2).cmp(&(&b.1, &b.2)));
        let ids: Vec<String> = (0..targets.len())
            .map(|n| format!("{}#{n}", req.id))
            .collect();
        {
            let mut gathers = self.gathers.lock().unwrap();
            let mut target_of = self.target_of.lock().unwrap();
            gathers.insert(
                req.id.clone(),
                Gather {
                    sender: tx.clone(),
                    outstanding: ids
                        .iter()
                        .zip(&targets)
                        .map(|(id, (n, s, i, _))| (id.clone(), (*n, s.clone(), i.clone())))
                        .collect(),
                    parts: 0,
                    ok: 0,
                    failed: 0,
                    max_results: req.max_results,
                },
            );
            for id in &ids {
                target_of.insert(id.clone(), req.id.clone());
            }
        }
        let timeout = if req.timeout_ms > 0 {
            req.timeout_ms
        } else {
            5_000
        };
        let me = Arc::clone(self);
        let gather = req.id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(timeout.into())).await;
            deliver(me.close(&gather, Closed::Deadline)).await;
        });
        for (id, (_, _, _, target)) in ids.into_iter().zip(targets) {
            let forwarded = proto::Request {
                id,
                mode: proto::RequestMode::Unary as i32,
                max_results: 0,
                ..req.clone()
            };
            let _ = target
                .send(Ok(server(server_message::Message::Request(forwarded))))
                .await;
        }
        true
    }

    /// The answer of scatter-gather target `id`, if it is one: its part, and the end when done.
    async fn answer(&self, id: &str, result: Result<Vec<u8>, proto::Error>) {
        let out = {
            let Some(parent) = self.target_of.lock().unwrap().remove(id) else {
                return;
            };
            let mut gathers = self.gathers.lock().unwrap();
            let Some(g) = gathers.get_mut(&parent) else {
                return;
            };
            let Some((_, service, instance)) = g.outstanding.remove(id) else {
                return;
            };
            let seq = u64::from(g.parts);
            g.parts += 1;
            let (payload, error) = match result {
                Ok(p) => {
                    g.ok += 1;
                    (p, None)
                }
                Err(mut e) => {
                    g.failed += 1;
                    e.request_id = parent.clone();
                    (vec![], Some(e))
                }
            };
            let part = server(server_message::Message::Part(proto::ReplyPart {
                request_id: parent.clone(),
                payload,
                error,
                responder_service: service,
                responder_instance: instance,
                seq,
            }));
            let done = if g.outstanding.is_empty() {
                Some(Closed::Complete)
            } else if g.max_results > 0 && g.ok >= g.max_results {
                Some(Closed::MaxResults)
            } else {
                None
            };
            let sender = g.sender.clone();
            drop(gathers);
            let mut out = vec![(sender, part)];
            if let Some(how) = done {
                out.extend(self.close(&parent, how));
            }
            out
        };
        deliver(out).await;
    }

    /// Ends gather `id` (if still open): its `ReplyEnd` (unless the sender is gone), the
    /// unanswered targets timed out at the deadline, and a `Cancel` to each still-outstanding
    /// target whose session negotiated "streaming" when the gather ends early.
    fn close(&self, id: &str, how: Closed) -> Vec<(Outbound, ServerMessage)> {
        let Some(g) = self.gathers.lock().unwrap().remove(id) else {
            return vec![];
        };
        let mut out = vec![];
        {
            let mut target_of = self.target_of.lock().unwrap();
            for t in g.outstanding.keys() {
                target_of.remove(t);
            }
        }
        if how != Closed::SenderGone {
            out.push((
                g.sender,
                server(server_message::Message::End(proto::ReplyEnd {
                    request_id: id.to_string(),
                    error: None,
                    parts: g.parts,
                    failed: g.failed,
                    timed_out: if how == Closed::Deadline {
                        g.outstanding.len() as u32
                    } else {
                        0
                    },
                })),
            ));
        }
        let reason = match how {
            Closed::Complete => return out,
            Closed::MaxResults => "max results",
            Closed::Deadline => "deadline",
            Closed::SenderGone => "sender disconnected",
        };
        let routes = self.routes.lock().unwrap();
        let mut targets: Vec<(&String, &(usize, String, String))> = g.outstanding.iter().collect();
        targets.sort();
        for (target_id, (n, _, _)) in targets {
            if let Some(r) = routes.get(n).filter(|r| r.streaming || r.subscription) {
                out.push((r.tx.clone(), cancel_msg(target_id, reason)));
            }
        }
        out
    }

    /// A subscription `Request` from stream `stream_no`: rejected, or forwarded to every capable
    /// instance (and the initial request to one of them). `false` when the session knows no
    /// query modes at all (served as unary).
    async fn open_subscription(
        self: &Arc<Self>,
        stream_no: usize,
        req: &proto::Request,
        tx: &Outbound,
    ) -> bool {
        use proto::ErrorCode;
        let reject = |code, message: &str| hub_error(&req.id, code, message);
        let (knows_modes, listed) = self
            .routes
            .lock()
            .unwrap()
            .get(&stream_no)
            .map(|r| (r.scatter || r.streaming || r.subscription, r.subscription))
            .unwrap_or((false, false));
        if !knows_modes {
            return false;
        }
        if !listed || !self.subscription.load(Ordering::SeqCst) {
            let _ = tx
                .send(Ok(reject(
                    ErrorCode::Unsupported,
                    "subscription not negotiated",
                )))
                .await;
            return true;
        }
        let (is_command, owners, mut capable) = {
            let routes = self.routes.lock().unwrap();
            let is_command = req.kind == proto::RequestKind::Command as i32
                || routes.values().any(|r| r.commands.contains(&req.r#type));
            let owners: Vec<String> = routes
                .values()
                .filter(|r| r.queries.contains(&req.r#type))
                .map(|r| r.service.clone())
                .collect();
            let capable: Vec<(usize, String, String, Outbound)> = routes
                .iter()
                .filter(|(_, r)| r.queries.contains(&req.r#type) && r.subscription)
                .map(|(n, r)| (*n, r.service.clone(), r.instance.clone(), r.tx.clone()))
                .collect();
            (is_command, owners, capable)
        };
        capable.sort_by(|a, b| (&a.1, &a.2).cmp(&(&b.1, &b.2)));
        let max = self.max_subscriptions_per_instance.load(Ordering::SeqCst);
        let rejection = if is_command {
            Some(reject(
                ErrorCode::InvalidRequest,
                "invalid request: a command",
            ))
        } else if owners.is_empty() {
            Some(reject(ErrorCode::NoHandler, "no handler"))
        } else if capable.is_empty() {
            Some(reject(
                ErrorCode::Unsupported,
                &format!("no instance of {owners:?} negotiated subscription"),
            ))
        } else if self.subscriptions.lock().unwrap().contains_key(&req.id)
            || self.open_streams.lock().unwrap().contains_key(&req.id)
            || self.gathers.lock().unwrap().contains_key(&req.id)
        {
            Some(reject(ErrorCode::InvalidRequest, "duplicate id"))
        } else if let Some((_, _, instance, _)) = {
            let subscriptions = self.subscriptions.lock().unwrap();
            capable.iter().find(|(n, _, _, _)| {
                subscriptions
                    .values()
                    .flat_map(|s| s.targets.values())
                    .filter(|(target_no, _, _, _)| target_no == n)
                    .count()
                    >= max
            })
        } {
            Some(reject(
                ErrorCode::ResourceExhausted,
                &format!(
                    "handler instance {instance} already serves {max} subscription{} (KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE)",
                    if max == 1 { "" } else { "s" }
                ),
            ))
        } else {
            None
        };
        if let Some(rejection) = rejection {
            let _ = tx.send(Ok(rejection)).await;
            return true;
        }
        let pick = self.sub_rotation.fetch_add(1, Ordering::SeqCst) % capable.len();
        let (initial_no, initial_service, initial_instance, initial_tx) = capable[pick].clone();
        let credit = if req.initial_credit == 0 {
            64
        } else {
            req.initial_credit
        };
        let ids: Vec<String> = (1..=capable.len())
            .map(|n| format!("{}#{n}", req.id))
            .collect();
        let initial_id = format!("{}#0", req.id);
        {
            let mut subscriptions = self.subscriptions.lock().unwrap();
            subscriptions.insert(
                req.id.clone(),
                SubState {
                    sender: tx.clone(),
                    sender_no: stream_no,
                    targets: ids.iter().cloned().zip(capable.iter().cloned()).collect(),
                    initial: Some((
                        initial_no,
                        initial_service,
                        initial_instance,
                        initial_tx.clone(),
                    )),
                    initial_done: false,
                    credit,
                    parts: 0,
                    updates: 0,
                    max_results: req.max_results,
                    buffered: VecDeque::new(),
                },
            );
            let mut target_of = self.sub_target_of.lock().unwrap();
            for id in &ids {
                target_of.insert(id.clone(), req.id.clone());
            }
            target_of.insert(initial_id.clone(), req.id.clone());
        }
        // The initial request's deadline, as a unary request's.
        let timeout = if req.timeout_ms > 0 {
            req.timeout_ms
        } else {
            5_000
        };
        let me = Arc::clone(self);
        let id = req.id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(timeout.into())).await;
            if !me.stream_timeouts.load(Ordering::SeqCst) {
                return;
            }
            let pending = me
                .subscriptions
                .lock()
                .unwrap()
                .get(&id)
                .is_some_and(|s| !s.initial_done);
            if pending {
                let end = me.end_subscription(
                    &id,
                    Some(Some(error(
                        &id,
                        proto::ErrorCode::Timeout,
                        "initial request timed out",
                    ))),
                    Some("initial result failed"),
                );
                deliver(end).await;
            }
        });
        if let Some(max) = *self.subscription_max_duration.lock().unwrap() {
            let me = Arc::clone(self);
            let id = req.id.clone();
            tokio::spawn(async move {
                tokio::time::sleep(max).await;
                let message = format!("max subscription duration ({max:?}) exceeded");
                let end = me.end_subscription(
                    &id,
                    Some(Some(error(&id, proto::ErrorCode::Cancelled, &message))),
                    Some(&message),
                );
                deliver(end).await;
            });
        }
        // Written directly, no permit: the targets first, then the initial request.
        for (id, (_, _, _, target)) in ids.iter().zip(&capable) {
            let forwarded = proto::Request {
                id: id.clone(),
                mode: proto::RequestMode::Subscription as i32,
                initial_credit: 0,
                max_results: 0,
                ..req.clone()
            };
            let _ = target
                .send(Ok(server(server_message::Message::Request(forwarded))))
                .await;
        }
        let initial = proto::Request {
            id: initial_id,
            mode: proto::RequestMode::Unary as i32,
            initial_credit: 0,
            max_results: 0,
            ..req.clone()
        };
        let _ = initial_tx
            .send(Ok(server(server_message::Message::Request(initial))))
            .await;
        true
    }

    /// The `Reply` to `<id>#0` from stream `stream_no`, if it is a subscription's initial
    /// request: the first part (`seq` 1), then the buffered updates under the credit; an error
    /// ends the subscription.
    async fn initial_reply(
        &self,
        stream_no: usize,
        id: &str,
        result: Result<Vec<u8>, proto::Error>,
    ) {
        let Some(parent) = self.sub_target_of.lock().unwrap().get(id).cloned() else {
            return;
        };
        if !id.ends_with("#0") {
            return;
        }
        let out = {
            let mut subscriptions = self.subscriptions.lock().unwrap();
            let Some(st) = subscriptions.get_mut(&parent) else {
                return;
            };
            if !st.initial.as_ref().is_some_and(|i| i.0 == stream_no) {
                return;
            }
            let Some((_, service, instance, _)) = st.initial.take() else {
                return;
            };
            match result {
                Ok(payload) => {
                    st.initial_done = true;
                    st.parts += 1;
                    st.credit = st.credit.saturating_sub(1);
                    let part = server(server_message::Message::Part(proto::ReplyPart {
                        request_id: parent.clone(),
                        payload,
                        error: None,
                        responder_service: service,
                        responder_instance: instance,
                        seq: u64::from(st.parts),
                    }));
                    let mut out = vec![(st.sender.clone(), part)];
                    let (drained, maxed) = Self::drain(&parent, st);
                    out.extend(drained);
                    let complete = st.targets.is_empty() && st.buffered.is_empty() && !maxed;
                    drop(subscriptions);
                    if maxed {
                        out.extend(self.end_subscription(&parent, Some(None), Some("max results")));
                    } else if complete {
                        out.extend(self.end_subscription(&parent, Some(None), None));
                    }
                    out
                }
                Err(e) => {
                    drop(subscriptions);
                    self.end_subscription(&parent, Some(Some(e)), Some("initial result failed"))
                }
            }
        };
        self.sub_target_of.lock().unwrap().remove(id);
        deliver(out).await;
    }

    /// Relays the buffered updates of `id` while it has credit, with the hub's `seq`. True when
    /// `max_results` is reached.
    fn drain(id: &str, st: &mut SubState) -> (Vec<(Outbound, ServerMessage)>, bool) {
        let mut out = vec![];
        while st.credit > 0 {
            let Some((payload, service, instance)) = st.buffered.pop_front() else {
                return (out, false);
            };
            st.credit -= 1;
            st.parts += 1;
            st.updates += 1;
            out.push((
                st.sender.clone(),
                server(server_message::Message::Part(proto::ReplyPart {
                    request_id: id.to_string(),
                    payload,
                    error: None,
                    responder_service: service,
                    responder_instance: instance,
                    seq: u64::from(st.parts),
                })),
            ));
            if st.max_results > 0 && st.updates >= st.max_results {
                return (out, true);
            }
        }
        (out, false)
    }

    /// Ends subscription `id` (if still open): `end` is `Some(error)` for a `ReplyEnd` to the
    /// sender (`None`: nothing more to the sender), `cancel` the reason of a `Cancel` to every
    /// target still serving (and to `<id>#0` while forwarded).
    fn end_subscription(
        &self,
        id: &str,
        end: Option<Option<proto::Error>>,
        cancel: Option<&str>,
    ) -> Vec<(Outbound, ServerMessage)> {
        let Some(st) = self.subscriptions.lock().unwrap().remove(id) else {
            return vec![];
        };
        {
            let mut target_of = self.sub_target_of.lock().unwrap();
            for t in st.targets.keys() {
                target_of.remove(t);
            }
            target_of.remove(&format!("{id}#0"));
        }
        let mut out = vec![];
        if let Some(error) = end {
            let error = error.map(|mut e| {
                e.request_id = id.to_string();
                e
            });
            out.push((
                st.sender.clone(),
                server(server_message::Message::End(proto::ReplyEnd {
                    request_id: id.to_string(),
                    error,
                    parts: st.parts,
                    failed: 0,
                    timed_out: 0,
                })),
            ));
        }
        if let Some(reason) = cancel {
            if let Some((_, _, _, initial_tx)) = &st.initial {
                out.push((initial_tx.clone(), cancel_msg(&format!("{id}#0"), reason)));
            }
            let mut targets: Vec<(&String, &Target)> = st.targets.iter().collect();
            targets.sort_by(|a, b| a.0.cmp(b.0));
            for (target_id, (_, _, _, target)) in targets {
                out.push((target.clone(), cancel_msg(target_id, reason)));
            }
        }
        out
    }

    /// A target's `ReplyPart` for `<id>#n` from stream `stream_no`: an update, relayed under
    /// the credit (buffered behind the initial part and beyond the credit).
    fn on_sub_part(
        &self,
        stream_no: usize,
        p: proto::ReplyPart,
    ) -> Option<Vec<(Outbound, ServerMessage)>> {
        let parent = self
            .sub_target_of
            .lock()
            .unwrap()
            .get(&p.request_id)
            .cloned()?;
        let cap = self.subscription_buffer.load(Ordering::SeqCst);
        let ended = {
            let mut subscriptions = self.subscriptions.lock().unwrap();
            let st = subscriptions.get_mut(&parent)?;
            let (_, service, instance, _) = st
                .targets
                .get(&p.request_id)
                .filter(|t| t.0 == stream_no)?
                .clone();
            if let Some(e) = p.error {
                st.targets.remove(&p.request_id);
                Err((Some(Some(e)), format!("target {instance} failed")))
            } else if st.buffered.len() >= cap {
                Err((
                    Some(Some(error(
                        &parent,
                        proto::ErrorCode::ResourceExhausted,
                        &format!("subscriber buffer ({cap}) exceeded"),
                    ))),
                    "subscriber buffer exceeded".to_string(),
                ))
            } else {
                st.buffered.push_back((p.payload, service, instance));
                if st.initial_done {
                    Ok(Self::drain(&parent, st))
                } else {
                    Ok((vec![], false))
                }
            }
        };
        Some(match ended {
            Ok((mut out, maxed)) => {
                if maxed {
                    out.extend(self.end_subscription(&parent, Some(None), Some("max results")));
                }
                out
            }
            Err((end, reason)) => self.end_subscription(&parent, end, Some(&reason)),
        })
    }

    /// A target's `ReplyEnd` for `<id>#n` from stream `stream_no`: that target is done; the
    /// last one's clean end ends the subscription once the buffer is out, an error ends it now.
    fn on_sub_end(
        &self,
        stream_no: usize,
        e: proto::ReplyEnd,
    ) -> Option<Vec<(Outbound, ServerMessage)>> {
        let parent = self
            .sub_target_of
            .lock()
            .unwrap()
            .get(&e.request_id)
            .cloned()?;
        let (instance, complete) = {
            let mut subscriptions = self.subscriptions.lock().unwrap();
            let st = subscriptions.get_mut(&parent)?;
            let (_, _, instance, _) = st
                .targets
                .get(&e.request_id)
                .filter(|t| t.0 == stream_no)?
                .clone();
            st.targets.remove(&e.request_id);
            (
                instance,
                st.targets.is_empty() && st.initial_done && st.buffered.is_empty(),
            )
        };
        self.sub_target_of.lock().unwrap().remove(&e.request_id);
        Some(match e.error {
            Some(error) => self.end_subscription(
                &parent,
                Some(Some(error)),
                Some(&format!("target {instance} failed")),
            ),
            None if complete => self.end_subscription(&parent, Some(None), None),
            None => vec![],
        })
    }

    /// A sender's `Flow` for a subscription: more credit, the buffer drains (nothing to the
    /// targets). `None` when `id` is no subscription of that sender.
    fn on_sub_flow(
        &self,
        stream_no: usize,
        f: &proto::Flow,
    ) -> Option<Vec<(Outbound, ServerMessage)>> {
        let (mut out, maxed, complete) = {
            let mut subscriptions = self.subscriptions.lock().unwrap();
            let st = subscriptions
                .get_mut(&f.request_id)
                .filter(|s| s.sender_no == stream_no)?;
            st.credit = st.credit.saturating_add(f.credits);
            let (out, maxed) = if st.initial_done {
                Self::drain(&f.request_id, st)
            } else {
                (vec![], false)
            };
            (
                out,
                maxed,
                st.targets.is_empty() && st.initial_done && st.buffered.is_empty(),
            )
        };
        if maxed {
            out.extend(self.end_subscription(&f.request_id, Some(None), Some("max results")));
        } else if complete {
            out.extend(self.end_subscription(&f.request_id, Some(None), None));
        }
        Some(out)
    }

    /// A sender's `Cancel` for a subscription: every target is cancelled, nothing more to the
    /// sender. `None` when `id` is no subscription of that sender.
    fn on_sub_cancel(
        &self,
        stream_no: usize,
        c: &proto::Cancel,
    ) -> Option<Vec<(Outbound, ServerMessage)>> {
        if !self
            .subscriptions
            .lock()
            .unwrap()
            .get(&c.request_id)
            .is_some_and(|s| s.sender_no == stream_no)
        {
            return None;
        }
        Some(self.end_subscription(&c.request_id, None, Some(&c.reason)))
    }

    /// Stream `stream_no` ended: the subscriptions it sent are cancelled at their targets; the
    /// ones it served lose that target (silently, unless it was the last one or the initial's
    /// handler before the initial part).
    fn subscriber_or_target_closed(&self, stream_no: usize) -> Vec<(Outbound, ServerMessage)> {
        let mut out = vec![];
        let sent: Vec<String> = self
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, s)| s.sender_no == stream_no)
            .map(|(id, _)| id.clone())
            .collect();
        for id in sent {
            out.extend(self.end_subscription(&id, None, Some("sender disconnected")));
        }
        let served: Vec<String> = self
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, s)| {
                s.targets.values().any(|t| t.0 == stream_no)
                    || s.initial.as_ref().is_some_and(|i| i.0 == stream_no)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in served {
            let (instance, initial_lost, last) = {
                let mut subscriptions = self.subscriptions.lock().unwrap();
                let Some(st) = subscriptions.get_mut(&id) else {
                    continue;
                };
                let gone: Vec<String> = st
                    .targets
                    .iter()
                    .filter(|(_, t)| t.0 == stream_no)
                    .map(|(t, _)| t.clone())
                    .collect();
                let mut instance = String::new();
                for t in &gone {
                    if let Some((_, _, i, _)) = st.targets.remove(t) {
                        instance = i;
                    }
                    self.sub_target_of.lock().unwrap().remove(t);
                }
                let initial_lost = st.initial.as_ref().is_some_and(|i| i.0 == stream_no);
                if initial_lost {
                    st.initial = None;
                }
                (instance, initial_lost, st.targets.is_empty())
            };
            let error = error(
                &id,
                proto::ErrorCode::HandlerDisconnected,
                &format!("handler instance {instance} disconnected"),
            );
            if initial_lost {
                out.extend(self.end_subscription(
                    &id,
                    Some(Some(error)),
                    Some("initial result failed"),
                ));
            } else if last {
                out.extend(self.end_subscription(&id, Some(Some(error)), None));
            }
        }
        out
    }

    /// A streaming `Request` from stream `stream_no`: rejected, or forwarded to one capable
    /// instance. `false` when the session knows no query modes at all (served as unary).
    async fn open_stream(
        self: &Arc<Self>,
        stream_no: usize,
        req: &proto::Request,
        tx: &Outbound,
    ) -> bool {
        use proto::ErrorCode;
        let reject = |code, message: &str| hub_error(&req.id, code, message);
        let (knows_modes, listed) = self
            .routes
            .lock()
            .unwrap()
            .get(&stream_no)
            .map(|r| (r.scatter || r.streaming, r.streaming))
            .unwrap_or((false, false));
        if !knows_modes {
            return false;
        }
        if !listed || !self.streaming.load(Ordering::SeqCst) {
            let _ = tx
                .send(Ok(reject(
                    ErrorCode::Unsupported,
                    "streaming not negotiated",
                )))
                .await;
            return true;
        }
        let (is_command, owners, mut capable) = {
            let routes = self.routes.lock().unwrap();
            let is_command = req.kind == proto::RequestKind::Command as i32
                || routes.values().any(|r| r.commands.contains(&req.r#type));
            let owners: Vec<String> = routes
                .values()
                .filter(|r| r.queries.contains(&req.r#type))
                .map(|r| r.service.clone())
                .collect();
            let capable: Vec<(usize, String, String, Outbound)> = routes
                .iter()
                .filter(|(_, r)| r.queries.contains(&req.r#type) && r.streaming)
                .map(|(n, r)| (*n, r.service.clone(), r.instance.clone(), r.tx.clone()))
                .collect();
            (is_command, owners, capable)
        };
        let rejection = if is_command {
            Some(reject(
                ErrorCode::InvalidRequest,
                "invalid request: a command",
            ))
        } else if owners.is_empty() {
            Some(reject(ErrorCode::NoHandler, "no handler"))
        } else if capable.is_empty() {
            Some(reject(
                ErrorCode::Unsupported,
                &format!("no instance of {owners:?} negotiated streaming"),
            ))
        } else if self.open_streams.lock().unwrap().contains_key(&req.id)
            || self.gathers.lock().unwrap().contains_key(&req.id)
        {
            Some(reject(ErrorCode::InvalidRequest, "duplicate id"))
        } else {
            None
        };
        if let Some(rejection) = rejection {
            let _ = tx.send(Ok(rejection)).await;
            return true;
        }
        capable.sort_by(|a, b| (&a.1, &a.2).cmp(&(&b.1, &b.2)));
        let pick = self.rotation.fetch_add(1, Ordering::SeqCst) % capable.len();
        let (target_no, service, instance, target) = capable.swap_remove(pick);
        let credit = if req.initial_credit == 0 {
            64
        } else {
            req.initial_credit
        };
        let idle = Duration::from_millis(
            if req.timeout_ms > 0 {
                req.timeout_ms
            } else {
                5_000
            }
            .into(),
        );
        let target_id = format!("{}#1", req.id);
        self.open_streams.lock().unwrap().insert(
            req.id.clone(),
            StreamState {
                sender: tx.clone(),
                sender_no: stream_no,
                target_no,
                service,
                instance,
                target: target.clone(),
                credit,
                parts: 0,
                max_results: req.max_results,
                idle,
                deadline: Some(Instant::now() + idle),
            },
        );
        self.stream_target_of
            .lock()
            .unwrap()
            .insert(target_id.clone(), req.id.clone());
        let me = Arc::clone(self);
        let id = req.id.clone();
        tokio::spawn(async move {
            loop {
                let Some(deadline) = me.open_streams.lock().unwrap().get(&id).map(|s| s.deadline)
                else {
                    return;
                };
                if !me.stream_timeouts.load(Ordering::SeqCst) {
                    return;
                }
                // Paused (credit 0): look again shortly, the Flow that resumes it re-arms it.
                let Some(deadline) = deadline else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                if Instant::now() >= deadline {
                    let end = me.end_stream(
                        &id,
                        Some(Some(error(&id, proto::ErrorCode::Timeout, "idle timeout"))),
                        Some("idle timeout"),
                    );
                    deliver(end).await;
                    return;
                }
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            }
        });
        let forwarded = proto::Request {
            id: target_id,
            mode: proto::RequestMode::Streaming as i32,
            initial_credit: credit,
            max_results: 0,
            ..req.clone()
        };
        let _ = target
            .send(Ok(server(server_message::Message::Request(forwarded))))
            .await;
        true
    }

    /// Ends stream `id` (if still open): `end` is `Some(error)` for a `ReplyEnd` to the sender
    /// (`None`: nothing more to the sender), `cancel` the reason of a `Cancel` to the handler.
    fn end_stream(
        &self,
        id: &str,
        end: Option<Option<proto::Error>>,
        cancel: Option<&str>,
    ) -> Vec<(Outbound, ServerMessage)> {
        let Some(st) = self.open_streams.lock().unwrap().remove(id) else {
            return vec![];
        };
        let target_id = format!("{id}#1");
        self.stream_target_of.lock().unwrap().remove(&target_id);
        let mut out = vec![];
        if let Some(error) = end {
            let error = error.map(|mut e| {
                e.request_id = id.to_string();
                e
            });
            out.push((
                st.sender.clone(),
                server(server_message::Message::End(proto::ReplyEnd {
                    request_id: id.to_string(),
                    error,
                    parts: st.parts,
                    failed: 0,
                    timed_out: 0,
                })),
            ));
        }
        if let Some(reason) = cancel {
            out.push((st.target.clone(), cancel_msg(&target_id, reason)));
        }
        out
    }

    /// A handler's `ReplyPart` for `<id>#1` from stream `stream_no`: relayed under the credit.
    fn on_part(&self, stream_no: usize, p: proto::ReplyPart) -> Vec<(Outbound, ServerMessage)> {
        let Some(parent) = self
            .stream_target_of
            .lock()
            .unwrap()
            .get(&p.request_id)
            .cloned()
        else {
            return vec![];
        };
        let relayed = {
            let mut streams = self.open_streams.lock().unwrap();
            let Some(st) = streams
                .get_mut(&parent)
                .filter(|s| s.target_no == stream_no)
            else {
                return vec![];
            };
            if let Some(e) = p.error {
                Err((Some(Some(e)), None))
            } else if st.credit == 0 {
                Err((
                    Some(Some(error(
                        &parent,
                        proto::ErrorCode::InvalidRequest,
                        "part without credit",
                    ))),
                    Some("part without credit"),
                ))
            } else {
                st.credit -= 1;
                st.parts += 1;
                st.deadline = (st.credit > 0).then(|| Instant::now() + st.idle);
                let part = server(server_message::Message::Part(proto::ReplyPart {
                    request_id: parent.clone(),
                    payload: p.payload,
                    error: None,
                    responder_service: st.service.clone(),
                    responder_instance: st.instance.clone(),
                    seq: p.seq,
                }));
                let maxed = st.max_results > 0 && st.parts >= st.max_results;
                Ok((st.sender.clone(), part, maxed))
            }
        };
        match relayed {
            Ok((sender, part, maxed)) => {
                let mut out = vec![(sender, part)];
                if maxed {
                    out.extend(self.end_stream(&parent, Some(None), Some("max results")));
                }
                out
            }
            Err((end, cancel)) => self.end_stream(&parent, end, cancel),
        }
    }

    /// A handler's `ReplyEnd` for `<id>#1` from stream `stream_no`: relayed.
    fn on_end(&self, stream_no: usize, e: proto::ReplyEnd) -> Vec<(Outbound, ServerMessage)> {
        let Some(parent) = self
            .stream_target_of
            .lock()
            .unwrap()
            .get(&e.request_id)
            .cloned()
        else {
            return vec![];
        };
        if !self
            .open_streams
            .lock()
            .unwrap()
            .get(&parent)
            .is_some_and(|s| s.target_no == stream_no)
        {
            return vec![];
        }
        self.end_stream(&parent, Some(e.error), None)
    }

    /// A sender's `Flow` from stream `stream_no`: more credit, relayed to the handler.
    fn on_flow(&self, stream_no: usize, f: proto::Flow) -> Vec<(Outbound, ServerMessage)> {
        if f.credits == 0 {
            return vec![];
        }
        let mut streams = self.open_streams.lock().unwrap();
        let Some(st) = streams
            .get_mut(&f.request_id)
            .filter(|s| s.sender_no == stream_no)
        else {
            return vec![];
        };
        if st.credit == 0 {
            st.deadline = Some(Instant::now() + st.idle);
        }
        st.credit = st.credit.saturating_add(f.credits);
        vec![(
            st.target.clone(),
            server(server_message::Message::Flow(proto::Flow {
                request_id: format!("{}#1", f.request_id),
                credits: f.credits,
            })),
        )]
    }

    /// A sender's `Cancel` from stream `stream_no`: the handler is cancelled, nothing more to
    /// the sender.
    fn on_cancel(&self, stream_no: usize, c: proto::Cancel) -> Vec<(Outbound, ServerMessage)> {
        if !self
            .open_streams
            .lock()
            .unwrap()
            .get(&c.request_id)
            .is_some_and(|s| s.sender_no == stream_no)
        {
            return vec![];
        }
        self.end_stream(&c.request_id, None, Some(&c.reason))
    }

    /// Stream `stream_no` (written through `tx`) ended: its gathers end silently (their targets
    /// cancelled), it is no longer a target and its unanswered targets fail as parts (unless
    /// every stream was `dropped`); the streams it sent are cancelled at their handlers, the
    /// streams it handled end with `HANDLER_DISCONNECTED`.
    async fn stream_closed(&self, stream_no: usize, tx: &Outbound, dropped: bool) {
        self.routes.lock().unwrap().remove(&stream_no);
        let gone: Vec<String> = self
            .gathers
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, g)| g.sender.same_channel(tx))
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            let msgs = self.close(&id, Closed::SenderGone);
            if !dropped {
                deliver(msgs).await;
            }
        }
        let (sent, handled): (Vec<String>, Vec<String>) = {
            let streams = self.open_streams.lock().unwrap();
            (
                streams
                    .iter()
                    .filter(|(_, s)| s.sender_no == stream_no)
                    .map(|(id, _)| id.clone())
                    .collect(),
                streams
                    .iter()
                    .filter(|(_, s)| s.target_no == stream_no)
                    .map(|(id, _)| id.clone())
                    .collect(),
            )
        };
        for id in sent {
            let msgs = self.end_stream(&id, None, Some("sender disconnected"));
            if !dropped {
                deliver(msgs).await;
            }
        }
        for id in handled {
            let msgs = self.end_stream(
                &id,
                Some(Some(error(
                    &id,
                    proto::ErrorCode::HandlerDisconnected,
                    "handler disconnected",
                ))),
                None,
            );
            if !dropped {
                deliver(msgs).await;
            }
        }
        let msgs = self.subscriber_or_target_closed(stream_no);
        if !dropped {
            deliver(msgs).await;
        }
        if dropped {
            return;
        }
        let lost: Vec<String> = self
            .gathers
            .lock()
            .unwrap()
            .values()
            .flat_map(|g| {
                g.outstanding
                    .iter()
                    .filter(|(_, (n, _, _))| *n == stream_no)
                    .map(|(id, _)| id.clone())
            })
            .collect();
        for id in lost {
            let error = proto::Error {
                request_id: id.clone(),
                code: proto::ErrorCode::HandlerDisconnected as i32,
                message: "handler disconnected".into(),
                details: vec![],
            };
            self.answer(&id, Err(error)).await;
        }
    }
}

#[derive(Clone)]
struct Service(Arc<State>);

#[tonic::async_trait]
impl Hub for Service {
    type ConnectStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

    async fn connect(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let state = self.0.clone();
        if state.down.load(Ordering::SeqCst) {
            return Err(Status::unavailable("hub down (test)"));
        }
        if state.reject_next.swap(false, Ordering::SeqCst) {
            return Err(Status::already_exists(
                "type t.Cmd is owned by other-service",
            ));
        }
        if state.reject_invalid_next.swap(false, Ordering::SeqCst) {
            return Err(Status::invalid_argument("service_name must not be empty"));
        }
        let peer = request
            .peer_certs()
            .and_then(|c| c.first().map(|d| d.to_vec()));
        let mut inbound = request.into_inner();
        let stream_no = state.streams.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(der) = peer {
            state.peers.lock().unwrap().push((stream_no, der));
        }
        let (tx, rx) = mpsc::channel::<Result<ServerMessage, Status>>(64);
        state.clients.lock().await.push((stream_no, tx.clone()));
        let mut drop_rx = state.drop_tx.subscribe();
        let cancel = CancellationToken::new();
        tokio::spawn(async move {
            let mut instance: Option<String> = None;
            let mut principal: Principal = None;
            // Dropped with every other stream (a hub restart): nothing more is delivered.
            let mut dropped = false;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = drop_rx.recv() => {
                        dropped = true;
                        let status = match state.close_with.lock().unwrap().clone() {
                            Some((code, message)) => Status::new(code, message),
                            None => Status::unavailable("dropped by test"),
                        };
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)), if state.stall_reads.load(Ordering::SeqCst) => {}
                    msg = inbound.next(), if !state.stall_reads.load(Ordering::SeqCst) => match msg {
                        None => {
                            // The client half-closed: like a hub that still has things to say.
                            let half_close = state.half_close.lock().unwrap().clone();
                            if let Some((linger, farewell)) = half_close {
                                for msg in farewell() {
                                    let _ = tx.send(Ok(msg)).await;
                                }
                                tokio::time::sleep(linger).await;
                            }
                            break;
                        }
                        Some(Err(_)) => break,
                        Some(Ok(ClientMessage { message: Some(m) })) => match m {
                            client_message::Message::Register(r) => {
                                if let Err(status) = state.admit(stream_no, &r, &mut principal) {
                                    state.rejections.lock().unwrap().push(Rejection {
                                        code: status.code(),
                                        message: status.message().to_string(),
                                        token: credential_text(r.credentials.as_ref()),
                                        at: std::time::Instant::now(),
                                    });
                                    // Like the hub: the stream ends with the status, nothing of the registration stays.
                                    let _ = tx.send(Err(status)).await;
                                    break;
                                }
                                let v2 = r.protocol_version >= 2;
                                let wants_flow = r.capabilities.iter().any(|c| c == "flow-control");
                                let wants_status = r.capabilities.iter().any(|c| c == "instance-status");
                                let wants_scatter = r.capabilities.iter().any(|c| c == "scatter-gather");
                                let wants_streaming = r.capabilities.iter().any(|c| c == "streaming");
                                let wants_subscription = r.capabilities.iter().any(|c| c == "subscription");
                                let wants_refresh = r.capabilities.iter().any(|c| c == "credentials-refresh");
                                state.log(Event::Register { stream: stream_no, types: r.command_types.len() + r.query_types.len() + r.shared_query_types.len() });
                                let scatter = wants_scatter && v2 && state.scatter_gather.load(Ordering::SeqCst) && state.ack.lock().unwrap().is_some();
                                let streaming = wants_streaming && v2 && state.streaming.load(Ordering::SeqCst) && state.ack.lock().unwrap().is_some();
                                let subscription = wants_subscription && v2 && state.subscription.load(Ordering::SeqCst) && state.ack.lock().unwrap().is_some();
                                state.routes.lock().unwrap().insert(stream_no, Route {
                                    service: r.service_name.clone(),
                                    instance: r.instance_id.clone(),
                                    commands: r.command_types.clone(),
                                    queries: r.query_types.iter().chain(&r.shared_query_types).cloned().collect(),
                                    scatter,
                                    streaming,
                                    subscription,
                                    tx: tx.clone(),
                                });
                                instance = Some(r.instance_id.clone());
                                let replaced = state.sessions.lock().unwrap().insert(r.instance_id.clone(), (stream_no, cancel.clone()));
                                if let Some((old, token)) = replaced
                                    && old != stream_no
                                    && state.replace_sessions.load(Ordering::SeqCst)
                                {
                                    state.evictions.fetch_add(1, Ordering::SeqCst);
                                    state.log(Event::Evicted { stream: old });
                                    token.cancel();
                                }
                                let flow = wants_flow && state.flow_control.load(Ordering::SeqCst) && state.ack.lock().unwrap().is_some() && v2;
                                if flow && r.initial_permits > 0 {
                                    // Like the real hub: the first Register of a stream fixes its permits.
                                    state.held.lock().unwrap().entry(stream_no).or_insert((r.initial_permits, r.initial_permits));
                                }
                                state.registers.lock().await.push(r);
                                // Like the real hub: the ack is written before the echo of the next heartbeat.
                                let ack = state.ack.lock().unwrap().clone().map(|mut ack| {
                                    if wants_flow && state.flow_control.load(Ordering::SeqCst) {
                                        ack.capabilities.push("flow-control".to_string());
                                    }
                                    if wants_status && state.instance_status.load(Ordering::SeqCst) {
                                        ack.capabilities.push("instance-status".to_string());
                                    }
                                    if wants_scatter && state.scatter_gather.load(Ordering::SeqCst) {
                                        ack.capabilities.push("scatter-gather".to_string());
                                    }
                                    if wants_streaming && state.streaming.load(Ordering::SeqCst) {
                                        ack.capabilities.push("streaming".to_string());
                                    }
                                    if wants_subscription && state.subscription.load(Ordering::SeqCst) {
                                        ack.capabilities.push("subscription".to_string());
                                    }
                                    if wants_refresh && state.credentials_refresh.load(Ordering::SeqCst) {
                                        ack.capabilities.push("credentials-refresh".to_string());
                                    }
                                    ack
                                });
                                let delay = state.ack_delay_ms.load(Ordering::SeqCst);
                                if delay > 0 {
                                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                                }
                                if let (true, Some(ack)) = (v2, ack) {
                                    let msg = ServerMessage { message: Some(server_message::Message::RegisterAck(ack)) };
                                    if tx.send(Ok(msg)).await.is_err() { break; }
                                }
                            }
                            client_message::Message::Heartbeat(h) => {
                                state.heartbeats.fetch_add(1, Ordering::SeqCst);
                                if state.echo_heartbeats.load(Ordering::SeqCst) {
                                    let echo = ServerMessage { message: Some(server_message::Message::Heartbeat(h)) };
                                    if tx.send(Ok(echo)).await.is_err() { break; }
                                }
                            }
                            client_message::Message::Reply(r) => {
                                state.replies.lock().await.push((stream_no, r.clone()));
                                let result = match r.result {
                                    Some(proto::reply::Result::Error(e)) => Err(e),
                                    Some(proto::reply::Result::Payload(p)) => Ok(p),
                                    None => Ok(b"{}".to_vec()),
                                };
                                state.answer(&r.request_id, result.clone()).await;
                                state.initial_reply(stream_no, &r.request_id, result).await;
                            }
                            client_message::Message::Permits(p) => {
                                state.permits.lock().unwrap().push((stream_no, p.count, std::time::Instant::now()));
                                state.grants.lock().unwrap().push((stream_no, p.grant));
                                if let Some((held, cap)) = state.held.lock().unwrap().get_mut(&stream_no) {
                                    if state.permit_grants.load(Ordering::SeqCst) {
                                        *cap = (*cap).max(p.grant);
                                    }
                                    *held = held.saturating_add(p.count);
                                    if state.permit_cap.load(Ordering::SeqCst) {
                                        *held = (*held).min(*cap);
                                    }
                                }
                            }
                            client_message::Message::Status(st) => state.statuses.lock().unwrap().push((stream_no, st)),
                            client_message::Message::Credentials(c) => {
                                state.refreshes.lock().unwrap().push((stream_no, c.clone()));
                                if let Err(status) = state.admit_refresh(&c, &mut principal) {
                                    state.rejections.lock().unwrap().push(Rejection {
                                        code: status.code(),
                                        message: status.message().to_string(),
                                        token: credential_text(Some(&c)),
                                        at: std::time::Instant::now(),
                                    });
                                    let _ = tx.send(Err(status)).await;
                                    break;
                                }
                            }
                            client_message::Message::Request(req) => {
                                state.requests.lock().await.push(req.clone());
                                if let Some(denied) = state.check_send(&principal, &req) {
                                    if tx.send(Ok(denied)).await.is_err() { break; }
                                    continue;
                                }
                                let scattered = req.mode == proto::RequestMode::ScatterGather as i32
                                    && state.scatter(stream_no, &req, &tx).await;
                                let streamed = req.mode == proto::RequestMode::Streaming as i32
                                    && state.open_stream(stream_no, &req, &tx).await;
                                let subscribed = req.mode == proto::RequestMode::Subscription as i32
                                    && state.open_subscription(stream_no, &req, &tx).await;
                                if !scattered && !streamed && !subscribed {
                                    let response = state.responder.lock().await.as_ref().and_then(|f| f(req));
                                    if let Some(resp) = response && tx.send(Ok(resp)).await.is_err() { break; }
                                }
                            }
                            // Multi-part messages of streaming queries (P4b) and subscriptions (P4c).
                            client_message::Message::Part(p) => {
                                state.parts.lock().unwrap().push((stream_no, p.clone()));
                                let msgs = match state.on_sub_part(stream_no, p.clone()) {
                                    Some(msgs) => msgs,
                                    None => state.on_part(stream_no, p),
                                };
                                deliver(msgs).await
                            }
                            client_message::Message::End(e) => {
                                state.ends.lock().unwrap().push((stream_no, e.clone()));
                                let msgs = match state.on_sub_end(stream_no, e.clone()) {
                                    Some(msgs) => msgs,
                                    None => state.on_end(stream_no, e),
                                };
                                deliver(msgs).await
                            }
                            client_message::Message::Flow(f) => {
                                state.flows.lock().unwrap().push((stream_no, f.clone()));
                                let msgs = match state.on_sub_flow(stream_no, &f) {
                                    Some(msgs) => msgs,
                                    None => state.on_flow(stream_no, f),
                                };
                                deliver(msgs).await
                            }
                            client_message::Message::Cancel(c) => {
                                state.cancels.lock().unwrap().push((stream_no, c.clone()));
                                let msgs = match state.on_sub_cancel(stream_no, &c) {
                                    Some(msgs) => msgs,
                                    None => state.on_cancel(stream_no, c),
                                };
                                deliver(msgs).await
                            }
                        },
                        Some(Ok(ClientMessage { message: None })) => {}
                    }
                }
            }
            if let Some(id) = instance {
                let mut sessions = state.sessions.lock().unwrap();
                if sessions.get(&id).is_some_and(|(s, _)| *s == stream_no) {
                    sessions.remove(&id);
                }
            }
            state.stream_closed(stream_no, &tx, dropped).await;
            state.log(Event::Closed { stream: stream_no });
            let mut clients = state.clients.lock().await;
            clients.retain(|(_, c)| !c.same_channel(&tx));
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn topology(
        &self,
        _: Request<proto::TopologyRequest>,
    ) -> Result<Response<proto::TopologyResponse>, Status> {
        Err(Status::unimplemented("fake hub has no topology"))
    }
}

/// The TLS side of a fake hub.
struct Tls {
    cert: String,
    key: String,
    /// The CA that signs client certificates and whether one is required.
    client_ca: Option<(String, bool)>,
}

impl Tls {
    fn server(cert: &str, key: &str) -> Tls {
        Tls {
            cert: cert.to_string(),
            key: key.to_string(),
            client_ca: None,
        }
    }
}

pub struct FakeHub {
    pub addr: SocketAddr,
    pub state: Arc<State>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl FakeHub {
    pub async fn start() -> FakeHub {
        Self::start_with(None, 0).await
    }

    /// A hub that serves mutual TLS: the client certificate is verified against `client_ca_pem`
    /// (`required`: a client without one fails the handshake; else a client may present none, but
    /// one that does must present a valid one), and the leaf each stream presented is recorded
    /// ([`peer_fingerprints`](Self::peer_fingerprints)).
    pub async fn start_mtls(
        cert_pem: &str,
        key_pem: &str,
        client_ca_pem: &str,
        required: bool,
    ) -> FakeHub {
        Self::start_with_tls(
            Some(Tls {
                cert: cert_pem.to_string(),
                key: key_pem.to_string(),
                client_ca: Some((client_ca_pem.to_string(), required)),
            }),
            0,
        )
        .await
    }

    /// A hub that serves TLS only, with the given certificate and key (PEM): a plain-text client
    /// is dropped at the handshake, like the real hub's TLS listener does.
    pub async fn start_tls(cert_pem: &str, key_pem: &str) -> FakeHub {
        Self::start_with(Some(Tls::server(cert_pem, key_pem)), 0).await
    }

    /// Like [`start_tls`](Self::start_tls), on a given port (a hub that comes up where nothing
    /// listened a moment ago).
    pub async fn start_tls_on(port: u16, cert_pem: &str, key_pem: &str) -> FakeHub {
        Self::start_with(Some(Tls::server(cert_pem, key_pem)), port).await
    }

    async fn start_with(tls: Option<Tls>, port: u16) -> FakeHub {
        Self::start_with_tls(tls, port).await
    }

    async fn start_with_tls(tls: Option<Tls>, port: u16) -> FakeHub {
        if tls.is_some() {
            // tonic's server side builds its rustls config from the process-wide provider.
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
        let (drop_tx, _) = broadcast::channel(8);
        let state = Arc::new(State {
            auth: StdMutex::new(Auth::default()),
            rejections: StdMutex::new(vec![]),
            violations: StdMutex::new(vec![]),
            registers: Mutex::new(vec![]),
            replies: Mutex::new(vec![]),
            requests: Mutex::new(vec![]),
            heartbeats: AtomicUsize::new(0),
            reject_next: AtomicBool::new(false),
            reject_invalid_next: AtomicBool::new(false),
            echo_heartbeats: AtomicBool::new(true),
            drop_tx,
            clients: Mutex::new(vec![]),
            responder: Mutex::new(None),
            ack: StdMutex::new(Some(default_ack())),
            streams: AtomicUsize::new(0),
            flow_control: AtomicBool::new(true),
            permits: StdMutex::new(vec![]),
            grants: StdMutex::new(vec![]),
            held: StdMutex::new(HashMap::new()),
            permit_cap: AtomicBool::new(true),
            permit_grants: AtomicBool::new(true),
            instance_status: AtomicBool::new(true),
            statuses: StdMutex::new(vec![]),
            down: AtomicBool::new(false),
            replace_sessions: AtomicBool::new(true),
            stall_reads: AtomicBool::new(false),
            half_close: StdMutex::new(None),
            sessions: StdMutex::new(HashMap::new()),
            evictions: AtomicUsize::new(0),
            events: StdMutex::new(vec![]),
            scatter_gather: AtomicBool::new(true),
            streaming: AtomicBool::new(true),
            stream_timeouts: AtomicBool::new(true),
            routes: StdMutex::new(HashMap::new()),
            gathers: StdMutex::new(HashMap::new()),
            target_of: StdMutex::new(HashMap::new()),
            open_streams: StdMutex::new(HashMap::new()),
            stream_target_of: StdMutex::new(HashMap::new()),
            rotation: AtomicUsize::new(0),
            subscription: AtomicBool::new(true),
            subscriptions: StdMutex::new(HashMap::new()),
            sub_target_of: StdMutex::new(HashMap::new()),
            sub_rotation: AtomicUsize::new(0),
            subscription_buffer: AtomicUsize::new(256),
            max_subscriptions_per_instance: AtomicUsize::new(1024),
            subscription_max_duration: StdMutex::new(None),
            parts: StdMutex::new(vec![]),
            ends: StdMutex::new(vec![]),
            flows: StdMutex::new(vec![]),
            cancels: StdMutex::new(vec![]),
            refreshes: StdMutex::new(vec![]),
            jwks_loaded: AtomicBool::new(true),
            ack_delay_ms: AtomicU64::new(0),
            jwt_refusal: StdMutex::new(None),
            close_with: StdMutex::new(None),
            credentials_refresh: AtomicBool::new(false),
            peers: StdMutex::new(vec![]),
        });
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let svc = Service(state.clone());
        tokio::spawn(async move {
            let mut builder = tonic::transport::Server::builder();
            if let Some(tls) = tls {
                let mut config = tonic::transport::ServerTlsConfig::new()
                    .identity(tonic::transport::Identity::from_pem(tls.cert, tls.key));
                if let Some((ca, required)) = tls.client_ca {
                    config = config
                        .client_ca_root(tonic::transport::Certificate::from_pem(ca))
                        .client_auth_optional(!required);
                }
                builder = builder.tls_config(config).unwrap();
            }
            builder
                .add_service(HubServer::new(svc))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    shutdown_rx.await.ok();
                })
                .await
                .unwrap();
        });
        FakeHub {
            addr,
            state,
            shutdown: Some(shutdown_tx),
        }
    }

    /// Address in the form services configure: `static://host:port`.
    pub fn address(&self) -> String {
        format!("static://{}", self.addr)
    }

    pub async fn registers(&self) -> Vec<proto::Register> {
        self.state.registers.lock().await.clone()
    }

    pub fn heartbeats(&self) -> usize {
        self.state.heartbeats.load(Ordering::SeqCst)
    }

    pub async fn replies(&self) -> Vec<proto::Reply> {
        self.state
            .replies
            .lock()
            .await
            .iter()
            .map(|(_, r)| r.clone())
            .collect()
    }

    /// Replies with the 1-based number of the stream they arrived on.
    pub async fn replies_by_stream(&self) -> Vec<(usize, proto::Reply)> {
        self.state.replies.lock().await.clone()
    }

    /// Streams accepted so far.
    pub fn streams(&self) -> usize {
        self.state.streams.load(Ordering::SeqCst)
    }

    /// The ack sent for every v2 `Register`; `None` behaves like a hub older than protocol v2.
    pub fn set_ack(&self, ack: Option<proto::RegisterAck>) {
        *self.state.ack.lock().unwrap() = ack;
    }

    /// Whether the ack shares "flow-control" with clients that declare it (`false`: the P0 hub).
    pub fn set_flow_control(&self, on: bool) {
        self.state.flow_control.store(on, Ordering::SeqCst);
    }

    /// A hub whose ack carries `hub_id` (the default ack otherwise).
    pub async fn start_with_id(hub_id: &str) -> FakeHub {
        let hub = FakeHub::start().await;
        hub.set_ack(Some(proto::RegisterAck {
            hub_id: hub_id.to_string(),
            ..default_ack()
        }));
        hub
    }

    /// Whether the ack shares "instance-status" with clients that declare it (`false`: a hub
    /// before P3).
    pub fn set_instance_status(&self, on: bool) {
        self.state.instance_status.store(on, Ordering::SeqCst);
    }

    /// Whether the ack shares "scatter-gather" with clients that declare it (`false`: a P3 hub).
    pub fn set_scatter_gather(&self, on: bool) {
        self.state.scatter_gather.store(on, Ordering::SeqCst);
    }

    /// Whether the ack shares "streaming" with clients that declare it (`false`: a P4a hub).
    pub fn set_streaming(&self, on: bool) {
        self.state.streaming.store(on, Ordering::SeqCst);
    }

    /// Whether the ack shares "subscription" with clients that declare it (`false`: a P4b hub).
    pub fn set_subscription(&self, on: bool) {
        self.state.subscription.store(on, Ordering::SeqCst);
    }

    /// Updates held per subscription while its sender owes credit (default 256, as kraken-hub).
    pub fn set_subscription_buffer(&self, updates: usize) {
        self.state
            .subscription_buffer
            .store(updates, Ordering::SeqCst);
    }

    /// Subscription targets one handler instance serves at once (default 1024, as kraken-hub).
    pub fn set_max_subscriptions_per_instance(&self, max: usize) {
        self.state
            .max_subscriptions_per_instance
            .store(max, Ordering::SeqCst);
    }

    /// `KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION` (default `None`: unlimited) for subscriptions
    /// opened from now on.
    pub fn set_subscription_max_duration(&self, max: Option<Duration>) {
        *self.state.subscription_max_duration.lock().unwrap() = max;
    }

    /// Subscriptions still open.
    pub fn open_subscriptions(&self) -> usize {
        self.state.subscriptions.lock().unwrap().len()
    }

    /// Updates of subscription `id` waiting for credit (or for the initial part); `None` once it
    /// ended.
    pub fn subscription_buffered(&self, id: &str) -> Option<usize> {
        self.state
            .subscriptions
            .lock()
            .unwrap()
            .get(id)
            .map(|s| s.buffered.len())
    }

    /// Update targets still serving, over every open subscription (the hub's
    /// `kraken_hub_subscriptions_open` summed over the instances).
    pub fn subscription_targets_open(&self) -> usize {
        self.state
            .subscriptions
            .lock()
            .unwrap()
            .values()
            .map(|s| s.targets.len())
            .sum()
    }

    /// Scatter-gathers still open.
    pub fn open_gathers(&self) -> usize {
        self.state.gathers.lock().unwrap().len()
    }

    /// Whether idle streams and unanswered initial subscription requests end with `TIMEOUT`
    /// (default `true`, as kraken-hub); `false` is a hub that never ends them, so the client's
    /// local guard is what fires.
    pub fn set_stream_timeouts(&self, on: bool) {
        self.state.stream_timeouts.store(on, Ordering::SeqCst);
    }

    /// Streaming queries still open.
    pub fn open_streams(&self) -> usize {
        self.state.open_streams.lock().unwrap().len()
    }

    /// `ReplyPart`s clients sent (handlers of streaming queries): (1-based stream number, part).
    pub fn parts(&self) -> Vec<(usize, proto::ReplyPart)> {
        self.state.parts.lock().unwrap().clone()
    }

    /// `ReplyEnd`s clients sent: (1-based stream number, end).
    pub fn ends(&self) -> Vec<(usize, proto::ReplyEnd)> {
        self.state.ends.lock().unwrap().clone()
    }

    /// `Flow`s clients sent (senders of streaming queries): (1-based stream number, flow).
    pub fn flows(&self) -> Vec<(usize, proto::Flow)> {
        self.state.flows.lock().unwrap().clone()
    }

    /// `Cancel`s clients sent: (1-based stream number, cancel).
    pub fn cancels(&self) -> Vec<(usize, proto::Cancel)> {
        self.state.cancels.lock().unwrap().clone()
    }

    /// InstanceStatus messages received: (1-based stream number, report).
    pub fn statuses(&self) -> Vec<(usize, proto::InstanceStatus)> {
        self.state.statuses.lock().unwrap().clone()
    }

    /// A down hub refuses every new stream (UNAVAILABLE) and drops the open ones.
    pub fn set_down(&self, down: bool) {
        self.state.down.store(down, Ordering::SeqCst);
        if down {
            self.drop_streams();
        }
    }

    /// Permits messages received: (1-based stream number, count).
    pub fn permits(&self) -> Vec<(usize, u32)> {
        self.state
            .permits
            .lock()
            .unwrap()
            .iter()
            .map(|(s, n, _)| (*s, *n))
            .collect()
    }

    /// The `Permits.grant` of each Permits message received: (1-based stream number, grant).
    pub fn grants(&self) -> Vec<(usize, u32)> {
        self.state.grants.lock().unwrap().clone()
    }

    /// Permits the hub holds for stream `stream` (1-based); `None` unless that stream is
    /// flow-controlled with `initial_permits > 0`.
    pub fn permits_held(&self, stream: usize) -> Option<u32> {
        self.state
            .held
            .lock()
            .unwrap()
            .get(&stream)
            .map(|(held, _)| *held)
    }

    /// Whether permits stop at the stream's cap (default `true`, as kraken-hub).
    pub fn set_permit_cap(&self, on: bool) {
        self.state.permit_cap.store(on, Ordering::SeqCst);
    }

    /// Whether `Permits.grant` raises the cap (default `true`; `false`: a hub that predates it).
    pub fn set_permit_grants(&self, on: bool) {
        self.state.permit_grants.store(on, Ordering::SeqCst);
    }

    /// Permits messages with the instant each arrived.
    pub fn permits_timed(&self) -> Vec<(usize, u32, std::time::Instant)> {
        self.state.permits.lock().unwrap().clone()
    }

    pub async fn requests(&self) -> Vec<proto::Request> {
        self.state.requests.lock().await.clone()
    }

    /// Fails every open stream with UNAVAILABLE, as a hub restart would.
    pub fn drop_streams(&self) {
        *self.state.close_with.lock().unwrap() = None;
        let _ = self.state.drop_tx.send(());
    }

    /// Holds the `RegisterAck` back by `delay` after the `Register` was recorded (a slow hub: the
    /// client's registration is in flight, the handshake not complete).
    pub fn set_ack_delay(&self, delay: Duration) {
        self.state
            .ack_delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
    }

    /// Ends every open stream with `code` and `message` (a hub closing a session: "credentials
    /// expired", ...), as [`drop_streams`](Self::drop_streams) does with UNAVAILABLE.
    pub fn close_streams_with(&self, code: tonic::Code, message: &str) {
        *self.state.close_with.lock().unwrap() = Some((code, message.to_string()));
        let _ = self.state.drop_tx.send(());
    }

    /// While `Some`, every JWT `Register` and refresh is refused with this status (the hub judging
    /// the token); `None` judges them normally again.
    pub fn refuse_jwts_with(&self, refusal: Option<(tonic::Code, &str)>) {
        *self.state.jwt_refusal.lock().unwrap() = refusal.map(|(c, m)| (c, m.to_string()));
    }

    /// Access control (see the module docs): the mode and the token table (token to ACL), replacing
    /// both. Streams already open keep the identity they registered with.
    pub fn set_auth(&self, mode: AuthMode, tokens: &[(&str, Acl)]) {
        let mut auth = self.state.auth.lock().unwrap();
        auth.mode = Some(mode);
        auth.tokens = tokens
            .iter()
            .map(|(t, a)| (t.to_string(), a.clone()))
            .collect();
    }

    /// The `[[service]]` table for JWT credentials, replacing it (see the module docs). Streams
    /// already open keep the identity they registered with.
    pub fn set_services(&self, services: &[Acl]) {
        self.state.auth.lock().unwrap().services = services
            .iter()
            .map(|a| (a.service.clone(), a.clone()))
            .collect();
    }

    /// `false`: a JWT `Register` is answered with `UNAVAILABLE` "jwks not loaded yet".
    pub fn set_jwks_loaded(&self, loaded: bool) {
        self.state.jwks_loaded.store(loaded, Ordering::SeqCst);
    }

    /// Offer the `credentials-refresh` capability to clients that declare it.
    pub fn set_credentials_refresh(&self, on: bool) {
        self.state.credentials_refresh.store(on, Ordering::SeqCst);
    }

    /// The leaf certificate each stream presented in the TLS handshake, as its fingerprint (the
    /// first 8 hex characters of the SHA-256 of its DER), with the stream number; mutual TLS only.
    pub fn peer_fingerprints(&self) -> Vec<(usize, String)> {
        self.state
            .peers
            .lock()
            .unwrap()
            .iter()
            .map(|(stream, der)| (*stream, super::pki::fingerprint_of_der(der)))
            .collect()
    }

    /// The refused `Register`s so far, in order.
    pub fn rejections(&self) -> Vec<Rejection> {
        self.state.rejections.lock().unwrap().clone()
    }

    /// What a permissive hub would have refused.
    pub fn violations(&self) -> Vec<String> {
        self.state.violations.lock().unwrap().clone()
    }

    /// The credential refreshes (`ClientMessage.credentials`) received, with the stream number.
    pub fn refreshes(&self) -> Vec<(usize, proto::Credentials)> {
        self.state.refreshes.lock().unwrap().clone()
    }

    pub fn reject_next_with_already_exists(&self) {
        self.state.reject_next.store(true, Ordering::SeqCst);
    }

    pub fn reject_next_with_invalid_argument(&self) {
        self.state.reject_invalid_next.store(true, Ordering::SeqCst);
    }

    pub fn set_echo_heartbeats(&self, echo: bool) {
        self.state.echo_heartbeats.store(echo, Ordering::SeqCst);
    }

    /// Pushes a server message (typically a Request) to every connected client.
    /// A Request spends one permit of each flow-controlled stream it is pushed to.
    pub async fn send_to_clients(&self, msg: ServerMessage) {
        let request = matches!(msg.message, Some(server_message::Message::Request(_)));
        for (stream, c) in self.state.clients.lock().await.iter() {
            if c.send(Ok(msg.clone())).await.is_ok()
                && request
                && let Some((held, _)) = self.state.held.lock().unwrap().get_mut(stream)
            {
                *held = held.saturating_sub(1);
            }
        }
    }

    pub async fn connected_clients(&self) -> usize {
        self.state.clients.lock().await.len()
    }

    /// How the fake hub answers requests sent by clients (None = swallow).
    pub async fn set_responder(
        &self,
        f: impl Fn(proto::Request) -> Option<ServerMessage> + Send + Sync + 'static,
    ) {
        *self.state.responder.lock().await = Some(Box::new(f));
    }

    /// Whether a `Register` for an instance id held by another open stream ends that stream (the
    /// default, as kraken-hub does) or both streams stay (`false`).
    pub fn set_replace_sessions(&self, on: bool) {
        self.state.replace_sessions.store(on, Ordering::SeqCst);
    }

    /// Stops reading what clients write (`true`): once the HTTP/2 windows are full, a client's
    /// messages stay in its own outbound buffer, never written.
    pub fn set_stall_reads(&self, on: bool) {
        self.state.stall_reads.store(on, Ordering::SeqCst);
    }

    /// When a client half-closes its stream the hub sends `farewell()` at once and keeps its own
    /// side open for `linger` before it ends the stream (a hub still finishing its work).
    pub fn set_half_close(
        &self,
        linger: Duration,
        farewell: impl Fn() -> Vec<ServerMessage> + Send + Sync + 'static,
    ) {
        *self.state.half_close.lock().unwrap() = Some((linger, Arc::new(farewell)));
    }

    /// Sessions replaced by a newer stream of the same instance id so far.
    pub fn evictions(&self) -> usize {
        self.state.evictions.load(Ordering::SeqCst)
    }

    /// True while an open stream holds `instance_id`'s session (the hub routes to it).
    pub fn holds(&self, instance_id: &str) -> bool {
        self.state
            .sessions
            .lock()
            .unwrap()
            .contains_key(instance_id)
    }

    /// Registrations, evictions and stream ends in the order they happened.
    pub fn events(&self) -> Vec<Event> {
        self.events_timed().into_iter().map(|(e, _)| e).collect()
    }

    pub fn events_timed(&self) -> Vec<(Event, std::time::Instant)> {
        self.state.events.lock().unwrap().clone()
    }

    pub fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}
