# kraken-messaging-rs Full Participant Implementation Plan (plan 1 of 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the `kraken-messaging-rs` skeleton into a Rust client that is wire- and storage-compatible with the Kotlin starter: hub registration and handling, sending, outbox publishing, idempotent consuming, identity with JWKS validation and handler guards.

**Architecture:** One library crate `kraken_messaging` organised by transport (`hub/`, `kafka/`) plus cross-cutting `identity/`, `types`, `context`, `codec`, `error`, and an `app` builder that wires everything from a `Config`. Hub traffic is one bidirectional tonic stream per instance with a reconnect loop; Kafka events go through a Mongo outbox and a polling publisher, and are consumed with a Mongo transaction per record. Tests: unit tests with an in-process fake hub (tonic server) and `wiremock` JWKS; integration tests with testcontainers against `kraken-hub:dev`, Kafka and Mongo.

**Tech Stack:** Rust 1.99.1, tokio 1.54, tonic 0.14.8 + prost 0.14.4, rdkafka 0.39 (`cmake-build`, `tokio`), mongodb 3.9.1 + bson, serde/serde_json, chrono 0.4.45, jsonwebtoken 11.1.0, reqwest 0.13.5 (rustls), uuid, tracing, thiserror; dev: wiremock 0.6.5, testcontainers 0.28 + testcontainers-modules 0.15 (`kafka`, `mongo`).

**Spec:** `docs/superpowers/specs/2026-10-30-rust-crate-and-service-c-design.md`

## Global Constraints

- Wire names explicit: every message type implements `MessageType { const TYPE: &'static str }` via `message_type!(Type, "wire.Name")`; no prefix-derived names.
- Hub header keys: `authorization`, `correlationId`, `traceparent`, `tracestate`. Kafka header keys: `type`, `id`, `correlationId`, `timestamp`, `source`, `userId`, `traceparent`.
- Timestamps on the wire are RFC 3339 UTC strings; in Mongo they are BSON dates (`bson::DateTime`).
- Mongo collections `outbox` and `processed_events` with the exact field names of the spec; no `_class` field written.
- Defaults: request timeout 5 s, heartbeat 10 s, backoff 500 ms..30 s, 256 concurrent handlers, outbox poll 200 ms, batch 100, 10 send attempts, retention 7 d / 30 d, consumer `max_attempts` 5 with 1 s backoff, `auto.offset.reset=earliest`.
- Error codes mapped exactly as `HubErrors.kt`: NO_HANDLER→`NoHandler`, TIMEOUT→`Timeout`, HANDLER_DISCONNECTED→`HandlerDisconnected`, HANDLER_ERROR→`RemoteHandler`, UNAUTHENTICATED→`Unauthenticated`, INVALID_REQUEST→`InvalidMessage`, PERMISSION_DENIED→`AccessDenied`, else `RemoteHandler`.
- Identity: signed tokens only in unverified mode (reject unsigned/encrypted with `Unauthenticated`), warn once per process; with `jwks_url` verify RS256 + `exp` + `iss`.
- Integration tests: skip with a notice when Docker is unavailable; with Docker but no `kraken-hub:dev` image, fail with `docker build -t kraken-hub:dev . (in ../kraken-hub)` in the message. `KRAKEN_HUB_IMAGE` overrides.
- Never log payloads or tokens.
- `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` must pass at every commit. Commit trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Run in a worktree under `.worktrees/` on branch `feat/full-participant`.

## Review Focus

1. **Hub unreachable at startup**: `KrakenMessaging::start()` returns `Ok` and connects in the background; `send` fails with `HubUnavailable` until the first confirmed registration. Pinned: Task 3 `reconnects_after_server_restart` and Task 7 `send_before_connected_is_hub_unavailable`.
2. **Jackson timestamp variants**: `2026-10-30T00:49:45.227152891Z`, `2026-10-30T00:49:45Z`, `2026-10-30T00:49:45.227+00:00` all decode into `DateTime<Utc>`. Pinned: Task 1 `decodes_jackson_instant_variants`.
3. **Token without `realm_access`**: roles are empty, identity still present. Pinned: Task 2 `roles_empty_when_realm_access_missing`.
4. **Command without a result**: a handler returning `()` replies `{}` and the sender decodes `()` from `{}`. Pinned: Task 4 `unit_reply_is_empty_object` and Task 5 `send_decodes_unit_from_empty_object`.
5. **Handler panic in the consumer**: a panicking event handler is treated as a failed attempt (catch_unwind on the task), the record is retried then skipped, the consumer loop keeps running. Pinned: Task 6 `panicking_handler_is_retried_then_skipped`.

---

### Task 1: Workspace, proto refresh, core types and codec

**Files:**
- Modify: `Cargo.toml` (workspace root + library package; members `[".", "examples/service-c"]` added in plan 2, for now `members = ["."]`)
- Modify: `build.rs` (`build_server(true)`; the generated server is used by tests' fake hub)
- Replace: `proto/hub.proto` with a byte-identical copy of `../kraken/protocol/hub.proto`
- Delete: `src/envelope.rs`, `src/handlers.rs`, `src/hub_client.rs`, `src/kafka.rs`, `src/error.rs` (recreated below)
- Create: `src/lib.rs`, `src/proto.rs`, `src/types.rs`, `src/context.rs`, `src/codec.rs`, `src/error.rs`
- Test: `tests/proto_sync.rs`, unit tests inside `src/codec.rs` and `src/types.rs`

**Interfaces (Produces):**
```rust
pub trait MessageType { const TYPE: &'static str; }
pub trait Command: MessageType + Serialize + DeserializeOwned + Send + Sync + 'static { type Reply: Serialize + DeserializeOwned + Send + 'static; }
pub trait Query:   MessageType + Serialize + DeserializeOwned + Send + Sync + 'static { type Reply: Serialize + DeserializeOwned + Send + 'static; }
pub trait Event:   MessageType + Serialize + DeserializeOwned + Send + Sync + 'static {}
#[macro_export] macro_rules! message_type { ($t:ty, $name:literal) => { impl $crate::MessageType for $t { const TYPE: &'static str = $name; } }; }

#[derive(Clone, Debug)]
pub struct MessageContext { pub message_id: String, pub type_name: String, pub correlation_id: String,
    pub identity: Option<Identity>, pub authorization: Option<String>, pub headers: HashMap<String, String> }
impl MessageContext { pub fn user_id(&self) -> Option<&str>; pub fn roles(&self) -> &[String] /* empty when anonymous */; }
tokio::task_local! { pub static CURRENT: MessageContext }   // in context.rs; `MessageContext::current() -> Option<MessageContext>`

pub mod codec { pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, MessagingError>;
                pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, MessagingError>;
                pub fn is_json_object(bytes: &[u8]) -> bool; }

#[derive(Debug, thiserror::Error)]
pub enum MessagingError {
    NoHandler(String), Timeout(String), HandlerDisconnected(String),
    RemoteHandler { type_name: String, message: String, details: Vec<u8> },
    Unauthenticated(String), AccessDenied(String), InvalidMessage(String), HubUnavailable(String),
    HubRegistrationRejected(String), Serialization(#[from] serde_json::Error), Storage(String), Transport(String),
}
impl MessagingError { pub fn from_hub(type_name: &str, code: i32, message: &str, details: &[u8]) -> Self; pub fn to_reply_code(&self) -> ErrorCode /* for handlers: Unauthenticated→5, AccessDenied→7, InvalidMessage→6, else 4 */ }
```
`Identity` is defined in Task 2; Task 1 declares it as `pub struct Identity { pub user_id: Option<String>, pub roles: Vec<String>, pub verified: bool }` in `src/identity/mod.rs` so `MessageContext` compiles.

- [ ] **Step 1: Dependencies and proto.** `cp ../kraken/protocol/hub.proto proto/hub.proto`. Cargo deps: add `chrono = { version = "0.4.45", features = ["serde"] }`, `mongodb = "3.9.1"`, `bson = { version = "2", features = ["chrono-0_4"] }` (use the version `mongodb` 3.9 re-exports; prefer `mongodb::bson`), `jsonwebtoken = "11.1.0"`, `reqwest = { version = "0.13.5", default-features = false, features = ["json", "rustls-tls"] }`, `tokio-util`, `tracing-subscriber` (dev), `wiremock = "0.6.5"` (dev), `testcontainers = "0.28.0"` + `testcontainers-modules = { version = "0.15.0", features = ["kafka", "mongo"] }` (dev), `rsa`/`rand` (dev, to generate a test key; or ship a fixed PEM pair under `tests/fixtures/`, builder's choice). Remove `dashmap`, `tracing-opentelemetry`, `opentelemetry`, `async-trait` unless used. Edition 2024 stays.

Run: `cargo build 2>&1 | tail -3`
Expected: compiles (tonic generates `kraken.hub.v1` with client and server).

- [ ] **Step 2: Failing tests.** `tests/proto_sync.rs`: `vendored_proto_matches_umbrella` reads `../kraken/protocol/hub.proto`; if missing, `eprintln!("umbrella checkout not found, skipping")` and return; else `assert_eq!(bytes, fs::read("proto/hub.proto"))`. In `src/codec.rs` tests: `decodes_jackson_instant_variants` (the three strings above into a struct `{ at: DateTime<Utc> }`, nanos preserved for the first), `encodes_instant_as_rfc3339_utc` (`2026-10-30T00:49:45.227152891Z` round-trips byte-identically with `serde` default `chrono` format; if chrono emits a different but equivalent string, assert parse-equality and document), `is_json_object_rejects_arrays_and_scalars`. In `src/types.rs` tests: `message_type_macro_sets_const` (`message_type!(Ping, "test.Ping")` → `Ping::TYPE == "test.Ping"`).

Run: `cargo test 2>&1 | tail -5`
Expected: compile errors on missing modules.

- [ ] **Step 3: Implement** the modules per Interfaces. `lib.rs` re-exports: `pub use types::*; pub use context::MessageContext; pub use error::MessagingError; pub use identity::{Identity, Guard}; pub mod proto;` (identity items appear in Task 2; keep `identity/mod.rs` minimal now).

- [ ] **Step 4: Run** `cargo test 2>&1 | tail -5` → all pass; `cargo clippy --all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean.

- [ ] **Step 5: Commit** `feat: core types, codec, errors; proto synced with umbrella`.

---

### Task 2: Identity, guards, JWKS verification

**Files:**
- Create: `src/identity/mod.rs` (Identity, `realm_roles(claims) -> Vec<String>`), `src/identity/guard.rs`, `src/identity/jwks.rs`, `src/identity/resolver.rs`
- Test: `tests/identity_jwks.rs` (wiremock), unit tests in each file

**Interfaces (Produces):**
```rust
pub struct Identity { pub user_id: Option<String>, pub roles: Vec<String>, pub verified: bool }
impl Identity { pub fn has_role(&self, role: &str) -> bool; }

#[derive(Clone)] pub struct Guard(Arc<dyn Fn(&Identity, &MessageContext) -> bool + Send + Sync>);
impl Guard { pub fn authenticated() -> Guard; pub fn role(r: &str) -> Guard; pub fn any_role<I: IntoIterator<Item = S>, S: Into<String>>(rs: I) -> Guard;
             pub fn custom(f: impl Fn(&Identity, &MessageContext) -> bool + Send + Sync + 'static) -> Guard;
             pub fn check(&self, identity: Option<&Identity>, ctx: &MessageContext) -> Result<(), MessagingError> /* None→Unauthenticated("handler requires authentication"), false→AccessDenied("access denied") */ }

pub struct JwksVerifier { /* url, issuer, reqwest client, cache: RwLock<(Instant, JwkSet)> */ }
impl JwksVerifier { pub fn new(jwks_url: String, issuer: String) -> JwksVerifier;
                    pub async fn verify(&self, token: &str) -> Result<serde_json::Map<String, Value>, MessagingError> /* Unauthenticated on any failure; refresh JWKS on unknown kid at most once per 60 s */ }

pub struct IdentityResolver { verifier: Option<JwksVerifier>, warned: AtomicBool }
impl IdentityResolver { pub fn new(verifier: Option<JwksVerifier>) -> Self;
                        pub async fn resolve(&self, authorization: Option<&str>) -> Result<Option<Identity>, MessagingError> }
```
Resolve rules: `None`/blank → `Ok(None)`; not `Bearer <token>` → `Unauthenticated("malformed authorization header")`; verifier present → verified claims, `verified: true`; else decode header+claims without verification (`jsonwebtoken::decode_header` + base64 claims), require `alg` to be a signature algorithm (reject `none`) and exactly 3 segments (a 5-segment JWE → `Unauthenticated("unsupported token")`), `verified: false`, `tracing::warn!` once.

- [ ] **Step 1: Failing tests.** Unit: `roles_empty_when_realm_access_missing`, `realm_roles_read_from_claims`, `guard_role_denies_missing_role`, `guard_anonymous_is_unauthenticated`, `unverified_mode_rejects_alg_none`, `unverified_mode_rejects_five_segment_token`, `absent_header_is_anonymous`, `malformed_header_is_unauthenticated`. `tests/identity_jwks.rs` with wiremock serving `/certs` (a JWKS built from a test RSA key, `kid = "k1"`): `valid_token_is_verified_identity` (sub `u-1`, roles `["kraken-user"]`, `verified == true`), `expired_token_is_unauthenticated`, `wrong_issuer_is_unauthenticated`, `unknown_kid_refreshes_once_then_fails` (wiremock expects exactly 2 calls), `token_signed_by_other_key_is_unauthenticated`.

Run: `cargo test identity 2>&1 | tail -5` → compile errors.

- [ ] **Step 2: Implement** per Interfaces; `jsonwebtoken::Validation::new(Algorithm::RS256)` with `set_issuer(&[issuer])`, `validate_exp = true`; key from `DecodingKey::from_jwk(&jwk)`.

- [ ] **Step 3: Run** `cargo test identity 2>&1 | tail -5` → pass; clippy + fmt clean.

- [ ] **Step 4: Commit** `feat(identity): guards, unverified parsing, JWKS verification`.

---

### Task 3: Hub connection (stream, register, heartbeat, reconnect)

**Files:**
- Create: `src/hub/mod.rs`, `src/hub/connection.rs`
- Test: `tests/support/fake_hub.rs` (tonic `Hub` server: records `Register`s, echoes heartbeats, can be told to drop streams, reject with `ALREADY_EXISTS`, forward a `Request`, and capture `Reply`s), `tests/hub_connection.rs`

**Interfaces (Produces):**
```rust
pub struct HubSettings { pub address: String, pub heartbeat_interval: Duration, pub backoff_min: Duration, pub backoff_max: Duration, pub handshake_timeout: Duration /* 10 s */, pub max_concurrent_handlers: usize }
pub struct HubConnection { /* ... */ }
impl HubConnection {
    pub fn new(settings: HubSettings, service_name: String, instance_id: String, inbound: mpsc::Sender<Inbound>) -> Arc<HubConnection>;
    pub fn set_types(&self, command_types: Vec<String>, query_types: Vec<String>);   // before start; re-announced on every stream
    pub fn start(self: &Arc<Self>);                                                   // spawns the connect loop
    pub async fn await_registered(&self, timeout: Duration) -> bool;                 // true once the heartbeat echo after Register arrives
    pub fn is_connected(&self) -> bool;
    pub async fn send(&self, msg: ClientMessage) -> Result<(), MessagingError>;       // HubUnavailable when no stream
    pub fn last_rejection(&self) -> Option<String>;
    pub async fn shutdown(&self);                                                     // stops the loop, closes the stream
}
pub enum Inbound { Request(proto::Request), Reply(proto::Reply), Error(proto::Error), Disconnected }  // Disconnected emitted on every stream loss
```
Loop semantics as `HubConnection.kt`: connect with `tonic::transport::Endpoint` (strip `static://`), open `connect`, send `Register` then a `Heartbeat`; the echo sets `registered`; heartbeat every `heartbeat_interval`, fail the stream if no echo within 3 intervals; on any failure emit `Inbound::Disconnected`, backoff doubling from `backoff_min` to `backoff_max`, reset after a confirmed registration; `ALREADY_EXISTS` → record `last_rejection`, log error, wait `backoff_max`. `handshake_timeout` bounds the wait for the first echo.

- [ ] **Step 1: Failing tests** (`tests/hub_connection.rs`, using `fake_hub::start().await -> FakeHub { addr, registers(), drop_streams(), reject_next_with_already_exists(), send_request(..), replies() }`): `registers_with_declared_types` (service `svc`, commands `["t.Cmd"]`, queries `["t.Q"]`, `await_registered(5s) == true`), `heartbeats_are_sent_at_interval` (interval 100 ms; ≥ 3 heartbeats within 1 s), `reconnects_after_server_restart` (drop streams → `is_connected()` false → re-registered within 2 s with backoff_min 50 ms; two `Register`s recorded), `already_exists_is_reported_and_retried_at_max_backoff` (`last_rejection()` contains the status message; a third Register arrives after `backoff_max` = 300 ms, not before 200 ms), `send_without_stream_is_hub_unavailable`.

Run: `cargo test --test hub_connection 2>&1 | tail -5` → compile errors.

- [ ] **Step 2: Implement** `connection.rs` and the fake hub.

- [ ] **Step 3: Run** tests → pass; clippy + fmt clean.

- [ ] **Step 4: Commit** `feat(hub): connection with registration, heartbeats and reconnect`.

---

### Task 4: Handler registry and inbound dispatcher

**Files:**
- Create: `src/hub/registry.rs`, `src/hub/dispatcher.rs`
- Test: unit tests in both files (dispatcher tested directly with a `proto::Request`, no network)

**Interfaces (Produces):**
```rust
pub struct HandlerRegistry { /* commands, queries, events keyed by TYPE; guards keyed by TYPE */ }
impl HandlerRegistry {
    pub fn new() -> Self;
    pub fn command<C: Command, F, Fut>(&mut self, f: F) where F: Fn(C, MessageContext) -> Fut + Send + Sync + 'static, Fut: Future<Output = Result<C::Reply, MessagingError>> + Send + 'static;
    pub fn query<Q: Query, F, Fut>(&mut self, f: F)   /* same shape */;
    pub fn event<E: Event, F, Fut>(&mut self, f: F) where F: Fn(E, EventContext) -> Fut + ..., Fut: Future<Output = Result<(), MessagingError>> + Send + 'static;
    pub fn requires<M: MessageType>(&mut self, guard: Guard);          // panics if M is not registered as command/query
    pub fn command_types(&self) -> Vec<String>; pub fn query_types(&self) -> Vec<String>; pub fn event_types(&self) -> Vec<String>;
    pub fn has_event_handler(&self, type_name: &str) -> bool;
    // registering the same TYPE twice panics with "duplicate handler for <TYPE>"
}
pub struct EventContext { pub message: MessageContext, pub session: Arc<tokio::sync::Mutex<mongodb::ClientSession>> }

pub struct Dispatcher { registry: Arc<HandlerRegistry>, resolver: Arc<IdentityResolver>, permits: Arc<Semaphore> }
impl Dispatcher { pub async fn handle(&self, req: proto::Request) -> proto::Reply; }
```
`handle` order: resolve identity from `headers["authorization"]` (failure → `UNAUTHENTICATED` reply); find handler by kind+type (none → `NO_HANDLER`; wrong kind → `INVALID_REQUEST "unsupported request kind"`); guard check before decoding (`UNAUTHENTICATED`/`PERMISSION_DENIED`); decode payload (`INVALID_REQUEST`); build `MessageContext` (`correlation_id = headers["correlationId"] or req.id`); run handler inside `CURRENT.scope(ctx, ...)` under a permit; `Ok(reply)` → `Reply{payload: json}` (unit → `{}`); `Err(e)` → `Reply{error{code: e.to_reply_code(), message: e.to_string(), details: []}}`; a panic → `HANDLER_ERROR "handler panicked"`.

- [ ] **Step 1: Failing tests**: `dispatches_command_and_encodes_reply`, `unit_reply_is_empty_object`, `unknown_type_is_no_handler`, `query_sent_as_command_is_invalid_request`, `undecodable_payload_is_invalid_request`, `guarded_handler_rejects_anonymous_before_decoding` (payload is garbage, still `UNAUTHENTICATED`), `guarded_handler_denies_missing_role` (unverified bearer with roles `[]` → `PERMISSION_DENIED`), `handler_error_maps_to_handler_error_code`, `handler_sees_context_and_current_task_local`, `duplicate_registration_panics`.

Run: `cargo test dispatcher 2>&1 | tail -5` → compile errors.

- [ ] **Step 2: Implement.** - [ ] **Step 3: Run** → pass, clippy, fmt. - [ ] **Step 4: Commit** `feat(hub): handler registry and inbound dispatcher`.

---

### Task 5: Sender (commands and queries)

**Files:**
- Create: `src/hub/sender.rs`
- Test: `tests/hub_sender.rs` (fake hub forwards requests back to the same connection or answers them itself)

**Interfaces (Produces):**
```rust
#[derive(Default, Clone)] pub struct SendOptions { pub routing_key: Option<String>, pub timeout: Option<Duration>, pub authorization: Option<String> /* overrides the task-local */ }
pub struct Sender { conn: Arc<HubConnection>, pending: Mutex<HashMap<String, oneshot::Sender<Result<Vec<u8>, MessagingError>>>>, default_timeout: Duration, forward_jwt: bool }
impl Sender {
    pub async fn send_command<C: Command>(&self, cmd: &C, opts: SendOptions) -> Result<C::Reply, MessagingError>;
    pub async fn send_query<Q: Query>(&self, q: &Q, opts: SendOptions) -> Result<Q::Reply, MessagingError>;
    pub fn on_inbound(&self, msg: Inbound);   // Reply/Error complete the pending oneshot; Disconnected fails all pending with HubUnavailable
}
```
Headers: `correlationId` = `MessageContext::current()` correlation id, else a new UUID; `authorization` = `opts.authorization` or the current context's when `forward_jwt`; `traceparent` when a tracing span carries one (builder's choice of propagation crate, may be left out with a TODO). `timeout_ms` = `opts.timeout or default_timeout`; client-side `tokio::time::timeout` at the same value + 1 s → `Timeout`.

- [ ] **Step 1: Failing tests**: `send_command_round_trips_reply` (fake hub replies `{"echoed":"x"}`), `hub_error_no_handler_maps_to_no_handler`, `hub_error_permission_denied_maps_to_access_denied`, `send_decodes_unit_from_empty_object`, `request_carries_correlation_and_forwarded_authorization` (run inside `CURRENT.scope(ctx with authorization "Bearer t")`), `disconnect_fails_pending_with_hub_unavailable`, `no_reply_times_out` (timeout 200 ms).

- [ ] **Step 2: Implement.** - [ ] **Step 3: Run** → pass, clippy, fmt. - [ ] **Step 4: Commit** `feat(hub): command and query sender`.

---

### Task 6: Kafka events: outbox, publisher, consumer

**Files:**
- Create: `src/kafka/mod.rs`, `src/kafka/headers.rs`, `src/kafka/outbox.rs`, `src/kafka/publisher.rs`, `src/kafka/consumer.rs`, `src/kafka/indexes.rs`
- Test: unit tests in `outbox.rs` (document shape) and `consumer.rs` (record classification, pure functions); `tests/kafka_integration.rs` (testcontainers Kafka + Mongo replica set)

**Interfaces (Produces):**
```rust
pub struct OutboxDocument { pub id: String /* _id */, pub type_name: String /* "type" */, pub key: Option<String>, pub headers: HashMap<String,String>,
    pub payload: bson::Document, pub created_at: bson::DateTime, pub seq: i64, pub sent_at: Option<bson::DateTime>, pub attempts: i32, pub failed_at: Option<bson::DateTime> }  // serde renames to the Kotlin field names
pub struct EventBus { db: mongodb::Database, seq: AtomicI64 /* seeded now_ms << 20 */ }
impl EventBus { pub async fn publish<E: Event>(&self, e: &E, key: Option<&str>) -> Result<(), MessagingError>;
                pub async fn publish_in<E: Event>(&self, e: &E, key: Option<&str>, session: &mut ClientSession) -> Result<(), MessagingError>;
                pub fn document<E: Event>(&self, e: &E, key: Option<&str>, ctx: Option<&MessageContext>) -> Result<OutboxDocument, MessagingError>; }
pub struct OutboxPublisher { /* db, FutureProducer, topic, source, poll, batch, max_attempts */ }
impl OutboxPublisher { pub fn start(self: Arc<Self>) -> JoinHandle<()>; pub async fn relay_once(&self) -> Result<usize, MessagingError>; }
pub struct EventConsumer { /* StreamConsumer, db, registry, max_attempts, backoff */ }
impl EventConsumer { pub fn start(self: Arc<Self>) -> JoinHandle<()>; }
pub enum RecordDisposition { Skip(&'static str), Handle { type_name: String, id: String } }
pub fn classify(headers: &HashMap<String,String>, payload: &[u8], topic: &str, partition: i32, offset: i64, registry: &HandlerRegistry) -> RecordDisposition;
pub async fn ensure_indexes(db: &Database, outbox_retention: Duration, idempotency_retention: Duration) -> Result<(), MessagingError>;
```
Consumer per record: `classify`; `Handle` → start session + transaction; build `MessageContext` (`message_id = id`, `correlation_id = headers.correlationId or id`, identity `None`, `user_id` from header when non-empty exposed via `headers`); run handler in `catch_unwind`-safe task with `EventContext`; insert `processed_events {_id, type, processedAt}` in the session; commit; commit offset. Duplicate key on the marker → abort, commit offset, debug log. Handler error/panic → abort; attempt counter per `(partition, offset)`; sleep `backoff` and re-handle; after `max_attempts` warn (type, id, topic-partition@offset, no payload) and commit offset.

- [ ] **Step 1: Failing unit tests** (`outbox.rs`): `document_matches_kotlin_shape` (bson has keys `_id,type,key,headers,payload,createdAt,seq,sentAt,attempts,failedAt`, `createdAt` is `Bson::DateTime`, `payload` is `Bson::Document`, `headers.userId == ""` when anonymous, `seq` increases), `document_takes_correlation_and_user_from_context`. (`consumer.rs`): `classify_skips_missing_type`, `classify_skips_unhandled_type`, `classify_skips_non_object_payload`, `classify_uses_topic_partition_offset_when_id_missing`, `classify_handles_known_type`.

- [ ] **Step 2: Failing integration tests** (`tests/kafka_integration.rs`, `#[ignore]`-free but guarded by `support::docker_available()`): `outbox_relays_to_kafka_with_kotlin_headers` (publish one event, `relay_once` → 1; consume with a raw rdkafka consumer and assert headers `type,id,correlationId,timestamp,source,userId`, key, JSON value, and `sentAt` set), `consumer_handles_once_and_dedupes` (produce the same record twice with the same `id` header → handler called once, `processed_events` has one doc), `panicking_handler_is_retried_then_skipped` (max_attempts 2, backoff 50 ms → handler invoked twice, loop continues: a following good record is handled), `failed_send_marks_attempts` (unit-level with an unreachable broker and `message.timeout.ms=500`: `attempts` incremented, document unsent).

- [ ] **Step 3: Implement.** - [ ] **Step 4: Run** `cargo test kafka 2>&1 | tail -8` → pass (integration ones run here; Docker is available). clippy, fmt. - [ ] **Step 5: Commit** `feat(kafka): outbox, publisher and idempotent consumer`.

---

### Task 7: `KrakenMessaging` app, config, end-to-end against the real hub

**Files:**
- Create: `src/config.rs`, `src/app.rs`; `tests/support/containers.rs` (hub image check, Kafka, Mongo repl set), `tests/e2e_hub.rs`
- Modify: `README.md` (API, config table, compatibility statement, test instructions), `Dockerfile` (library CI image: keep, add cmake/protoc already present), `.gitlab-ci.yml` (unchanged shape)

**Interfaces (Produces):**
```rust
pub struct Config { pub service_name: String, pub hub_address: String, pub hub_request_timeout: Duration, pub heartbeat_interval: Duration,
    pub reconnect_backoff_min: Duration, pub reconnect_backoff_max: Duration, pub max_concurrent_handlers: usize,
    pub kafka_bootstrap: String, pub kafka_topic: String, pub consumer_enabled: bool, pub consumer_max_attempts: u32,
    pub mongodb_uri: String, pub outbox_enabled: bool, pub outbox_poll_interval: Duration, pub outbox_batch_size: usize, pub outbox_max_attempts: u32,
    pub outbox_retention: Duration, pub idempotency_retention: Duration, pub jwks_url: Option<String>, pub issuer: Option<String>, pub forward_jwt: bool }
impl Config { pub fn from_env() -> Result<Config, MessagingError> /* KRAKEN_SERVICE_NAME (required), KRAKEN_HUB_ADDRESS, KRAKEN_KAFKA_BOOTSTRAP, KRAKEN_MONGODB_URI (required), KRAKEN_JWKS_URL, KRAKEN_ISSUER, others optional with defaults */ ; pub fn new(service_name: &str, hub_address: &str, kafka_bootstrap: &str, mongodb_uri: &str) -> Config }
pub struct KrakenMessagingBuilder { .. }
impl KrakenMessaging {
    pub fn builder(config: Config) -> KrakenMessagingBuilder;
}
impl KrakenMessagingBuilder {
    pub fn command<C: Command, F, Fut>(self, f: F) -> Self; pub fn query<Q: Query, F, Fut>(self, f: F) -> Self; pub fn event<E: Event, F, Fut>(self, f: F) -> Self;
    pub fn requires<M: MessageType>(self, guard: Guard) -> Self;
    pub async fn start(self) -> Result<KrakenMessaging, MessagingError>;   // connects Mongo, ensures indexes, starts hub loop, publisher, consumer; does NOT wait for the hub
}
impl KrakenMessaging { pub fn commands(&self) -> &Sender; pub fn queries(&self) -> &Sender; pub fn events(&self) -> &EventBus;
                       pub async fn await_hub(&self, timeout: Duration) -> bool; pub async fn shutdown(self); }
```

- [ ] **Step 1: Failing tests** (`tests/e2e_hub.rs`; containers: `kraken-hub:dev` via `GenericImage` with exposed port 7000 and wait for stdout `hub listening`, Kafka, Mongo repl set): `two_participants_round_trip` (A sends `t.Ping{"text"}` to B, B replies, B publishes `t.Happened` keyed by text, A's event handler receives it within 15 s), `no_handler_is_reported` (type nobody registered → `NoHandler`), `permission_denied_with_signed_token` (B requires `Guard::role("kraken-admin")`; A sends with a wiremock-verified token lacking the role → `AccessDenied`; with the role → `Ok`), `send_before_connected_is_hub_unavailable` (address of a closed port; `start()` ok; immediate send → `HubUnavailable`), `reconnects_after_hub_restart` (`container.stop()`/`start()`; `await_hub(30 s)` true; round trip succeeds again). `tests/support/containers.rs::hub_image_or_fail()` inspects the image and panics with the build command if missing.

- [ ] **Step 2: Implement `config.rs`, `app.rs`**, README.

- [ ] **Step 3: Run** `cargo test 2>&1 | tail -8` (full suite incl. integration) → pass; `cargo clippy --all-targets -- -D warnings`; `cargo fmt --all -- --check`.

- [ ] **Step 4: Commit** `feat: KrakenMessaging builder, config, end-to-end tests against kraken-hub`.
