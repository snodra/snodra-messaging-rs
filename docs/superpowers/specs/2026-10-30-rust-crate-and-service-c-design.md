# kraken-messaging-rs as a full participant, and service C

**Date:** 2026-10-30
**Status:** draft, awaiting approval
**Repos touched:** `kraken-messaging-rs` (the crate, `examples/service-c`), `kraken-messaging` (examples api, service-a, service-b), `kraken` umbrella (`infra/compose.local.yaml`, docs, tech stack)

## Intent

Guilherme wants a third example service written in Rust, talking to the two Spring Boot examples through the
same hub and the same Kafka topic: A sends a command and a query to C, C publishes an event while handling each,
and A and B log those events. The real reason to build it is the Rust client crate: `kraken-messaging-rs` is
today a 480-line skeleton with stubs, and the planned `files-service` needs the complete thing. So this project
turns the crate into a **full participant**, wire- and storage-compatible with the Kotlin starter, and proves it
with service C in the running sandbox.

## Goals

- A Rust service and a Kotlin service are indistinguishable from the hub's and Kafka's point of view: same
  protocol, same record format, same JSON shapes, same error codes, same Mongo collections.
- The crate offers every capability of the Kotlin starter the services use: register and handle commands and
  queries, send commands and queries, publish events through an outbox, consume events idempotently, forward
  and validate identity, opt-in handler authorization, reconnect on its own.
- Service C runs in the sandbox next to A and B from a local build; `smoke.sh` covers the new legs.
- Unit tests without Docker for all logic; integration tests against the real `kraken-hub:dev` image, Kafka
  and MongoDB via testcontainers, mirroring the Kotlin E2E.

## Non-goals

- Scatter-gather or subscription queries; the hub does not have them either.
- Outbox leader election across instances (the Kotlin starter does not have it: at-least-once per instance).
- A dead-letter topic; poison records are logged and skipped after `max_attempts`, like Kotlin.
- Publishing the crate to a registry; it stays a git/path dependency.
- HTTP endpoints or health probes in service C.
- Tracing export configuration inside the crate: it emits `tracing` spans and propagates `traceparent`; wiring
  an OTLP exporter is the application's job.

## Anti-goals

- No Rust-specific protocol or schema deviation, however convenient. If the Kotlin shape is awkward, note it
  as a finding for both sides; do not fork the contract.
- No second copy of `hub.proto` drifting again: the crate consumes the umbrella file.
- Service C must not become a place where the crate's gaps are patched around; the crate's public API is all
  it may use.

## Decisions taken with Guilherme

| Area | Decision |
|---|---|
| Crate scope | Full participant: send and handle commands/queries, publish and consume events, identity. |
| Kafka client | `rdkafka` 0.39 (librdkafka, `cmake-build`), as documented in the tech stack. `brew install cmake` on this Mac. |
| Durability | Mongo outbox + `processed_events` idempotency now, same collections and fields as the Kotlin starter. |
| Identity | Full: JWKS validation (RS256, issuer, expiry) when configured, unverified parsing otherwise, per-handler guards. |
| Location | `kraken-messaging-rs` becomes a Cargo workspace: the library plus `examples/service-c`. |
| Domain | Command `Shout{text}` → reply `Shouted{text}` (upper-cased) + event `TextShouted{text, at}`; query `Reverse{text}` → reply `Reversed{text}` + event `TextReversed{text, at}`. C also logs `GreetingDelivered`. |
| Kotlin side | A: `POST /shouts`, `GET /reverse/{text}`; A and B log `TextShouted` and `TextReversed`. |

## Wire and storage contracts (what "compatible" means, verified against the Kotlin code)

**Hub stream.** `Register{service_name, instance_id, command_types[], query_types[]}` first on every stream,
followed by a `Heartbeat{sent_at_ms}`; the echoed heartbeat confirms the registration. Heartbeat every 10 s.
Registration rejected with gRPC `ALREADY_EXISTS` (type owned by another service, or kind conflict) or
`INVALID_ARGUMENT`; the stream then fails and the client retries at the backoff ceiling, logging the reason.
Reconnect on any stream failure with exponential backoff 500 ms → 30 s, reset after a confirmed registration.
Outgoing `Request{id (UUID), kind, type, routing_key, payload (JSON), headers, timeout_ms}` with headers
`correlationId` always, `authorization: Bearer <jwt>` when the caller has one, `traceparent`/`tracestate` when a
span is active. Default request timeout 5 s. Incoming requests run at most 256 at a time; outcomes become a
`Reply{payload}` or `Reply{error{code, message, details}}` with codes `NO_HANDLER` (type not registered locally),
`INVALID_REQUEST` (undecodable payload, unsupported kind), `UNAUTHENTICATED`, `PERMISSION_DENIED`, `HANDLER_ERROR`
(handler returned an error; message is the error's display, details empty unless the handler supplies bytes).
Hub-produced errors map to typed Rust errors: `NoHandler`, `Timeout`, `HandlerDisconnected`, `RemoteHandler`,
`Unauthenticated`, `AccessDenied`, `InvalidMessage`, `HubUnavailable`.

**Kafka records.** Topic `kraken.events`; key = routing key (nullable); value = JSON object; headers `type`,
`id` (UUID, dedupe key), `correlationId`, `timestamp` (RFC 3339 of `createdAt`), `source` (service name),
`userId` (empty string when anonymous, as Kotlin writes), `traceparent` when present. Consumer group =
service name, `auto.offset.reset=earliest`, manual commits after the handler transaction. Records with no
`type`, with a type nobody handles, or with a non-object payload are committed and skipped (warn, never the
payload). Dedupe: insert `{_id: id, type, processedAt}` into `processed_events` in the same Mongo transaction as
the handler; duplicate key on that insert → committed and skipped; handler error → transaction aborted and the
record retried with 1 s backoff up to `max_attempts` (5), then logged and skipped. Missing `id` header →
`"<topic>-<partition>-<offset>"` as the dedupe id, with a warning.

**Outbox.** Collection `outbox`, documents `{_id (UUID), type, key, headers{correlationId, userId, traceparent?},
payload (BSON object of the event JSON), createdAt, seq (per-process counter seeded with now_ms << 20), sentAt,
attempts, failedAt}`. Publisher polls every 200 ms, batch 100, `sentAt == null && failedAt == null` ordered by
`createdAt, seq`, sends sequentially; a retriable failure increments `attempts` and ends the batch; after 10
attempts sets `failedAt` and moves on. Indexes: `(sentAt, createdAt)`, TTL on `sentAt` (7 d), TTL on
`processedAt` (30 d). `publish()` inserts into the outbox inside the caller's Mongo session when one is passed,
else in its own write.

**JSON shapes.** Field names identical to the Kotlin data classes; `Instant` as RFC 3339 with nanoseconds
(`2026-10-30T00:49:45.227152891Z`), `chrono::DateTime<Utc>` with serde. Replies to commands without a result:
`{}`.

**Identity.** From `headers["authorization"]`: with a configured `jwks_url` (and `issuer`), verify RS256
signature against the cached JWKS (refresh on unknown `kid`, at most once per minute), `exp`, `iss`; failure →
`UNAUTHENTICATED`. Without a `jwks_url`: parse the signed token's claims unverified (`verified = false`), reject
unsigned or encrypted tokens, warn once per process. Identity = `{user_id: sub, roles: realm_access.roles,
verified}`. Absent header → anonymous, request proceeds. A handler registered with `.requires(guard)` gets
`UNAUTHENTICATED` when anonymous, `PERMISSION_DENIED` when the guard returns false, before payload decoding.
Guards: `Guard::authenticated()`, `Guard::role("kraken-user")`, `Guard::any_role([...])`,
`Guard::custom(|identity, ctx| bool)`. Outgoing requests forward the current `authorization` from the
`MessageContext` the handler received (task-local), so chains re-forward the same token.

## Crate design (`kraken_messaging`)

```
src/
  lib.rs            re-exports, prelude
  proto.rs          tonic::include_proto!("kraken.hub.v1"), built from ../kraken/protocol/hub.proto via build.rs
  types.rs          traits MessageType { const TYPE: &str }, Command { type Reply }, Query { type Reply }, Event
  context.rs        MessageContext { message_id, type_name, correlation_id, identity: Option<Identity>, authorization, headers }
  error.rs          MessagingError enum (variants above) + ErrorCode mapping
  codec.rs          serde_json encode/decode helpers, object check
  identity/         identity.rs (Identity, realm roles), jwks.rs (fetch + cache + verify), guard.rs (Guard), resolver.rs
  hub/              connection.rs (stream, register, heartbeat, reconnect, permits), sender.rs (CommandBus/QueryBus),
                    dispatcher.rs (incoming Request → handler → Reply), registry.rs (typed handler registration)
  kafka/            outbox.rs (publish + documents), publisher.rs (poll loop), consumer.rs (loop, dedupe, retries),
                    headers.rs (constants), indexes.rs
  app.rs            KrakenMessaging builder: config → connected hub + publisher + consumer; graceful shutdown
```

Public API sketch (the plan fixes exact signatures):

```rust
let app = KrakenMessaging::builder(Config::from_env()?)          // KRAKEN_* variables, same names as compose
    .command::<Shout, _, _>(|cmd, ctx| async move { ... Ok(Shouted { .. }) })
    .query::<Reverse, _, _>(|q, ctx| async move { ... })
    .event::<GreetingDelivered, _, _>(|e, ctx| async move { ... })
    .requires_for::<Shout>(Guard::role("kraken-user"))            // optional
    .start().await?;                                              // registers, starts publisher + consumer
app.events().publish(&TextShouted { .. }, Some(&key)).await?;    // outbox insert
let reply: Shouted = app.commands().send(&Shout { .. }, SendOptions::default()).await?;
app.shutdown().await;                                            // deregister, drain, stop loops
```

Configuration (`Config`, all overridable by env with the sandbox's names): `service_name`, `type_prefix`
(default = service name, used for types without an explicit name), `hub_address` (`static://` prefix accepted),
`hub_request_timeout` 5 s, `heartbeat_interval` 10 s, `reconnect_backoff` 500 ms..30 s, `max_concurrent_handlers`
256, `kafka_bootstrap`, `kafka_topic` `kraken.events`, `consumer_enabled`, `consumer_max_attempts` 5,
`mongodb_uri`, `outbox_enabled`, `outbox_poll_interval` 200 ms, `outbox_batch_size` 100, `outbox_max_attempts` 10,
`outbox_retention` 7 d, `idempotency_retention` 30 d, `jwks_url`, `issuer`, `forward_jwt` true.

## Service C (`examples/service-c`)

Binary crate, no HTTP. Handlers log one `INFO` line each, same style as A and B:
`C: handled Shout text=<t> -> <UPPER> correlationId=<id>`, `C: answered Reverse text=<t> -> <reversed>`,
`C: received GreetingDelivered id=<id> name=<name>`. Publishes `TextShouted{text: UPPER, at}` keyed by the
original text and `TextReversed{text: reversed, at}` keyed likewise, through the outbox. Reads `KRAKEN_SERVICE_NAME`
(`service-c`), `KRAKEN_HUB_ADDRESS`, `KRAKEN_KAFKA_BOOTSTRAP`, `KRAKEN_MONGODB_URI`; `RUST_LOG` default `info`.
`examples/service-c/Dockerfile`: `rust:1.99-bookworm` builder with cmake + protoc, workspace build with cargo
cache mounts; runtime `debian:bookworm-slim` with `ca-certificates` (librdkafka links OpenSSL dynamically from
the builder image's libssl3, which bookworm-slim provides).

## Kotlin side

- `examples-api`: `Shout(text)`: `Command<Shouted>`, `Shouted(text)`, `Reverse(text)`: `Query<Reversed>`,
  `Reversed(text)`, `TextShouted(text, at)`: `Event`, `TextReversed(text, at)`: `Event`; wire names
  `examples.Shout`, `examples.Reverse`, `examples.TextShouted`, `examples.TextReversed`.
- service-a: `POST /shouts {"text": "hi"}` → `Shouted`, `GET /reverse/{text}` → `Reversed`, blank text → 400;
  `GreetingsListener` gains `on(TextShouted)` and `on(TextReversed)` (log + keep, `GET /texts/delivered`).
- service-b: new `TextEventsListener` logging both events.
- `smoke.sh`: after the greetings leg, `POST /shouts {"text":"ada"}` expects `{"text":"ADA"}`, `GET /reverse/ada`
  expects `{"text":"ada"}`, then waits until `GET /texts/delivered` holds both; topology must list `service-c`
  owning `examples.Shout` and `examples.Reverse`; B's and C's logs are not asserted (documented in README).

## Compose and docs

- `compose.local.yaml`: `service-c` built from `${KRAKEN_MESSAGING_RS_DIR:-../../kraken-messaging-rs}` with
  `examples/service-c/Dockerfile`, env as above, Mongo database `service-c`, `depends_on` hub, mongodb,
  kafka-init; no published port.
- Umbrella `docs/03-tech-stack.md`: add `jsonwebtoken`, `chrono`, `reqwest`, `wiremock`, `mongodb` 3.9.1 rows;
  `docs/02-messaging.md` Rust section rewritten against the real API; `docs/07-local-development.md` sandbox
  section mentions C; `kraken-messaging/examples/README.md` gains the C legs; crate `README.md` documents the API.

## Verification the builder must deliver

- Unit tests (no Docker): codec round trips including `Instant` shapes produced by Jackson; registry type names;
  dispatcher outcomes for each error code; guard decisions; JWKS verify with a generated RSA key served by
  `wiremock` (valid, expired, wrong issuer, unknown kid, unsigned token); outbox document construction; consumer
  record classification (no type, unknown type, non-object, missing id).
- Integration tests (testcontainers, `kraken-hub:dev`, `apache/kafka:4.4.0`, `mongo:8.3.13` replica set): two
  Rust participants through the hub (send and handle, NO_HANDLER, PERMISSION_DENIED with a signed token);
  publish through the outbox and consume with dedupe (same event id twice → handler once); reconnect after the
  hub container restarts. Skipped automatically when Docker is absent, run in the full suite here.
- Cross-language proof: the sandbox smoke with A, B and C.

## Builder calls (my decisions, flagged)

- Proto source: `build.rs` compiles `../kraken/protocol/hub.proto` when the sibling exists, else the vendored
  `proto/hub.proto`; a test asserts the two are byte-identical when both exist, so drift fails the build.
- Typed registration uses `const TYPE: &str` on a `MessageType` trait plus a `#[derive]`-free helper macro
  `message_type!(Shout, "examples.Shout")`; no proc-macro crate for now.
- Task-local `MessageContext` for forwarding `authorization` and `correlationId` from an incoming handler to
  outgoing sends, mirroring the Kotlin coroutine context.
- JWKS cache refresh at most once per minute; unknown `kid` triggers one refresh then fails.
- Mongo sessions: `publish()` accepts an optional `&mut ClientSession`; the consumer runs handler + marker in a
  transaction and hands the session to the handler through the context so handler writes join it.
- Versions: `jsonwebtoken` 11.1.0, `chrono` 0.4.45, `reqwest` 0.13.5 (rustls), `wiremock` 0.6.5, `mongodb` 3.9.1,
  `testcontainers-modules` 0.15.0 (`kafka`, `mongo`); existing pins kept (tonic 0.14.8, prost 0.14.4,
  rdkafka 0.39.0, tokio 1.54.1).
- Two implementation plans: (1) the crate with its tests, (2) service C + Kotlin changes + compose + docs.

## Left to the builder

- Exact Rust signatures and error-conversion details.
- How the consumer runs handlers concurrently (one task per partition is the suggested shape).
- Workspace layout details (`[workspace] members = [".", "examples/service-c"]`).
