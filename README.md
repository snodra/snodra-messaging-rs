<p align="center">
  <img alt="Snodra Messaging for Rust mascot" src="docs/assets/brand/svg/snodra-messaging.svg" width="160">
</p>

<h1 align="center">Snodra Messaging (Rust)</h1>

<p align="center">
  Commands, queries, events, sagas and event processors for Rust services, routed by Snodra Server and stored in Kafka, RabbitMQ, PostgreSQL, MySQL or MongoDB.
</p>

<p align="center">
  <a href="https://github.com/snodra/snodra-messaging-rs/actions/workflows/ci.yml"><img alt="ci" src="https://github.com/snodra/snodra-messaging-rs/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://crates.io/crates/snodra-messaging-rs"><img alt="crates.io" src="https://img.shields.io/crates/v/snodra-messaging-rs"></a>
  <img alt="Rust" src="https://img.shields.io/badge/Rust-1.99.1-000000?logo=rust&logoColor=white">
  <img alt="tonic" src="https://img.shields.io/badge/tonic-gRPC-244c5a">
  <a href="https://snodra.damoreira.com/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

---

## What it is

One builder that gives a Rust service everything it needs to take part in a Snodra system, with the
same wire protocol and the same stored records as the Kotlin library — a Kotlin and a Rust service are
indistinguishable in front of the server and on the event store.

- **Commands and queries** through Snodra Server, with deadlines, retries and streaming queries.
- **Events** published through the outbox of a message store, never by a second write.
- **Event processors** that are idempotent by construction: processed-event markers in the same store.
- **Sagas** with associations, deadlines and a relay that retries commands.
- **Stores are add-ons**: Kafka or RabbitMQ Streams for events, MongoDB or SQL (PostgreSQL, MySQL) for state.
- **Conformance kits** for event stores and message stores, and an in-memory everything for tests.

## Install

```toml
[dependencies]
snodra-messaging-rs    = "0.6"
snodra-messaging-kafka = "0.6"   # or snodra-messaging-rabbitmq
snodra-messaging-mongo = "0.6"   # or snodra-messaging-sqlx
```

All five crates are on crates.io; no git dependency and no extra registry.

## A handler

```rust
let app = SnodraMessaging::builder(Config::from_env()?)
    .command::<PlaceOrder, _>(|command, _ctx| async move {
        events.publish(OrderPlaced { order_id: command.order_id }).await
    })
    .start()
    .await?;
```

Settings come from the environment (`SNODRA_*`, and `SNODRA_SERVER_*` for the hub client) with
`Config::from_env()`.

## Documentation

Quick start, the settings map, the add-ons and the conformance kits: **<https://snodra.damoreira.com/messaging/rust/>**.
The Kotlin counterpart is [`snodra-messaging`](https://github.com/snodra/snodra-messaging).

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
