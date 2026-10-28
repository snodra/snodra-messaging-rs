# kraken-messaging-rs

Rust client for the kraken messaging layer: the Axon-style `send` and handler shape over the
gRPC hub (commands, queries) and Kafka (events). The Kotlin twin is `kraken-messaging`.

Status: **skeleton, no business logic.** Traits, the handler registry, the envelope, error types
and configuration exist; `HubClient` and the Kafka transport return `NotImplemented`.

Design: [`../kraken/docs/02-messaging.md`](../kraken/docs/02-messaging.md). Protocol:
`proto/hub.proto`, a copy of `../kraken/protocol/hub.proto` (see `proto/README.md`).

## Use

```toml
[dependencies]
kraken-messaging-rs = { path = "../kraken-messaging-rs" }
# later: { git = "https://gitlab.damoreira.ml/kraken/kraken-messaging-rs.git", tag = "v0.1.0" }
```

```rust
use kraken_messaging::{HandlerRegistry, HubClient, HubConfig, MessageType, Query};

let registry = HandlerRegistry::new("files-service")
    .query::<GetFileMetadata, _>(|q, _ctx| async move { Ok(find(q.file_id).await) });
let hub = HubClient::connect(HubConfig::from_env(), registry).await?;
```

## Build

Requires Rust 1.99.1, `protoc`, and `cmake` (rdkafka vendors librdkafka with `cmake-build`).

```sh
cargo build
cargo test
```

## Layout

```
src/lib.rs         re-exports, crate docs
src/envelope.rs    Envelope + header name constants
src/handlers.rs    MessageType / Event / Command / Query traits, HandlerRegistry builder
src/hub_client.rs  HubClient, HubConfig, SendOptions
src/kafka.rs       EventPublisher, EventConsumer, KafkaConfig
src/error.rs       MessagingError
proto/hub.proto    synced from the umbrella repo
examples/          placeholder
```

> **Builder call:** written without a local Rust toolchain; not compiled yet. Expect small fixes on
> the first build (trait bounds on the erased handlers, rdkafka feature flags).

> **Builder call:** no `Cargo.lock` committed, as is conventional for library crates.
