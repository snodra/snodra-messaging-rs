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

- **Commands and queries** through Snodra Server, with deadlines, retries and streaming queries. A command or unary
  query of a type the service handles itself (a query not registered as shared) is answered in-process by its own
  handler, without the hub, as the Kotlin library does; `SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB=true`
  (`Config::hub_always_route_through_hub`, or the builder's `always_route_through_hub(true)`) sends it through the hub
  like any other request. Before `start()`, an idempotent command type of this service is not answered in-process: it
  goes through the hub like another service's command (its call records are bound by `start()`), so with no hub yet it
  fails as never sent with `HubUnavailable` at its deadline and its handler does not run.
- **Events** published through the outbox of a message store, never by a second write.
- **Event processors** that are idempotent by construction: processed-event markers in the same store.
- **Sagas** with associations, deadlines and a relay that retries commands.
- **Stores are add-ons**: Kafka or RabbitMQ Streams for events, MongoDB or SQL (PostgreSQL, MySQL) for state.
- **Conformance kits** for event stores and message stores, and an in-memory everything for tests.

## Install

```toml
[dependencies]
snodra-messaging-rs    = "0.7"
snodra-messaging-kafka = "0.7"   # or snodra-messaging-rabbitmq
snodra-messaging-mongo = "0.7"   # or snodra-messaging-sqlx
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

## Releases

Versions are cut by CI from the commit types. Never push a tag. When the checks pass on a push to
`main`, the subjects since the last `v*` tag decide. Any `feat` is the next minor. Otherwise any `fix`
or `perf` is the next patch. Anything else (`docs`, `test`, `chore`, `ci`, `refactor`, `build`,
`style`) releases nothing.

All five crates share one version: `workspace.package.version` in `Cargo.toml`. CI checks that every
crate packages at the new version. Then it commits `release: X.Y.Z` (`Cargo.toml`, `Cargo.lock` and
the install lines of this README) and tags `vX.Y.Z`. The release workflow builds the tagged crates
once more, publishes them to crates.io in dependency order (core first), and then creates the GitHub
release. Pull before you push: `main` then carries that commit.

- Below 1.0.0, any `type!:` subject and any line starting `BREAKING CHANGE:` in a message count as a
  minor, never a major. 1.0.0 is declared by hand.
- A revert (`Revert "feat: ..."`, `revert:`) is not a release reason. The reverted change stays in the
  last published version until the next `feat`, `fix` or `perf`.
- Only tags shaped `vN.N.N` count. The highest one reachable from `main` is the last release.
- `scripts/next-version.sh` holds the rule. `for t in scripts/*.test.sh; do sh "$t"; done` tests it.

If a release fails halfway (the tag exists, some crates are not published), use **Re-run failed jobs**
on that run. Every step is safe to repeat: a crate already on crates.io is skipped. Or run the
`release` workflow by hand with the existing tag. **Re-run all jobs** on a run that released does
nothing, because `main` has moved to its release commit.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
