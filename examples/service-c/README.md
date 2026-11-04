# service-c

The Rust participant of the examples sandbox (see `kraken-messaging/examples/README.md` for the whole
picture and the curl walkthrough). Handles `examples.Shout` and `examples.Reverse`, publishes
`examples.TextShouted` / `examples.TextReversed` through its outbox, logs `examples.GreetingDelivered`.
Shares the query `examples.Inventory` with service-b (`shared_query`, ADR 0017): it answers with its
service name, its host name (the container id under Docker), a fixed count of 3 and how many
Inventory queries it answered. Shares the streaming query `examples.Rows` with service-b too
(`shared_streaming_query`, ADR 0017 P4b): it streams the rows `1..=count`, each naming the service
and instance, lazily under the sender's credits; with `failAt > 0` the item for that row is an
`Err` (`HANDLER_ERROR` "Rows failed at row n") and the stream ends there. Serves the shared
subscription query `examples.WatchCounter` too (`shared_subscription_query`, ADR 0017 P4c): an
in-memory counter per name; the handler answers the initial result (the current value, 0 for a
name nobody incremented) and the command `examples.IncrementCounter(name, service)` (c owns it
alone; service-b increments through its own HTTP endpoint) adds one and pushes a
`CounterChanged` to every subscription of that name through the `QueryUpdateEmitter`, in the
order of the values (the counters have no database write to commit first, so it emits
right away). It also answers the shared query `examples.OpenSubscriptions` with
`emitter.open_subscriptions()`.

## The banner saga (sagas S2)

Every shouted text is printed as a banner by the sandbox's fake printer (`kraken-messaging/examples/fake-printer`),
through the Rust form of service-b's postcard saga with service-c's own types (`src/banner.rs`):

- `examples.Banner` starts on `examples.TextShouted` (the job id is `banner-<text in lower case>`, the association
  `jobId`), sends `examples.PrintBanner` to service-c's own idempotent adapter (`idempotent_command`, delivered locally
  by the saga relay) and gives the printer 30 s. The printer prints the text in lower case and answers by its prefix,
  the way it answers postcards: `webhook-*` posts to service-c's `POST /printer/webhook` (checked with
  `external::webhook::verify`, applied once through `app.ingress()`), `topic-*` produces to the printer's topic for
  service-c, `printer.banner.events` (the external source `printer-banner`), `poll-*` makes the saga send
  `examples.PollBannerJob` every 2 s (the adapter publishes `examples.BannerPrinted` once the printer says `DONE`),
  `silent-*` never answers and `crash-*` crashes before replying.
- `examples.BannerPrinted` ends the saga; the timeout publishes `examples.BannerFailed` `TIMEOUT`, and a command that
  failed for good (`SagaCommandFailed`, for example `UNKNOWN_OUTCOME` after the crash) `BannerFailed` with that reason.
- Each request tells the printer where to answer (`replyWebhook`, `replyTopic`), so banner notifications never reach
  service-b's postcard saga.
- `GET /banners/{jobId}` reads the outcome (`{"jobId","status":"PRINTED"|"FAILED","reason"}`), 404 while in flight: an
  in-memory projection of `BannerPrinted` and `BannerFailed`, lost on restart.

The saga is unit-tested with `SagaTestFixture` (the crate's feature `testing`), see `src/banner.rs`. Settings:
`SERVER_PORT` (8094; compose publishes the container's 8080 on localhost:8094), `PRINTER_URL`
(`http://localhost:8093`), `PRINTER_WEBHOOK_SECRET` (the sandbox's dev-only value shared with the fake printer; not a
secret), `PRINTER_REPLY_WEBHOOK` (default `http://localhost:<port>/printer/webhook`), `PRINTER_BANNER_TOPIC`
(`printer.banner.events`).

Configuration is the crate's `Config::from_env`: `KRAKEN_SERVICE_NAME` (`service-c`), `KRAKEN_HUB_ADDRESS`,
`KRAKEN_KAFKA_BOOTSTRAP`, `KRAKEN_MONGODB_URI` (its own database), `RUST_LOG` (default `info`).

Run from the IDE against the sandbox infrastructure:

```bash
KRAKEN_SERVICE_NAME=service-c KRAKEN_HUB_ADDRESS=static://localhost:7002 KRAKEN_KAFKA_BOOTSTRAP=localhost:9092 \
KRAKEN_MONGODB_URI='mongodb://localhost:27017/service-c?replicaSet=rs0&directConnection=true' cargo run -p service-c
```
