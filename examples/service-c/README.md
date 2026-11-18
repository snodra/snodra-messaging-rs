# service-c

The Rust participant of the examples sandbox (see `snodra-messaging/examples/README.md` for the whole
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

Every shouted text is printed as a banner by the sandbox's fake printer (`snodra-messaging/examples/fake-printer`),
through the Rust form of service-b's postcard saga with service-c's own types (`src/banner.rs`):

- `examples.Banner` starts on `examples.TextShouted` (the job id is `banner-<text in lower case>`, the association
  `jobId`), sends `examples.PrintBanner` to service-c's own idempotent adapter (`idempotent_command`, delivered locally
  by the saga relay) and gives the printer 30 s. The printer prints the text in lower case and answers by its prefix,
  the way it answers postcards: `webhook-*` posts to service-c's `POST /printer/webhook` (checked with
  `external::webhook::verify`, applied once through `app.ingress()`), `topic-*` produces to the printer's topic for
  service-c, `printer.banner.events` (the external source `printer-banner`), `poll-*` makes the saga send
  `examples.PollBannerJob` every 2 s (the adapter publishes `examples.BannerPrinted` once the printer says `DONE`; a
  publish that fails is retryable, so the relay sends the poll again instead of failing the banner; the job id is sent
  as one percent-encoded path segment of `GET /jobs/{id}`),
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

## The audit processor and the dead letters (P6a)

service-c runs a named event processor, `audit` (`src/audit.rs`): `builder.event_in::<ShoutAudit, _, _>("audit", ...)`, its own
Kafka consumer in the group `service-c.audit` with the markers `processor:audit:<eventId>` and its own dead letters
(`default`'s are `service-c.default` and `processor:default:<eventId>`). `examples.ShoutAudit` (`{key, text, at}`) is service-c's own type: service-b's
`examples.AuditNote` is another, so each service audits only its own notes.

- `POST /audit/notes` `{"key","text"}` publishes a note keyed by `key` (202); `GET /audit/handled` lists what the processor handled,
  in order (in memory, lost on restart).
- `POST /audit/failing?on=true|false` is the example's failure switch: while on, a note whose text starts with `poison` fails
  (`RetryableExternal`), so after the crate's attempts (5, one second apart) it is parked as `HANDLER_FAILED`, the later notes of its
  key as `SEQUENCED`, while other keys flow.
- `GET /audit/dead-letters[?processor=&limit=]`, `GET /audit/dead-letters/count[?processor=]`,
  `POST /audit/dead-letters/{id}/retry` (`{"result","detail"}`), `POST /audit/dead-letters/retry-all[?processor=audit]` and
  `DELETE /audit/dead-letters/{id}` call `app.dead_letters()`: `list`, `count`, `retry`, `retry_all` and `discard`. Same paths and
  JSON as service-b's; the sandbox is closed, a real service puts its own authorization in front of such endpoints.
- Processor control (P6b, ADR 0023): the hub pauses, resumes and resets `audit` (its HTTP API with the write token). The
  processor has a second handler, registered with `EventOptions::new().processor("audit").disallow_replay()`, and a reset
  handler: `GET /audit/resets` lists the resets that replayed something as the reset handler saw them
  (`[{"operationId","target","ranges":[{"partition","from","until"}]}]`, `target` `earliest`, `latest`, `timestamp <ms>` or
  `offsets {<partition>=<offset>, ...}`), `GET /audit/notified` the notes the second handler handled (`[{"key","text","at"}]`;
  a replay adds nothing there, while `GET /audit/handled` gets the replayed notes again). Same paths and JSON as service-b's.

Through the hub, with the sandbox's write token (its dev PKI setup writes the token as
`hub-control/dashboard-write.token`; every sandbox mode mounts it into both hubs), from `snodra-messaging-rs`:

```bash
W="Authorization: Bearer $(cat dashboard-write.token)"
HUB=http://localhost:7001/api/v1/services/service-c/processors/audit
curl -s -X POST -H "$W" "$HUB/pause"             # service-c's audit PAUSED on /api/v1/processors; notes are held
curl -s -X POST -H 'content-type: application/json' localhost:8094/audit/notes -d '{"key":"ada","text":"held"}'
curl -s -X POST -H "$W" "$HUB/resume"            # "held" is handled
curl -s -X POST -H "$W" -H 'Content-Type: application/json' -d '{"to":"earliest"}' "$HUB/reset"   # "resetHandlers":1
curl -s localhost:8094/audit/resets              # the reset, target earliest, its ranges
curl -s localhost:8094/audit/handled             # every note again; /audit/notified unchanged
curl -s "$HUB/dead-letters"                      # the letters without payloads; retry: POST "$HUB/dead-letters/<id>/retry"
```

The pure parts (the switch, the views of a letter, of a retry result and of a reset, the query parsing) are unit-tested in
`src/audit.rs`; smoke steps 26 to 26h drive the HTTP side against the real hub, Kafka and MongoDB, and steps 27 to 27e pause, resume
and reset `audit` and retry and discard its letters through the hub's HTTP API (`snodra-messaging/examples/README.md`, "Event
processors and dead letters").

Configuration is the crate's `Config::from_env`: `SNODRA_SERVICE_NAME` (`service-c`), `SNODRA_SERVER_ADDRESS`,
`SNODRA_KAFKA_BOOTSTRAP`, `SNODRA_INSTANCE_ID` (default a fresh UUID), `RUST_LOG` (default `info`), and the service's own
`SNODRA_MONGODB_URI` (its database, the message store of `snodra-messaging-mongo`, whose indexes `start()` creates
with the config's retentions).

Metrics (ADR 0026, the crate README's "Metrics"): with `SNODRA_OTLP_ENDPOINT` set (OTLP/gRPC, e.g.
`http://otel-lgtm:4317`) `src/telemetry.rs` installs an OTLP meter provider as the global one before the messaging is
built, with the resource `service.name` = `SNODRA_SERVICE_NAME` and `service.instance.id` = the instance id the hub
sees, and exports every 60 s (`OTEL_METRIC_EXPORT_INTERVAL`, ms, changes it); the library's series then reach
Prometheus with `job="service-c"` and `instance="<instance id>"`. Unset (the default), no provider is installed and
the library records nothing. The examples sandbox sets both (`compose.local.yaml`: 5 s).

`GET /greetings/{name}/count` sends service-b's query `examples.CountGreetings` through the hub and answers
`{"name","count"}` (502 with the error when the call fails): the sandbox's one call from Rust to Kotlin, which gives
`snodra_hub_client_request_duration_seconds` a Rust sender (the examples smoke's step 29c compares it with
service-a's).

Run from the IDE against the sandbox infrastructure:

```bash
SNODRA_SERVICE_NAME=service-c SNODRA_SERVER_ADDRESS=static://localhost:7002 SNODRA_KAFKA_BOOTSTRAP=localhost:9092 \
SNODRA_MONGODB_URI='mongodb://localhost:27017/service-c?replicaSet=rs0&directConnection=true' cargo run -p service-c
```
