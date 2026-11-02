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
order of the values (service-c has no HTTP and no database write to commit first, so it emits
right away). It also answers the shared query `examples.OpenSubscriptions` with
`emitter.open_subscriptions()`.

Configuration is the crate's `Config::from_env`: `KRAKEN_SERVICE_NAME` (`service-c`), `KRAKEN_HUB_ADDRESS`,
`KRAKEN_KAFKA_BOOTSTRAP`, `KRAKEN_MONGODB_URI` (its own database), `RUST_LOG` (default `info`).

Run from the IDE against the sandbox infrastructure:

```bash
KRAKEN_SERVICE_NAME=service-c KRAKEN_HUB_ADDRESS=static://localhost:7002 KRAKEN_KAFKA_BOOTSTRAP=localhost:9092 \
KRAKEN_MONGODB_URI='mongodb://localhost:27017/service-c?replicaSet=rs0&directConnection=true' cargo run -p service-c
```
