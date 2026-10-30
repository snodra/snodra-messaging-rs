# service-c

The Rust participant of the examples sandbox (see `kraken-messaging/examples/README.md` for the whole
picture and the curl walkthrough). Handles `examples.Shout` and `examples.Reverse`, publishes
`examples.TextShouted` / `examples.TextReversed` through its outbox, logs `examples.GreetingDelivered`.

Configuration is the crate's `Config::from_env`: `KRAKEN_SERVICE_NAME` (`service-c`), `KRAKEN_HUB_ADDRESS`,
`KRAKEN_KAFKA_BOOTSTRAP`, `KRAKEN_MONGODB_URI` (its own database), `RUST_LOG` (default `info`).

Run from the IDE against the sandbox infrastructure:

```bash
KRAKEN_SERVICE_NAME=service-c KRAKEN_HUB_ADDRESS=static://localhost:7002 KRAKEN_KAFKA_BOOTSTRAP=localhost:9092 \
KRAKEN_MONGODB_URI='mongodb://localhost:27017/service-c?replicaSet=rs0&directConnection=true' cargo run -p service-c
```
