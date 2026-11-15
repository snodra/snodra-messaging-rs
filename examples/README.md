# Examples

- `service-c/`: the Rust example service of the sandbox; see its README. Besides the messaging, saga and query examples it runs the
  named event processor `audit` with example admin endpoints over `dead_letters()` (P6a), a reset handler and a no-replay handler
  (P6b); the sandbox's smoke steps 26 to 26h drive it and its dead letters locally, steps 27 to 27e through the hub (pause, resume,
  a reset to `earliest`, the dead letters), in every sandbox mode (`kraken-messaging/examples/README.md`, "Event processors and dead
  letters").
- `orders-sql/`: a small orders service on PostgreSQL with the SQL message store (`kraken-messaging-sqlx`), the twin of
  kraken-messaging's `examples/orders-sql`. `PlaceOrder` (a command through the hub) writes the order row and appends
  `OrderPlaced` to the outbox in one transaction; the relay publishes it to the event store (the in-memory one, to keep the
  example to one database); its handler confirms the order in the transaction of the event, with its processed-event marker.
  Run it with `DATABASE_URL` and the library's `KRAKEN_*` settings (`cargo run -p orders-sql`); its end-to-end test
  (`cargo test -p orders-sql`) starts PostgreSQL and the hub (`KRAKEN_HUB_IMAGE`) in containers.
