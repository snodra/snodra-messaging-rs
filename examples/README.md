# Examples

- `service-c/`: the Rust example service of the sandbox; see its README. Besides the messaging, saga and query examples it runs the
  named event processor `audit` with example admin endpoints over `dead_letters()` (P6a); the sandbox's smoke steps 26 to 26h drive
  it (`kraken-messaging/examples/README.md`, "Event processors and dead letters").
