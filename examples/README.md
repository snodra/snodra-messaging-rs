# Examples

- `service-c/`: the Rust example service of the sandbox; see its README. Besides the messaging, saga and query examples it runs the
  named event processor `audit` with example admin endpoints over `dead_letters()` (P6a), a reset handler and a no-replay handler
  (P6b); the sandbox's smoke steps 26 to 26h drive it and its dead letters locally, steps 27 to 27e through the hub (pause, resume,
  a reset to `earliest`, the dead letters), in every sandbox mode (`kraken-messaging/examples/README.md`, "Event processors and dead
  letters").
