//! The hub side: one bidirectional stream per instance, typed handler registration, inbound
//! dispatch and outbound sends. Protocol in `proto/hub.proto`, behaviour mirrors the Kotlin
//! `HubConnection`/`HubBus`.

mod connection;

pub use connection::{HubConnection, HubSettings, Inbound};
