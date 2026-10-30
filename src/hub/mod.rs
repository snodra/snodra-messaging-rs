//! The hub side: one bidirectional stream per instance, typed handler registration, inbound
//! dispatch and outbound sends. Protocol in `proto/hub.proto`, behaviour mirrors the Kotlin
//! `HubConnection`/`HubBus`.

mod connection;
mod dispatcher;
mod registry;

pub use connection::{HubConnection, HubSettings, Inbound};
pub use dispatcher::Dispatcher;
pub use registry::{EventContext, HandlerRegistry};
