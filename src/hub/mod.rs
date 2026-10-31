//! The hub side: one bidirectional stream per instance, typed handler registration, inbound
//! dispatch and outbound sends. Protocol in `proto/hub.proto`, behaviour mirrors the Kotlin
//! `HubConnection`/`HubBus`.

mod connection;
mod dispatcher;
mod registry;
mod sender;

pub use connection::{
    CAPABILITIES, CLIENT_VERSION, FLOW_CONTROL, Handshake, HubConnection, HubSettings, Inbound,
    PERMIT_BATCH, PERMIT_FLUSH_AFTER, PROTOCOL_VERSION, ReplyTo, StreamId,
};
pub use dispatcher::Dispatcher;
pub use registry::{EventContext, HandlerRegistry};
pub use sender::{
    LEGACY_REQUEST_TIMEOUT, LOCAL_GUARD_MARGIN, SendOptions, Sender, SenderSettings,
    request_timing, retry_budget,
};
