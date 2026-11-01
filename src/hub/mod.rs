//! The hub side: one bidirectional stream per hub ([`HubConnection`]; every hub of
//! `hub_address` through a [`HubPool`]), typed handler registration, inbound dispatch and outbound
//! sends. Protocol in `proto/hub.proto`, behaviour mirrors the Kotlin
//! `HubConnection`/`HubBus`.

mod connection;
mod dispatcher;
mod pool;
mod registry;
mod sender;
mod targets;

pub use connection::{
    CAPABILITIES, CLIENT_VERSION, FLOW_CONTROL, Handshake, HubConnection, HubSettings,
    INSTANCE_STATUS, Inbound, PERMIT_BATCH, PERMIT_FLUSH_AFTER, PROTOCOL_VERSION, ReplyTo,
    StreamId,
};
pub use dispatcher::Dispatcher;
pub use pool::{DRAIN_LIMIT, HubPool, Resolver};
pub use registry::{EventContext, HandlerRegistry};
pub use sender::{
    LEGACY_REQUEST_TIMEOUT, LOCAL_GUARD_MARGIN, RETRY_MIN_REMAINING, SendOptions, Sender,
    SenderSettings, request_timing, retry_budget,
};
pub use targets::{DNS_REFRESH, HubTargets, permit_share, rendezvous_score};
