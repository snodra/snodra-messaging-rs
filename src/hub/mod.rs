//! The hub side: one bidirectional stream per hub ([`HubConnection`]; every hub of
//! `hub_address` through a [`HubPool`]), typed handler registration, inbound dispatch and outbound
//! sends. Protocol in `proto/hub.proto`, behaviour mirrors the Kotlin
//! `HubConnection`/`HubBus`.

mod auth;
mod connection;
mod dispatcher;
mod pool;
mod registry;
mod sender;
mod subscriptions;
mod targets;
mod tls;

pub use auth::{
    AccessToken, AuthKind, AuthStatus, FetchResult, TokenError, TokenProvider, TokenStats,
    refresh_margin,
};
pub use connection::{
    CAPABILITIES, CLIENT_VERSION, FLOW_CONTROL, Handshake, HubConnection, HubSettings,
    INSTANCE_STATUS, Inbound, PERMIT_BATCH, PERMIT_FLUSH_AFTER, PROTOCOL_VERSION, RegisteredTypes,
    ReplyTo, SCATTER_GATHER, STREAMING, SUBSCRIPTION, StreamId,
};
pub use dispatcher::{AdmissionHook, Dispatcher};
pub use pool::{DRAIN_LIMIT, HubPool, Resolver};
pub use registry::{EventContext, HandlerRegistry};
pub use sender::{
    DEFAULT_CREDIT, LEGACY_REQUEST_TIMEOUT, LOCAL_GUARD_MARGIN, RETRY_MIN_REMAINING, RemoteError,
    ScatterOptions, ScatterPart, ScatterResult, SendOptions, Sender, SenderSettings, StreamOptions,
    Subscription, SubscriptionOptions, request_timing, retry_budget,
};
pub use subscriptions::{DEFAULT_MAX_SUBSCRIPTIONS, QueryUpdateEmitter};
pub use targets::{
    DNS_REFRESH, HubTargets, permit_share, rendezvous_score, resolved_targets, target_id,
};
pub use tls::read_ca_file;
