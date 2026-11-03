//! The hub side: one bidirectional stream per hub ([`HubConnection`]; every hub of
//! `hub_address` through a [`HubPool`]), typed handler registration, inbound dispatch and outbound
//! sends. Protocol in `proto/hub.proto`, behaviour mirrors the Kotlin
//! `HubConnection`/`HubBus`.

mod auth;
mod connection;
mod dispatcher;
mod mtls;
mod pool;
mod registry;
pub(crate) mod sender;
mod subscriptions;
mod targets;
mod tls;

pub use auth::{
    AccessToken, AuthKind, AuthStatus, CertificateStatus, FetchResult, TokenError, TokenProvider,
    TokenStats, WAKE_INTERVAL, WallClock, refresh_margin,
};
pub use connection::{
    CAPABILITIES, CLIENT_VERSION, CREDENTIALS_REFRESH, FLOW_CONTROL, Handshake, HubConnection,
    HubSettings, INSTANCE_STATUS, Inbound, PERMIT_BATCH, PERMIT_FLUSH_AFTER, PROTOCOL_VERSION,
    RegisteredTypes, ReplyTo, SCATTER_GATHER, STREAMING, SUBSCRIPTION, StreamId,
};
pub(crate) use dispatcher::{AbortOnDrop, invocation_error};
pub use dispatcher::{AdmissionHook, Dispatcher};
pub(crate) use mtls::{LoadError, check_valid_at, load as load_client_certificate};
pub use pool::{DRAIN_LIMIT, HubPool, ROLL_IDLE_LIMIT, ROLL_RECONNECT_LIMIT, Resolver};
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

#[cfg(test)]
pub(crate) use mtls::test_pki;
