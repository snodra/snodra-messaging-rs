//! The integration toolkit of ADR 0021 (S2): idempotent adapter handlers over `external_calls`
//! ([`calls`]), partner Kafka topics as local events ([`kafka`]), and the webhook ingress with its
//! signature verifier ([`ingress`], [`webhook`]). The Rust form of the Kotlin `external` package.

pub mod calls;
pub mod ingress;
pub mod kafka;
pub mod webhook;

pub use calls::{ExternalCallSettings, ExternalCalls, ExternalCallsHandle, IdempotencyKey};
pub use ingress::{ExternalEventIngress, INGRESS_MARKER_PREFIX, IngressResult};
pub use kafka::{ExternalKafkaSources, ExternalSource, TranslatedEvent};

/// Source names an external source (a partner topic or a webhook source) may not have (ADR 0022):
/// the library's own marker families in `processed_events` start with them (`processor:`, `saga:`,
/// `ingress:`, `failed:`), and a source's dedupe ids `<source>:...` would collide with them.
pub const RESERVED_SOURCE_NAMES: [&str; 4] = ["processor", "saga", "ingress", "failed"];

/// Whether `name` is one of the [`RESERVED_SOURCE_NAMES`].
pub fn is_reserved_source_name(name: &str) -> bool {
    RESERVED_SOURCE_NAMES.contains(&name)
}
