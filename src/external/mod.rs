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
