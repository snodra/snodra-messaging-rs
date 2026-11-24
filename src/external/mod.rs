//! The integration toolkit: idempotent adapter handlers over `external_calls`
//! ([`calls`]), partner sources as local events ([`sources`], read through an event-store
//! add-on's [`ExternalSourceProvider`]), and the webhook ingress with its signature verifier
//! ([`ingress`], [`webhook`]).

pub mod calls;
pub mod ingress;
pub mod sources;
pub mod webhook;

pub use calls::{ExternalCallSettings, ExternalCalls, ExternalCallsHandle, IdempotencyKey};
pub use ingress::{ExternalEventIngress, INGRESS_MARKER_PREFIX, IngressResult};
pub use sources::{
    ExternalRecord, ExternalSourceProvider, ExternalSources, TranslatedEvent, Translator,
};

/// Source names an external source (a partner source or a webhook source) may not have:
/// the library's own families, the `processor:`, `saga:` and `ingress:` markers in
/// `processed_events` and the relay's `failed:<entry id>` dedupe ids, which a source's dedupe ids
/// `<source>:...` must never look like.
pub const RESERVED_SOURCE_NAMES: [&str; 4] = ["processor", "saga", "ingress", "failed"];

/// Whether `name` is one of the [`RESERVED_SOURCE_NAMES`].
pub fn is_reserved_source_name(name: &str) -> bool {
    RESERVED_SOURCE_NAMES.contains(&name)
}
