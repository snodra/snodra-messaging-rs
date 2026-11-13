//! Named event processors (ADR 0022, P6a): each processor is its own Kafka consumer, with its own
//! consumer group, markers, dead letters and status.
//!
//! - Every processor's group is `<service>.<name>` and its markers `processor:<name>:<eventId>`,
//!   `default` included.
//! - The unnamed processor `default` holds the handlers registered with
//!   [`event`](crate::KrakenMessagingBuilder::event) and the sagas.
//! - A named processor holds the handlers registered with
//!   [`event_in`](crate::KrakenMessagingBuilder::event_in). It runs stage 1 only (no sagas).
//!
//! Processor control (ADR 0023, P6b): a processor is paused and resumed on every instance, and
//! reset to a position as a leased operation (its range replayed), through its state in the
//! message store ([`ProcessorControlStore`](crate::store::ProcessorControlStore)); the hub sends
//! the requests ([`ProcessorControl`]).

use crate::error::MessagingError;

mod admin;
mod control;
pub mod dead_letters;
pub mod monitor;
pub mod reset;
pub mod runner;

pub use admin::{DEFAULT_LIST_LIMIT, DeadLetters, RetryAllResult, RetryResult};
pub use control::{MAX_CONTROL_TEXT_BYTES, MAX_LIST_LIMIT, PAUSED_LOCALLY, ProcessorControl};
pub use monitor::ProcessorMonitor;
pub use reset::ResetContext;
pub use runner::ProcessorRunner;

/// How an event handler is registered
/// ([`event_with`](crate::KrakenMessagingBuilder::event_with)): the processor it belongs to
/// (`default` unless named, ADR 0022) and whether it runs again when that processor replays a
/// range after a reset (ADR 0023; Kotlin's `@DisallowReplay`).
///
/// ```ignore
/// builder.event_with::<OrderPlaced, _, _>(
///     EventOptions::new().processor("mail").disallow_replay(),
///     |e, ctx| async move { send_confirmation(&e).await },
/// )
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventOptions {
    processor: Option<String>,
    disallow_replay: bool,
}

impl EventOptions {
    /// Processor `default`, replayed.
    pub fn new() -> EventOptions {
        EventOptions::default()
    }

    /// The named processor the handler belongs to (as [`event_in`](crate::KrakenMessagingBuilder::event_in)).
    pub fn processor(mut self, name: &str) -> EventOptions {
        self.processor = Some(name.to_string());
        self
    }

    /// The handler does not run for a record its processor replays (a handler with an effect
    /// outside the service that must happen once per record); its marker is still written.
    pub fn disallow_replay(mut self) -> EventOptions {
        self.disallow_replay = true;
        self
    }

    pub fn processor_name(&self) -> Option<&str> {
        self.processor.as_deref()
    }

    pub fn disallows_replay(&self) -> bool {
        self.disallow_replay
    }
}

/// The unnamed processor's name.
pub const DEFAULT_PROCESSOR: &str = "default";
/// The prefix of every processor's markers in `processed_events`.
pub const PROCESSOR_MARKER_PREFIX: &str = "processor:";

/// Checks a processor name given to `event_in`: `^[a-z0-9][a-z0-9-]{0,47}$`, and not `default`.
/// The error names the handler (`what`, e.g. "event handler for orders.OrderPlaced").
pub fn validate_processor_name(name: &str, what: &str) -> Result<(), MessagingError> {
    if name == DEFAULT_PROCESSOR {
        return Err(MessagingError::InvalidMessage(format!(
            "{what}: processor name 'default' is the unnamed processor; register the handler with event() instead of event_in()"
        )));
    }
    let bytes = name.as_bytes();
    let head = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let valid = !bytes.is_empty()
        && bytes.len() <= 48
        && head(bytes[0])
        && bytes[1..].iter().all(|&b| head(b) || b == b'-');
    if valid {
        Ok(())
    } else {
        Err(MessagingError::InvalidMessage(format!(
            "{what}: processor name '{name}' must match ^[a-z0-9][a-z0-9-]{{0,47}}$"
        )))
    }
}

/// The consumer group of processor `name` when the service's group is `base`: `<base>.<name>`.
pub fn group_id(base: &str, name: &str) -> String {
    format!("{base}.{name}")
}

/// The prefix of processor `name`'s markers: `processor:<name>:`.
pub fn marker_prefix(name: &str) -> String {
    format!("{PROCESSOR_MARKER_PREFIX}{name}:")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_names() {
        for name in ["orders", "a", "0", "read-model-2", &"a".repeat(48), "9-x-"] {
            assert!(
                validate_processor_name(name, "event handler for t.E").is_ok(),
                "{name}"
            );
        }
    }

    #[test]
    fn invalid_names_are_an_error_naming_the_handler_and_the_rule() {
        for name in [
            "",
            "-orders",
            "Orders",
            "orders_1",
            "orders.x",
            "ørders",
            " orders",
            &"a".repeat(49),
        ] {
            let e = validate_processor_name(name, "event handler for t.OrderPlaced")
                .unwrap_err()
                .to_string();
            assert!(e.contains("event handler for t.OrderPlaced"), "{name}: {e}");
            assert!(e.contains("^[a-z0-9][a-z0-9-]{0,47}$"), "{name}: {e}");
        }
        let e = validate_processor_name("default", "event handler for t.OrderPlaced")
            .unwrap_err()
            .to_string();
        assert!(e.contains("event handler for t.OrderPlaced"), "{e}");
        assert!(e.contains("'default'"), "{e}");
    }

    #[test]
    fn groups_and_markers_are_uniform_default_included() {
        assert_eq!(group_id("service-c", "default"), "service-c.default");
        assert_eq!(group_id("service-c", "orders"), "service-c.orders");
        assert_eq!(marker_prefix("default"), "processor:default:");
        assert_eq!(marker_prefix("orders"), "processor:orders:");
    }
}
