//! Named event processors (ADR 0022, P6a): each processor is its own Kafka consumer, with its own
//! consumer group, markers, dead letters and status.
//!
//! - The unnamed processor `default` holds the handlers registered with
//!   [`event`](crate::KrakenMessagingBuilder::event) and the sagas; it keeps the service's group
//!   (the service name) and the plain `<eventId>` markers, so an upgrade re-handles nothing.
//! - A named processor holds the handlers registered with
//!   [`event_in`](crate::KrakenMessagingBuilder::event_in); its group is `<service>.<name>` and
//!   its markers `processor:<name>:<eventId>`. It runs stage 1 only (no sagas).

use crate::error::MessagingError;

mod admin;
pub mod dead_letters;
pub mod monitor;
pub mod store;

pub use admin::{DEFAULT_LIST_LIMIT, DeadLetters, RetryAllResult, RetryResult};
pub use monitor::ProcessorMonitor;

/// The unnamed processor's name.
pub const DEFAULT_PROCESSOR: &str = "default";
/// The prefix of a named processor's markers in `processed_events`.
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

/// The consumer group of processor `name` when the service's group is `base`: `base` itself for
/// `default`, else `<base>.<name>`.
pub fn group_id(base: &str, name: &str) -> String {
    if name == DEFAULT_PROCESSOR {
        base.to_string()
    } else {
        format!("{base}.{name}")
    }
}

/// The prefix of processor `name`'s markers: none for `default`, else `processor:<name>:`.
pub fn marker_prefix(name: &str) -> Option<String> {
    (name != DEFAULT_PROCESSOR).then(|| format!("{PROCESSOR_MARKER_PREFIX}{name}:"))
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
    fn groups_and_markers() {
        assert_eq!(group_id("service-c", "default"), "service-c");
        assert_eq!(group_id("service-c", "orders"), "service-c.orders");
        assert_eq!(marker_prefix("default"), None);
        assert_eq!(
            marker_prefix("orders").as_deref(),
            Some("processor:orders:")
        );
    }
}
