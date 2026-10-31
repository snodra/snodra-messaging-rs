//! Caller-side retry for commands and queries sent over the hub (opt-in).
//!
//! [`is_retryable`] is the one place that decides which failures may be retried. It is shared by
//! [`crate::hub::Sender`] and, later, the saga relay, so both apply the same transient /
//! non-transient split (mirrors Axon's `RetryScheduler`):
//!
//! | Failure | Command | Query |
//! |---|---|---|
//! | `HubUnavailable` (never written) | retry | retry |
//! | `NoHandler` (the handler never ran) | retry | retry |
//! | `HandlerBusy` (hub queue full, the handler never ran) | retry | retry |
//! | `HandlerDisconnected` (outcome unknown) | no | retry |
//! | `Timeout` (outcome unknown) | no | retry |
//! | `HubConnectionLost` (outcome unknown) | no | retry |
//! | anything else (handler errors, auth, invalid messages) | no | no |
//!
//! A command is never retried when its outcome is unknown: the handler may already have run.

use std::time::Duration;

use crate::error::MessagingError;
use crate::proto::RequestKind;

#[derive(Clone, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Off by default: without it every failure reaches the caller at once, as before.
    pub enabled: bool,
    /// Attempts in total, the first one included.
    pub max_attempts: u32,
    /// Wait before the second attempt.
    pub initial_backoff: Duration,
    /// Each further wait is the previous one times this.
    pub multiplier: f64,
    /// Upper bound of a single wait.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            enabled: false,
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            multiplier: 2.0,
            max_backoff: Duration::from_secs(2),
        }
    }
}

impl RetryPolicy {
    /// The default policy, switched on.
    pub fn enabled() -> Self {
        RetryPolicy {
            enabled: true,
            ..RetryPolicy::default()
        }
    }

    /// No retries (one attempt), whatever the configured policy says.
    pub fn disabled() -> Self {
        RetryPolicy::default()
    }

    /// Attempts this policy allows, at least one.
    pub fn attempts(&self) -> u32 {
        if self.enabled {
            self.max_attempts.max(1)
        } else {
            1
        }
    }

    /// The wait after failed attempt `attempt` (1-based) before the next one:
    /// `initial_backoff * multiplier^(attempt - 1)`, capped at `max_backoff`.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let factor = self
            .multiplier
            .max(1.0)
            .powi(attempt.saturating_sub(1) as i32);
        let secs =
            (self.initial_backoff.as_secs_f64() * factor).min(self.max_backoff.as_secs_f64());
        Duration::from_secs_f64(secs.max(0.0))
    }
}

/// Whether a request of `kind` that failed with `error` may be sent again (with a fresh request
/// id). See the module table.
pub fn is_retryable(kind: RequestKind, error: &MessagingError) -> bool {
    match error {
        MessagingError::HubUnavailable(_)
        | MessagingError::NoHandler(_)
        | MessagingError::HandlerBusy(_) => true,
        MessagingError::HandlerDisconnected(_)
        | MessagingError::Timeout(_)
        | MessagingError::HubConnectionLost(_) => kind == RequestKind::Query,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_errors() -> Vec<(MessagingError, bool, bool)> {
        // (error, retried for commands, retried for queries)
        vec![
            (MessagingError::HubUnavailable("x".into()), true, true),
            (MessagingError::NoHandler("t".into()), true, true),
            (MessagingError::HandlerBusy("t".into()), true, true),
            (MessagingError::HandlerDisconnected("t".into()), false, true),
            (MessagingError::Timeout("t".into()), false, true),
            (MessagingError::HubConnectionLost("x".into()), false, true),
            (
                MessagingError::RemoteHandler {
                    type_name: "t".into(),
                    message: "boom".into(),
                    details: vec![],
                },
                false,
                false,
            ),
            (MessagingError::Unauthenticated("x".into()), false, false),
            (MessagingError::AccessDenied("x".into()), false, false),
            (MessagingError::InvalidMessage("x".into()), false, false),
            (
                MessagingError::HubRegistrationRejected("x".into()),
                false,
                false,
            ),
            (MessagingError::Storage("x".into()), false, false),
            (MessagingError::Transport("x".into()), false, false),
        ]
    }

    #[test]
    fn classification_table() {
        for (error, command, query) in all_errors() {
            assert_eq!(
                is_retryable(RequestKind::Command, &error),
                command,
                "command {error:?}"
            );
            assert_eq!(
                is_retryable(RequestKind::Query, &error),
                query,
                "query {error:?}"
            );
        }
    }

    #[test]
    fn defaults_are_off_three_attempts_100ms_doubling_to_2s() {
        let p = RetryPolicy::default();
        assert!(!p.enabled);
        assert_eq!(p.attempts(), 1);
        assert_eq!(RetryPolicy::enabled().attempts(), 3);
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.initial_backoff, Duration::from_millis(100));
        assert_eq!(p.multiplier, 2.0);
        assert_eq!(p.max_backoff, Duration::from_secs(2));
    }

    #[test]
    fn backoff_grows_by_the_multiplier_up_to_the_cap() {
        let p = RetryPolicy::enabled();
        let ms = |a| p.backoff(a).as_millis();
        assert_eq!(
            [ms(1), ms(2), ms(3), ms(4), ms(5), ms(6)],
            [100, 200, 400, 800, 1600, 2000]
        );
        let zero = RetryPolicy {
            max_attempts: 0,
            ..RetryPolicy::enabled()
        };
        assert_eq!(zero.attempts(), 1);
    }
}
