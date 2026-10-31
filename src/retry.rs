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
    /// Wait before the second attempt (each wait is jittered by ±20 %).
    pub initial_backoff: Duration,
    /// Each further wait is the previous one times this.
    pub multiplier: f64,
    /// Upper bound of a single wait (at the bound: 80-100 % of it).
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

    /// The base of the wait after failed attempt `attempt` (1-based), deterministic:
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

    /// The wait actually used after failed attempt `attempt`: [`backoff`](Self::backoff) jittered
    /// by ±20 % and never above `max_backoff` (at the cap 80-100 % of it), like the reconnect
    /// backoff, so callers that failed together do not retry together.
    pub fn jittered_backoff(&self, attempt: u32) -> Duration {
        jittered(self.backoff(attempt), self.max_backoff, random_unit())
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

/// `base` scaled by a factor in [0.8, 1.2) chosen by `unit` (in [0, 1)), never above `max`. Used
/// for the hub reconnect backoff and the retry waits.
pub(crate) fn jittered(base: Duration, max: Duration, unit: f64) -> Duration {
    let unit = unit.clamp(0.0, 1.0);
    if base >= max {
        // At the cap jitter downward only, over [0.8, 1.0] x max: clamping a ±20 % wait would put
        // about half of a reconnecting fleet on exactly the maximum.
        max.mul_f64(0.8 + 0.2 * unit)
    } else {
        base.mul_f64(0.8 + 0.4 * unit).min(max)
    }
}

/// A uniformly distributed value in [0, 1) from the v4 UUID generator (no extra dependency).
pub(crate) fn random_unit() -> f64 {
    let bits = (uuid::Uuid::new_v4().as_u128() >> 64) as u64;
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_twenty_percent_and_never_exceeds_max() {
        let max = Duration::from_secs(5);
        let base = Duration::from_secs(1);
        assert_eq!(jittered(base, max, 0.0), Duration::from_millis(800));
        assert_eq!(jittered(base, max, 0.5), Duration::from_millis(1000));
        assert!(jittered(base, max, 0.999_999) < Duration::from_millis(1200));
        assert!(jittered(base, max, 0.999_999) > Duration::from_millis(1199));
        // At the cap the wait is jittered downward over [0.8, 1.0] x max, so a fleet does not all
        // land on exactly the maximum.
        assert_eq!(jittered(max, max, 0.0), Duration::from_secs(4));
        assert_eq!(jittered(max, max, 0.5), Duration::from_millis(4500));
        let top = jittered(max, max, 0.999_999);
        assert!(top < max && top > Duration::from_millis(4999), "{top:?}");
        // Below the cap the ±20 % never exceeds the maximum either.
        assert!(jittered(Duration::from_millis(4500), max, 0.999) <= max);
    }

    #[test]
    fn random_unit_is_in_range_and_varies() {
        let values: Vec<f64> = (0..200).map(|_| random_unit()).collect();
        assert!(values.iter().all(|v| (0.0..1.0).contains(v)));
        let distinct: std::collections::HashSet<u64> = values.iter().map(|v| v.to_bits()).collect();
        assert!(distinct.len() > 190);
    }

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
    fn waits_are_jittered_by_twenty_percent_and_never_exceed_the_cap() {
        let p = RetryPolicy::enabled(); // 100 ms doubling to 2 s
        // Attempt 2 of this one has a base of 1.8 s: 1.2 x that would pass the 2 s cap.
        let near_cap = RetryPolicy {
            initial_backoff: Duration::from_millis(900),
            ..RetryPolicy::enabled()
        };
        for (policy, attempt) in [(&p, 1), (&p, 3), (&p, 5), (&p, 6), (&p, 9), (&near_cap, 2)] {
            let base = policy.backoff(attempt);
            let low = base.mul_f64(0.8);
            let high = base.mul_f64(1.2).min(policy.max_backoff);
            let waits: Vec<Duration> = (0..500).map(|_| policy.jittered_backoff(attempt)).collect();
            assert!(
                waits.iter().all(|w| (low..=high).contains(w)),
                "attempt {attempt}, base {base:?}: a wait outside {low:?}..={high:?}"
            );
            let distinct: std::collections::HashSet<&Duration> = waits.iter().collect();
            assert!(
                distinct.len() > 1,
                "attempt {attempt}, base {base:?}: every wait was {:?}",
                waits[0]
            );
        }
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
