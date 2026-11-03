//! The bounded retry of one saga event inside stage 2 (ADR 0021 rulings 3 and 33). Only a
//! **conflict** (a version check, a start-key duplicate, a Mongo write conflict or a transient
//! transaction error) backs off: it is retried up to `max_attempts` attempts in all with the
//! policy's jittered exponential backoff (default 5 attempts, 200 ms doubling up to 5 s). Any
//! other failure, a handler's above all, is not cured by waiting: it gets
//! [`SagaRetry::HANDLER_ATTEMPTS`] attempts at most, [`SagaRetry::handler_pause`] (never more
//! than 50 ms) apart, and the event is then parked.

use std::time::Duration;

use crate::retry::RetryPolicy;

#[derive(Clone, Debug)]
pub struct SagaRetry {
    policy: RetryPolicy,
}

impl SagaRetry {
    /// Attempts in all for a failure that is not a conflict: the original and one repeat, in case
    /// it was a blip.
    pub const HANDLER_ATTEMPTS: u32 = 2;
    /// The longest pause before the quick repeat.
    pub const QUICK_PAUSE: Duration = Duration::from_millis(50);

    pub fn new(policy: RetryPolicy) -> SagaRetry {
        SagaRetry { policy }
    }

    /// Attempts in all for a conflict (`sagas.max-attempts`).
    pub fn max_attempts(&self) -> u32 {
        self.policy.max_attempts
    }

    /// The wait after `conflicts` conflicts (1 for the first), jittered.
    pub fn conflict_pause(&self, conflicts: u32) -> Duration {
        self.policy.jittered_backoff(conflicts)
    }

    /// The short wait before the one quick repeat of an attempt that failed for a reason other
    /// than a conflict.
    pub fn handler_pause(&self) -> Duration {
        self.policy.initial_backoff.min(Self::QUICK_PAUSE)
    }
}

impl SagaRetry {
    /// The default policy: 5 attempts, 200 ms doubling up to 5 s (ruling 16: only the attempts
    /// are configurable, `KRAKEN_SAGAS_MAX_ATTEMPTS`).
    pub fn default_policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(200),
            multiplier: 2.0,
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl Default for SagaRetry {
    /// 5 attempts, 200 ms doubling up to 5 s.
    fn default() -> Self {
        SagaRetry::new(SagaRetry::default_policy())
    }
}
