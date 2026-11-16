//! The injectable clock of the saga runtime, relay and deadline scheduler: what "now" is for
//! deadline due times, backoffs and leases. Production uses [`Clock::system`]; tests (and the
//! test fixture) move a [`ManualClock`].

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crate::time::epoch_ms;

/// A source of the current time, cheap to clone and share.
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> SystemTime + Send + Sync>);

impl Clock {
    /// The wall clock.
    pub fn system() -> Clock {
        Clock(Arc::new(SystemTime::now))
    }

    /// The wall clock as it read when this clock was made, moved on by tokio's clock since: the
    /// wall clock in a running service, and a clock that follows paused time in a test
    /// (`#[tokio::test(start_paused = true)]`, `tokio::time::advance`). The in-memory stores'
    /// default.
    pub fn tokio() -> Clock {
        let (wall, start) = (SystemTime::now(), tokio::time::Instant::now());
        Clock(Arc::new(move || wall + start.elapsed()))
    }

    /// A clock reading `now()`.
    pub fn new(now: impl Fn() -> SystemTime + Send + Sync + 'static) -> Clock {
        Clock(Arc::new(now))
    }

    pub fn now(&self) -> SystemTime {
        (self.0)()
    }

    /// `now()` as epoch milliseconds (the precision the stores keep).
    pub fn now_ms(&self) -> i64 {
        epoch_ms(self.now())
    }
}

impl Default for Clock {
    fn default() -> Self {
        Clock::system()
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Clock({:?})", self.now())
    }
}

/// A clock a test moves: [`ManualClock::clock`] hands out the [`Clock`] that reads it.
#[derive(Clone, Debug)]
pub struct ManualClock(Arc<Mutex<SystemTime>>);

impl ManualClock {
    pub fn new(start: SystemTime) -> ManualClock {
        ManualClock(Arc::new(Mutex::new(start)))
    }

    pub fn now(&self) -> SystemTime {
        *self.0.lock().expect("clock lock")
    }

    pub fn set(&self, now: SystemTime) {
        *self.0.lock().expect("clock lock") = now;
    }

    pub fn advance(&self, by: Duration) {
        let mut now = self.0.lock().expect("clock lock");
        *now += by;
    }

    /// A [`Clock`] that follows this one.
    pub fn clock(&self) -> Clock {
        let shared = Arc::clone(&self.0);
        Clock::new(move || *shared.lock().expect("clock lock"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tokio clock reads the wall clock and moves with tokio's time, paused time included.
    #[tokio::test(start_paused = true)]
    async fn the_tokio_clock_follows_paused_time() {
        let before = SystemTime::now();
        let clock = Clock::tokio();
        let start = clock.now();
        assert!(
            start >= before - Duration::from_secs(1)
                && start <= SystemTime::now() + Duration::from_secs(1),
            "starts at the wall clock"
        );
        tokio::time::sleep(Duration::from_secs(3600)).await;
        let moved = clock.now().duration_since(start).expect("forward");
        assert!(
            moved >= Duration::from_secs(3600) && moved < Duration::from_secs(3601),
            "moved with paused time: {moved:?}"
        );
    }
}
