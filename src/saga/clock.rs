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
