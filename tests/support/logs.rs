//! Captures what `tracing` prints, to assert on log lines (current-thread tests only: the
//! subscriber is thread-local).
#![allow(dead_code)]

use std::io::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    /// Captures everything at DEBUG and above (no ANSI) until the guard is dropped.
    ///
    /// First makes a global subscriber that enables nothing but keeps every callsite worth
    /// asking: a callsite first reached on a test thread without a subscriber would otherwise be
    /// cached as of no interest, and a capture on another thread would miss it (tests run in
    /// parallel). Threads without a capture do no work for it.
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        static GLOBAL: std::sync::Once = std::sync::Once::new();
        GLOBAL.call_once(|| {
            let _ = tracing::subscriber::set_global_default(AskEveryTime);
        });
        let sink = self.clone();
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_ansi(false)
                .with_writer(move || sink.clone())
                .finish(),
        )
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    pub fn lines(&self) -> Vec<String> {
        self.text().lines().map(str::to_string).collect()
    }
}

/// A subscriber that records nothing and answers "ask me each time" for every callsite, so the
/// thread-local captures decide.
struct AskEveryTime;

impl tracing::Subscriber for AskEveryTime {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, _: &tracing::Event<'_>) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}
