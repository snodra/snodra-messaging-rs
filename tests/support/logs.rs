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
    /// Captures everything at DEBUG and above (no ANSI) until the guard is dropped (after
    /// [`install_global`]).
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        install_global();
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

/// The one global subscriber of a test binary, made once by [`Captured::install`] and by
/// `support::init_tracing`: it answers every callsite "ask each time" (a callsite first reached on
/// a thread without a capture would otherwise be cached as of no interest, and a capture on
/// another thread would miss it), and prints what `RUST_LOG` selects when it is set (debugging
/// with `--nocapture`), nothing otherwise.
pub fn install_global() {
    static GLOBAL: std::sync::Once = std::sync::Once::new();
    GLOBAL.call_once(|| {
        let printer = std::env::var_os("RUST_LOG").map(|_| {
            tracing::Dispatch::new(
                tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                    .with_test_writer()
                    .finish(),
            )
        });
        let _ = tracing::subscriber::set_global_default(AskEveryTime { printer });
    });
}

/// See [`install_global`].
struct AskEveryTime {
    printer: Option<tracing::Dispatch>,
}

impl tracing::Subscriber for AskEveryTime {
    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if let Some(printer) = &self.printer {
            printer.register_callsite(metadata);
        }
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        self.printer.as_ref().is_some_and(|p| p.enabled(metadata))
    }

    fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        match &self.printer {
            Some(printer) => printer.new_span(attributes),
            None => tracing::span::Id::from_u64(1),
        }
    }

    fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        if let Some(printer) = &self.printer {
            printer.record(span, values);
        }
    }

    fn record_follows_from(&self, span: &tracing::span::Id, follows: &tracing::span::Id) {
        if let Some(printer) = &self.printer {
            printer.record_follows_from(span, follows);
        }
    }

    fn event(&self, event: &tracing::Event<'_>) {
        if let Some(printer) = &self.printer {
            printer.event(event);
        }
    }

    fn enter(&self, span: &tracing::span::Id) {
        if let Some(printer) = &self.printer {
            printer.enter(span);
        }
    }

    fn exit(&self, span: &tracing::span::Id) {
        if let Some(printer) = &self.printer {
            printer.exit(span);
        }
    }

    fn clone_span(&self, span: &tracing::span::Id) -> tracing::span::Id {
        match &self.printer {
            Some(printer) => printer.clone_span(span),
            None => span.clone(),
        }
    }

    fn try_close(&self, span: tracing::span::Id) -> bool {
        self.printer.as_ref().is_some_and(|p| p.try_close(span))
    }
}
