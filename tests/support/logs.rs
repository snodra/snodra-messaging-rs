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
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
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
