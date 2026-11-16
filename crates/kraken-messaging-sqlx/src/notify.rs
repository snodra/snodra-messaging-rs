//! The relay wake-up on PostgreSQL: `LISTEN kraken_outbox` on a connection of its own, and every
//! notification an append's commit delivers wakes the registered relays
//! ([`Notifications::register`]). The listening task runs while at least one relay is registered.
//!
//! The connection is a dedicated one (a pool of one, from the store pool's connect options),
//! never one of the store's pool. A failure (the database restarted, the network) is logged once
//! and retried with a backoff from [`MIN_BACKOFF`] to [`MAX_BACKOFF`]; the relays wake up whenever
//! listening starts again (an append may have committed meanwhile) and keep polling anyway. A
//! connection quiet for [`HEALTH_CHECK`] is checked with `SELECT 1` under [`NETWORK_TIMEOUT`], so a
//! half-open connection (a peer gone without closing it) is noticed and replaced.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use kraken_messaging::store::AppendListener;
use sqlx::postgres::{PgConnectOptions, PgListener, PgPool, PgPoolOptions};
use tokio_util::sync::CancellationToken;

/// The channel appends notify and relays listen on; the payload is empty.
pub(crate) const CHANNEL: &str = "kraken_outbox";
/// How long a quiet listening connection goes before it is checked.
const HEALTH_CHECK: Duration = Duration::from_secs(30);
/// The most the check, or the connect, waits for the server.
const NETWORK_TIMEOUT: Duration = Duration::from_secs(10);
const MIN_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

type Wake = Arc<dyn Fn() + Send + Sync>;

pub(crate) struct Notifications {
    options: Arc<PgConnectOptions>,
    state: Mutex<State>,
    listens: AtomicU64,
}

#[derive(Default)]
struct State {
    listeners: HashMap<u64, Wake>,
    next: u64,
    /// The running listening task's stop token (each task its own, so a restarted one never
    /// revives a stopping one).
    running: Option<CancellationToken>,
}

impl Notifications {
    pub(crate) fn new(pool: &PgPool) -> Arc<Notifications> {
        Arc::new(Notifications {
            options: pool.connect_options(),
            state: Mutex::new(State::default()),
            listens: AtomicU64::new(0),
        })
    }

    /// How many times `LISTEN` was established and its wake-up done.
    pub(crate) fn listens(&self) -> u64 {
        self.listens.load(Ordering::SeqCst)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Wakes `wake` after every append that commits, until the listener returned is dropped.
    /// Outside a Tokio runtime nothing listens (the relay polls).
    pub(crate) fn register(self: &Arc<Self>, wake: Wake) -> AppendListener {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return AppendListener::none();
        };
        let id = {
            let mut state = self.lock();
            let id = state.next;
            state.next += 1;
            state.listeners.insert(id, wake);
            if state.running.is_none() {
                let stop = CancellationToken::new();
                state.running = Some(stop.clone());
                runtime.spawn(listen(Arc::downgrade(self), stop));
            }
            id
        };
        let me = Arc::downgrade(self);
        AppendListener::new(move || {
            if let Some(me) = me.upgrade() {
                let mut state = me.lock();
                if state.listeners.remove(&id).is_some()
                    && state.listeners.is_empty()
                    && let Some(stop) = state.running.take()
                {
                    stop.cancel();
                }
            }
        })
    }

    fn wake_all(&self) {
        let listeners: Vec<Wake> = self.lock().listeners.values().cloned().collect();
        for wake in listeners {
            wake();
        }
    }

    /// A listener on a dedicated connection, listening.
    async fn connect(&self) -> Result<(PgPool, PgListener), sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .idle_timeout(None)
            .max_lifetime(None)
            .acquire_timeout(NETWORK_TIMEOUT)
            .connect_lazy_with((*self.options).clone());
        let mut listener = PgListener::connect_with(&pool).await?;
        listener.listen(CHANNEL).await?;
        Ok((pool, listener))
    }
}

/// The listening task: until `stop`, or until the store is gone.
async fn listen(me: Weak<Notifications>, stop: CancellationToken) {
    let mut backoff = MIN_BACKOFF;
    let mut failing = false;
    loop {
        let Some(n) = me.upgrade() else { return };
        let connected = tokio::select! {
            _ = stop.cancelled() => return,
            c = n.connect() => c,
        };
        let failure = match connected {
            Ok((pool, mut listener)) => {
                if failing {
                    tracing::info!("listening for outbox appends again");
                }
                failing = false;
                backoff = MIN_BACKOFF;
                n.wake_all(); // an append may have committed while nobody listened
                n.listens.fetch_add(1, Ordering::SeqCst);
                drop(n);
                let failure = receive(&me, &mut listener, &stop).await;
                drop(listener);
                pool.close().await;
                match failure {
                    None => return,
                    Some(e) => e,
                }
            }
            Err(e) => e,
        };
        if stop.is_cancelled() {
            return;
        }
        if !failing {
            tracing::warn!(
                "cannot listen for outbox appends (the relay polls meanwhile); retrying: {failure}"
            );
        }
        failing = true;
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Wakes the relays on every notification until `stop` (`None`) or a failure (`Some`).
async fn receive(
    me: &Weak<Notifications>,
    listener: &mut PgListener,
    stop: &CancellationToken,
) -> Option<sqlx::Error> {
    loop {
        // Reading a notification is cancel-safe: a message is consumed only once whole.
        let received = tokio::select! {
            _ = stop.cancelled() => return None,
            r = tokio::time::timeout(HEALTH_CHECK, listener.try_recv()) => r,
        };
        let n = me.upgrade()?;
        match received {
            // A notification, or the connection lost and listened again (eagerly): an append
            // may have committed in between.
            Ok(Ok(_)) => n.wake_all(),
            Ok(Err(e)) => return Some(e),
            // Quiet for a while: is the connection still there?
            Err(_) => {
                match tokio::time::timeout(
                    NETWORK_TIMEOUT,
                    sqlx::query("SELECT 1").execute(&mut *listener),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Some(e),
                    Err(_) => {
                        return Some(sqlx::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "the listening connection did not answer its health check",
                        )));
                    }
                }
            }
        }
    }
}
