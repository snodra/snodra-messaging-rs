//! End to end with two real P4b hubs (`kraken-hub:p4b-hub`, ADR 0017) and the P3 pool
//! configuration (every instance on both hubs): one service streams 1 000 rows under a credit
//! window of 8 and another collects them all in order (the stream pins one instance and one hub,
//! which count it in flight until the end); a stream dropped mid-way is cancelled at the hub and
//! at its handler (the counts fall back to 0); `max_results` and the idle timeout end a stream
//! from the hub.
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use kraken_messaging::hub::StreamOptions;
use kraken_messaging::{Config, KrakenMessaging, MessagingError, StreamingQuery, message_type};
use serde::{Deserialize, Serialize};
use support::containers::{HubContainer, HubImage, HubOptions, free_port, hub_with, mongo};
use tokio_stream::StreamExt;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

/// `rows` rows, each after `delay_ms` (the first at once).
#[derive(Serialize, Deserialize)]
struct Export {
    rows: u32,
    delay_ms: u64,
}
#[derive(Serialize, Deserialize, Debug)]
struct Row {
    i: u32,
    instance: String,
}
message_type!(Export, "e2e.Export");
impl StreamingQuery for Export {
    type Item = Row;
}

const IDS: [&str; 2] = ["e2e-p4b-1", "e2e-p4b-2"];

async fn two_hubs() -> (HubContainer, HubContainer) {
    let start = |id: &'static str| {
        hub_with(
            HubOptions::new(HubImage::P4b)
                .port(free_port())
                .env("KRAKEN_HUB_ID", id),
        )
    };
    tokio::join!(start(IDS[0]), start(IDS[1]))
}

fn config(service: &str, hubs: &str, mongo: &str) -> Config {
    let mut c = Config::new(service, hubs, "127.0.0.1:1", mongo);
    c.outbox_enabled = false;
    c.consumer_enabled = false;
    c.reconnect_backoff_min = Duration::from_millis(100);
    c.max_concurrent_handlers = 8;
    c
}

/// What the exporter's handler saw: rows pulled from its streams, streams dropped.
#[derive(Default)]
struct Probe {
    pulled: AtomicU32,
    dropped: AtomicU32,
}

/// Counts what is pulled from the handler's stream and notices its drop.
struct Watched {
    inner: futures::stream::BoxStream<'static, Result<Row, MessagingError>>,
    probe: Arc<Probe>,
}

impl futures::Stream for Watched {
    type Item = Result<Row, MessagingError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let polled = self.inner.as_mut().poll_next(cx);
        if let std::task::Poll::Ready(Some(_)) = &polled {
            self.probe.pulled.fetch_add(1, Ordering::SeqCst);
        }
        polled
    }
}

impl Drop for Watched {
    fn drop(&mut self) {
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// The owner of `e2e.Export`: streams the rows, each naming this instance.
async fn exporter(cfg: Config, probe: Arc<Probe>) -> KrakenMessaging {
    KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .streaming_query::<Export, _, _, _>(move |q, ctx| {
            let probe = Arc::clone(&probe);
            async move {
                let instance = ctx.message_id.clone();
                let rows = tokio_stream::iter(0..q.rows).then(move |i| {
                    let instance = instance.clone();
                    async move {
                        if i > 0 {
                            tokio::time::sleep(Duration::from_millis(q.delay_ms)).await;
                        }
                        Ok(Row { i, instance })
                    }
                });
                Ok(Watched {
                    inner: Box::pin(rows),
                    probe,
                })
            }
        })
        .start()
        .await
        .unwrap()
}

async fn caller(cfg: Config) -> KrakenMessaging {
    KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .start()
        .await
        .unwrap()
}

async fn get(hub: &HubContainer, path: &str) -> serde_json::Value {
    reqwest::get(format!("{}{path}", hub.http))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// `(inFlightAsSender, inFlightAsHandler)` of `service`'s instance on `hub` (0 while unlisted).
async fn in_flight(hub: &HubContainer, service: &str) -> (u64, u64) {
    let t = get(hub, "/topology").await;
    let instance = t["services"]
        .as_array()
        .and_then(|s| s.iter().find(|s| s["serviceName"] == service))
        .map(|s| s["instances"][0].clone())
        .unwrap_or_default();
    (
        instance["inFlightAsSender"].as_u64().unwrap_or(0),
        instance["inFlightAsHandler"].as_u64().unwrap_or(0),
    )
}

/// Polls `check` every 50 ms for at most 10 s.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_thousand_rows_stream_in_order_under_a_window_of_eight_and_a_drop_cancels_the_handler() {
    require_docker!();
    let ((h1, h2), db) = tokio::join!(two_hubs(), mongo());
    let hubs = format!("{},{}", h1.address, h2.address);
    let probe = Arc::new(Probe::default());
    let exporter = exporter(
        config("e2e-exporter", &hubs, &db.uri_for("exporter")),
        Arc::clone(&probe),
    )
    .await;
    let c = caller(config("e2e-collector", &hubs, &db.uri_for("collector"))).await;
    for app in [&exporter, &c] {
        assert!(app.await_all_hubs(Duration::from_secs(15)).await);
    }

    // 1 000 rows, credit 8: all of them, in order, from one handler.
    let mut stream = c
        .queries()
        .stream(
            &Export {
                rows: 1_000,
                delay_ms: 0,
            },
            StreamOptions {
                initial_credit: 8,
                ..StreamOptions::default()
            },
        )
        .await
        .unwrap();
    let mut rows = Vec::new();
    let mut handler_ids = std::collections::BTreeSet::new();
    while let Some(item) = stream.next().await {
        let row = item.unwrap();
        rows.push(row.i);
        handler_ids.insert(row.instance);
    }
    assert_eq!(rows, (0..1_000).collect::<Vec<_>>());
    assert_eq!(handler_ids.len(), 1, "one handler request, one instance");
    assert!(handler_ids.iter().next().unwrap().ends_with("#1"));
    drop(stream);
    assert_eq!(probe.pulled.load(Ordering::SeqCst), 1_000);
    eventually("the handler's stream dropped after the end", async || {
        probe.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    for hub in [&h1, &h2] {
        assert_eq!(in_flight(hub, "e2e-exporter").await.1, 0);
        assert_eq!(in_flight(hub, "e2e-collector").await.0, 0);
    }

    // A slow export dropped mid-way: the hub carrying it counts it until the drop, then the
    // handler is cancelled on that hub and the counts fall back.
    let mut stream = c
        .queries()
        .stream(
            &Export {
                rows: 1_000,
                delay_ms: 50,
            },
            StreamOptions {
                initial_credit: 8,
                ..StreamOptions::default()
            },
        )
        .await
        .unwrap();
    for i in 0..5 {
        assert_eq!(stream.next().await.unwrap().unwrap().i, i);
    }
    let carrier = if in_flight(&h1, "e2e-collector").await.0 > 0 {
        &h1
    } else {
        &h2
    };
    assert_eq!(in_flight(carrier, "e2e-collector").await, (1, 0));
    assert_eq!(in_flight(carrier, "e2e-exporter").await, (0, 1));
    drop(stream);
    eventually("the handler cancelled", async || {
        probe.dropped.load(Ordering::SeqCst) == 2
    })
    .await;
    eventually("nothing in flight on the carrier", async || {
        in_flight(carrier, "e2e-collector").await == (0, 0)
            && in_flight(carrier, "e2e-exporter").await == (0, 0)
    })
    .await;
    let pulled = probe.pulled.load(Ordering::SeqCst);
    assert!(
        (1_005..=1_016).contains(&pulled),
        "the handler never ran more than a window ahead: {pulled}"
    );

    // max_results ends a stream from the hub (the handler is cancelled); the idle timeout too.
    let stream = c
        .queries()
        .stream(
            &Export {
                rows: 1_000,
                delay_ms: 0,
            },
            StreamOptions {
                max_results: 3,
                ..StreamOptions::default()
            },
        )
        .await
        .unwrap();
    let got: Vec<u32> = stream.map(|r| r.unwrap().i).collect().await;
    assert_eq!(got, [0, 1, 2]);
    eventually("the handler cancelled at max_results", async || {
        probe.dropped.load(Ordering::SeqCst) == 3
    })
    .await;
    let mut stream = c
        .queries()
        .stream(
            &Export {
                rows: 2,
                delay_ms: 30_000,
            },
            StreamOptions {
                timeout: Some(Duration::from_millis(500)),
                ..StreamOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().i, 0);
    let e = stream.next().await.unwrap().unwrap_err();
    assert!(matches!(e, MessagingError::Timeout(_)), "{e:?}");
    assert!(stream.next().await.is_none());
    drop(stream);
    eventually("the handler cancelled at the idle timeout", async || {
        probe.dropped.load(Ordering::SeqCst) == 4
    })
    .await;

    c.shutdown().await;
    exporter.shutdown().await;
}
