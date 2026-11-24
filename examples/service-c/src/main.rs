//! Service C: the Rust participant of the examples sandbox. Owns `examples.Shout` (command) and
//! `examples.Reverse` (query), publishes `examples.TextShouted` / `examples.TextReversed` while
//! handling them, and logs `examples.GreetingDelivered` from service-b. Shares the query
//! `examples.Inventory`, the streaming query `examples.Rows` and the subscription query
//! `examples.WatchCounter` (and `examples.OpenSubscriptions`) with service-b. Owns the
//! command `examples.IncrementCounter`, which drives its counters. Prints every shouted text as a
//! banner on the sandbox's fake printer through a saga (`banner.rs`), with its own
//! idempotent printer adapters, the printer's topic as an external source and a small HTTP side for
//! the printer's webhook and the banner outcomes (`http.rs`), which also sends service-b's
//! `examples.CountGreetings` through the hub. Runs a
//! named event processor, `audit` (`audit.rs`), whose failures become dead letters the
//! example's admin endpoints retry and discard.

mod audit;
mod banner;
mod handlers;
mod http;
mod messages;
mod printer;
mod telemetry;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use chrono::Utc;
use messages::{
    BannerFailed, BannerPrinted, Counter, GreetingDelivered, IncrementCounter, Inventory,
    OpenSubscriptions, PollBannerJob, PrintBanner, Reverse, Reversed, Rows, Shout, Shouted,
    SubscriptionCount, TextReversed, TextShouted, WatchCounter,
};
use snodra_messaging::external::IdempotencyKey;
use snodra_messaging::{Config, MessagingError, SnodraMessaging};
use snodra_messaging_kafka::{ExternalSource, KafkaConfig, KafkaEventStore, KafkaExternalSource};
use snodra_messaging_mongo::MongoStore;

/// The banner saga's settings, from the environment (compose.local.yaml sets them in-network):
/// `SERVER_PORT` (8094), `PRINTER_URL` (http://localhost:8093), `PRINTER_WEBHOOK_SECRET` (the
/// sandbox's dev-only value, shared with the fake printer; not a secret), `PRINTER_REPLY_WEBHOOK`
/// (where the printer posts, default this service's own `/printer/webhook`) and
/// `PRINTER_BANNER_TOPIC` (the printer's topic for service-c, `printer.banner.events`).
struct BannerSettings {
    port: u16,
    printer_url: String,
    secret: String,
    reply: printer::ReplyTo,
}

impl BannerSettings {
    fn from_env() -> Result<BannerSettings, MessagingError> {
        let var = |name: &str, default: &str| {
            std::env::var(name)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        let port: u16 = var("SERVER_PORT", "8094").parse().map_err(|e| {
            MessagingError::InvalidMessage(format!("SERVER_PORT is not a port: {e}"))
        })?;
        Ok(BannerSettings {
            port,
            printer_url: var("PRINTER_URL", "http://localhost:8093"),
            secret: var("PRINTER_WEBHOOK_SECRET", "dev-only-printer-webhook-secret"),
            reply: printer::ReplyTo {
                webhook: var(
                    "PRINTER_REPLY_WEBHOOK",
                    &format!("http://localhost:{port}/printer/webhook"),
                ),
                topic: var("PRINTER_BANNER_TOPIC", "printer.banner.events"),
            },
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), MessagingError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Config::from_env()?;
    // Before the builder: the library records on the global meter provider as it is then.
    let otlp_endpoint = std::env::var("SNODRA_OTLP_ENDPOINT")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let _metrics = telemetry::init_metrics(&config, otlp_endpoint.as_deref())?;
    let banners = BannerSettings::from_env()?;
    let audit = audit::Audit::default();
    let outcomes = banner::BannerOutcomes::default();
    let (printed_outcomes, failed_outcomes) = (outcomes.clone(), outcomes.clone());
    let print_printer = printer::Printer::new(&banners.printer_url, banners.reply.clone())?;
    let poll_printer = print_printer.clone();
    let service = config.service_name.clone();
    // The container id under Docker, like service-b's InventoryLevel.instance.
    let instance = std::env::var("HOSTNAME").unwrap_or_else(|_| "local".to_string());
    let answered = Arc::new(AtomicI64::new(0));
    let (rows_service, rows_instance) = (service.clone(), instance.clone());
    let counters = Arc::new(handlers::Counters::new(&service, &instance));
    let (watch_counters, increment_counters) = (counters.clone(), counters);
    let (count_service, count_instance) = (service.clone(), instance.clone());
    let increment_service = service.clone();
    // The message store: the service's own MongoDB database (a replica set); `start` creates its
    // indexes with the retentions of the config.
    let mongo = mongodb::Client::with_uri_str(&required_env("SNODRA_MONGODB_URI")?)
        .await
        .map_err(|e| MessagingError::Storage(e.to_string()))?;
    let db = mongo.default_database().ok_or_else(|| {
        MessagingError::InvalidMessage("SNODRA_MONGODB_URI must name a database".into())
    })?;
    let store = MongoStore::new(mongo, db);
    // The event store: Kafka (`SNODRA_KAFKA_BOOTSTRAP`), and the printer's partner topic on it.
    let kafka = KafkaConfig::from_env();
    let printer_source = KafkaExternalSource::new(
        printer::SOURCE,
        ExternalSource::new(&banners.reply.topic),
        &service,
        &kafka.bootstrap_servers,
    )?;
    let events = KafkaEventStore::new(kafka)?;
    let builder = SnodraMessaging::builder(config)
        .await?
        .store(Arc::new(store))
        .event_store(Arc::new(events));
    let emitter = builder.emitter();
    let count_emitter = emitter.clone();
    let shout_events = builder.events();
    let reverse_events = builder.events();
    let poll_events = builder.events();
    // The named processor `audit`: its own consumer, group and dead letters.
    let app = audit::register(builder, &audit)
        .command::<Shout, _, _>(move |cmd, ctx| {
            let events = shout_events.clone();
            async move {
                let text = handlers::shout(&cmd.text);
                events
                    .publish(
                        &TextShouted {
                            text: text.clone(),
                            at: Utc::now(),
                        },
                        Some(&cmd.text),
                    )
                    .await?;
                tracing::info!(
                    "C: handled Shout text={} -> {} correlationId={}",
                    cmd.text,
                    text,
                    ctx.correlation_id
                );
                Ok(Shouted { text })
            }
        })
        .query::<Reverse, _, _>(move |q, _ctx| {
            let events = reverse_events.clone();
            async move {
                let text = handlers::reverse(&q.text);
                events
                    .publish(
                        &TextReversed {
                            text: text.clone(),
                            at: Utc::now(),
                        },
                        Some(&q.text),
                    )
                    .await?;
                tracing::info!("C: answered Reverse text={} -> {}", q.text, text);
                Ok(Reversed { text })
            }
        })
        .shared_query::<Inventory, _, _>(move |q, _ctx| {
            let n = answered.fetch_add(1, Ordering::Relaxed) + 1;
            let level = handlers::inventory(&service, &instance, &q.item, n);
            async move {
                tracing::info!("C: answered Inventory item={} answered={}", level.item, n);
                Ok(level)
            }
        })
        .shared_streaming_query::<Rows, _, _, _>(move |q, _ctx| {
            let (service, instance) = (rows_service.clone(), rows_instance.clone());
            async move {
                tracing::info!("C: streaming Rows count={} failAt={}", q.count, q.fail_at);
                Ok(handlers::rows(&service, &instance, q.count, q.fail_at))
            }
        })
        .shared_subscription_query::<WatchCounter, _, _>(move |q, _ctx| {
            let counter = watch_counters.current(&q.name);
            async move {
                tracing::info!(
                    "C: subscribed WatchCounter name={} value={}",
                    counter.name,
                    counter.value
                );
                Ok(counter)
            }
        })
        .command::<IncrementCounter, _, _>(move |cmd, _ctx| {
            let emitter = emitter.clone();
            let counters = increment_counters.clone();
            let own = increment_service.clone();
            async move {
                handlers::check_addressee(&own, &cmd.service)?;
                let counter: Counter = counters.increment(&cmd.name, |changed| {
                    emitter.emit::<WatchCounter>(|q| q.name == changed.name, changed)
                });
                tracing::info!(
                    "C: incremented counter name={} -> {}",
                    counter.name,
                    counter.value
                );
                Ok(counter)
            }
        })
        .shared_query::<OpenSubscriptions, _, _>(move |_q, _ctx| {
            let count: SubscriptionCount = handlers::subscription_count(
                &count_service,
                &count_instance,
                count_emitter.open_subscriptions(),
            );
            async move { Ok(count) }
        })
        .event::<GreetingDelivered, _, _>(|e, _ctx| async move {
            tracing::info!("C: received GreetingDelivered id={} name={}", e.id, e.name);
            Ok(())
        })
        // The banner saga and its edge.
        .saga(banner::SAGA, banner::banner_saga)
        // Submits the banner; the command's `idempotency-key` (`<sagaId>:<seq>`) goes to the
        // printer too, which de-duplicates by it.
        .idempotent_command::<PrintBanner, _, _>(
            IdempotencyKey::default(),
            false,
            move |cmd, ctx| {
                let printer = print_printer.clone();
                async move {
                    let key = ctx.headers.get("idempotency-key").cloned().ok_or_else(|| {
                        MessagingError::InvalidMessage(format!(
                            "PrintBanner {} arrived without an idempotency-key header",
                            cmd.job_id
                        ))
                    })?;
                    printer
                        .submit(&printer.job(&cmd.job_id, &cmd.text), &key)
                        .await?;
                    tracing::info!("C: submitted banner {} to the printer", cmd.job_id);
                    Ok(())
                }
            },
        )
        // Asks the printer about the job and publishes BannerPrinted when it is done: one call per
        // attempt number, however often the relay sends it.
        .idempotent_command::<PollBannerJob, _, _>(
            IdempotencyKey::From(|cmd, _| format!("poll:{}:{}", cmd.job_id, cmd.attempt)),
            false,
            move |cmd, _ctx| {
                let (printer, events) = (poll_printer.clone(), poll_events.clone());
                async move { printer::poll(&printer, &events, &cmd).await }
            },
        )
        .external_source(printer::SOURCE, printer_source, printer::translate)
        .event::<BannerPrinted, _, _>(move |e, _ctx| {
            printed_outcomes.printed(&e);
            async move {
                tracing::info!("C: banner {} printed", e.job_id);
                Ok(())
            }
        })
        .event::<BannerFailed, _, _>(move |e, _ctx| {
            failed_outcomes.failed(&e);
            async move {
                tracing::info!("C: banner {} failed: {}", e.job_id, e.reason);
                Ok(())
            }
        })
        .start()
        .await?;

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", banners.port))
        .await
        .map_err(|e| MessagingError::Transport(format!("HTTP port {}: {e}", banners.port)))?;
    let router = http::router(http::Http {
        ingress: app.ingress(),
        secret: banners.secret,
        outcomes,
        sender: app.sender(),
    })
    .merge(audit::router(audit::AuditHttp {
        audit,
        events: app.events().clone(),
        dead_letters: app.dead_letters(),
    }));
    let (stop_http, http_stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let serve = axum::serve(listener, router).with_graceful_shutdown(async {
            let _ = http_stopped.await;
        });
        if let Err(e) = serve.await {
            tracing::error!("C: HTTP server failed: {e}");
        }
    });
    tracing::info!("C: HTTP on port {}", banners.port);

    if !app.await_hub(Duration::from_secs(30)).await {
        tracing::warn!("hub not confirmed after 30 s; still retrying in the background");
    }
    shutdown_signal().await;
    tracing::info!("C: shutting down");
    let _ = stop_http.send(());
    let _ = server.await;
    app.shutdown().await;
    Ok(())
}

/// The value of the environment variable `name`; `InvalidMessage` when it is unset or blank.
fn required_env(name: &str) -> Result<String, MessagingError> {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| MessagingError::InvalidMessage(format!("{name} is required")))
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
