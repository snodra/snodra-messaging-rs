//! Service C: the Rust participant of the examples sandbox. Owns `examples.Shout` (command) and
//! `examples.Reverse` (query), publishes `examples.TextShouted` / `examples.TextReversed` while
//! handling them, and logs `examples.GreetingDelivered` from service-b. No HTTP.

mod handlers;
mod messages;

use std::time::Duration;

use chrono::Utc;
use kraken_messaging::{Config, KrakenMessaging, MessagingError};
use messages::{GreetingDelivered, Reverse, Reversed, Shout, Shouted, TextReversed, TextShouted};

#[tokio::main]
async fn main() -> Result<(), MessagingError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let builder = KrakenMessaging::builder(Config::from_env()?).await?;
    let shout_events = builder.events();
    let reverse_events = builder.events();
    let app = builder
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
        .event::<GreetingDelivered, _, _>(|e, _ctx| async move {
            tracing::info!("C: received GreetingDelivered id={} name={}", e.id, e.name);
            Ok(())
        })
        .start()
        .await?;

    if !app.await_hub(Duration::from_secs(30)).await {
        tracing::warn!("hub not confirmed after 30 s; still retrying in the background");
    }
    shutdown_signal().await;
    tracing::info!("C: shutting down");
    app.shutdown().await;
    Ok(())
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
