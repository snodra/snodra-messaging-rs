//! The orders service: `DATABASE_URL` (PostgreSQL), the `SNODRA_*` settings of the library and of
//! the SQL store (`SNODRA_STORE_SQL_*`). The in-memory event store keeps the example to one
//! database; a real service adds an event-store add-on (Kafka, RabbitMQ Streams).

use std::sync::Arc;

use snodra_messaging::event_store::memory::MemoryEventStore;
use snodra_messaging::{Config, SnodraMessaging};
use snodra_messaging_sqlx::{SqlxSettings, SqlxStore};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is not set")?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await?;
    sqlx::raw_sql(orders_sql::SCHEMA).execute(&pool).await?;
    let store = SqlxStore::postgres(pool).with_settings(SqlxSettings::from_env()?);
    let builder = SnodraMessaging::builder(Config::from_env()?)
        .await?
        .event_store(Arc::new(MemoryEventStore::default()));
    let app = orders_sql::handlers(builder, store).start().await?;
    tokio::signal::ctrl_c().await?;
    app.shutdown().await;
    Ok(())
}
