//! `PlaceOrder` through the hub (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`): the order row and
//! the outbox row in one transaction, the relay publishes `OrderPlaced`, the handler confirms the
//! order, and its marker `processor:default:<eventId>` is in `snodra_processed_events`; a command
//! failing after its writes leaves neither row.

use std::sync::Arc;
use std::time::Duration;

use orders_sql::PlaceOrder;
use snodra_messaging::event_store::memory::MemoryEventStore;
use snodra_messaging::{Config, MessagingError, SendOptions, SnodraMessaging};
use snodra_messaging_sqlx::SqlxStore;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

async fn tcp(addr: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while tokio::net::TcpStream::connect(addr).await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{addr} never answered"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn count(pool: &PgPool, sql: &'static str, id: &str) -> i64 {
    sqlx::query(sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_order_is_placed_published_and_confirmed_once() {
    let image = std::env::var("SNODRA_SERVER_IMAGE").unwrap_or_else(|_| "snodra-server:dev".into());
    let (name, tag) = image.rsplit_once(':').unwrap_or((&image, "latest"));
    let hub = GenericImage::new(name, tag)
        .with_exposed_port(7000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("hub listening"))
        .with_env_var("SNODRA_SERVER_OTLP_ENDPOINT", "")
        .start()
        .await
        .expect("hub container (build it or set SNODRA_SERVER_IMAGE)");
    let hub_port = hub.get_host_port_ipv4(7000.tcp()).await.unwrap();
    tcp(&format!("127.0.0.1:{hub_port}")).await;
    let pg = Postgres::default().with_tag("17.7").start().await.unwrap();
    let pg_port = pg.get_host_port_ipv4(5432.tcp()).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");
    tcp(&format!("127.0.0.1:{pg_port}")).await;
    let pool = loop {
        match PgPoolOptions::new().max_connections(8).connect(&url).await {
            Ok(pool) => break pool,
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    };
    sqlx::raw_sql(orders_sql::SCHEMA)
        .execute(&pool)
        .await
        .unwrap();

    let events = MemoryEventStore::default();
    let config = Config::new("orders-sql", &format!("static://127.0.0.1:{hub_port}"));
    let builder = SnodraMessaging::builder(config.clone())
        .await
        .unwrap()
        .event_store(Arc::new(events.clone()));
    let app = orders_sql::handlers(builder, SqlxStore::postgres(pool.clone()))
        .start()
        .await
        .unwrap();
    assert!(app.await_hub(Duration::from_secs(10)).await);

    let placed = app
        .commands()
        .send_command(
            &PlaceOrder {
                id: "o-1".into(),
                item: "book".into(),
                fail: false,
            },
            SendOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(placed, "o-1");
    eventually("the order confirmed by its event", async || {
        sqlx::query("SELECT status FROM orders WHERE id = 'o-1'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<String, _>(0)
            == "CONFIRMED"
    })
    .await;
    let published = events.events(&config.events_source);
    assert_eq!(published.len(), 1, "published once");
    let event_id = published[0]
        .headers
        .get("id")
        .cloned()
        .expect("the event id");
    // The relay marks the row sent after the event store took the event, so the handler can
    // confirm the order first.
    eventually("the outbox row, sent", async || {
        count(
            &pool,
            "SELECT COUNT(*) FROM snodra_outbox WHERE id = $1 AND sent_at IS NOT NULL",
            &event_id,
        )
        .await
            == 1
    })
    .await;
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM snodra_processed_events WHERE id = $1",
            &format!("processor:default:{event_id}")
        )
        .await,
        1,
        "the handler's marker"
    );

    let refused = app
        .commands()
        .send_command(
            &PlaceOrder {
                id: "o-2".into(),
                item: "lamp".into(),
                fail: true,
            },
            SendOptions::default(),
        )
        .await;
    assert!(
        matches!(
            refused,
            Err(MessagingError::RemoteHandler { .. }) | Err(MessagingError::InvalidMessage(_))
        ),
        "{refused:?}"
    );
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM orders WHERE id = $1", "o-2").await,
        0,
        "no order row"
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM snodra_outbox WHERE event_key = $1",
            "o-2"
        )
        .await,
        0,
        "no outbox row"
    );
    app.shutdown().await;
}
