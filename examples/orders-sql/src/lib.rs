//! A small orders service on PostgreSQL with the SQL message store (snodra-messaging-sqlx), the
//! Rust twin of snodra-messaging's `examples/orders-sql`:
//!
//! - `PlaceOrder` (a command, through the hub) writes the order row and appends `OrderPlaced` to
//!   the outbox **in one transaction** of the store: both commit, or neither (a command that fails
//!   after its writes leaves nothing behind);
//! - the outbox relay publishes `OrderPlaced` to the event store;
//! - the `OrderPlaced` handler confirms the order in the transaction of the event it handles,
//!   which also records its processed-event marker (`processor:default:<eventId>`): a redelivery
//!   is skipped.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use snodra_messaging::store::MessageStore;
use snodra_messaging::{Command, Event, MessagingError, SnodraMessagingBuilder, message_type};
use snodra_messaging_sqlx::{SqlxStore, SqlxTxExt};
use sqlx::Postgres;

/// The service's own table, beside the library's.
pub const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS orders (\
    id VARCHAR(64) NOT NULL PRIMARY KEY, \
    item TEXT NOT NULL, \
    status VARCHAR(16) NOT NULL)";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaceOrder {
    pub id: String,
    pub item: String,
    /// The command fails after its writes (to show they roll back).
    #[serde(default)]
    pub fail: bool,
}
message_type!(PlaceOrder, "orders.PlaceOrder");
impl Command for PlaceOrder {
    type Reply = String;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderPlaced {
    pub id: String,
}
message_type!(OrderPlaced, "orders.OrderPlaced");
impl Event for OrderPlaced {}

fn storage(e: sqlx::Error) -> MessagingError {
    MessagingError::Storage(e.to_string())
}

/// The service's handlers on `builder`, whose store is `store`.
pub fn handlers(builder: SnodraMessagingBuilder, store: SqlxStore) -> SnodraMessagingBuilder {
    let events = builder.events();
    let txs = store.tx();
    builder
        .store(Arc::new(store))
        .command::<PlaceOrder, _, _>(move |order, _ctx| {
            let (events, txs) = (events.clone(), Arc::clone(&txs));
            async move {
                let mut tx = txs.begin().await?;
                let written = async {
                    sqlx::query("INSERT INTO orders (id, item, status) VALUES ($1, $2, 'PLACED')")
                        .bind(&order.id)
                        .bind(&order.item)
                        .execute(tx.connection::<Postgres>())
                        .await
                        .map_err(storage)?;
                    events
                        .publish_in(
                            &OrderPlaced {
                                id: order.id.clone(),
                            },
                            Some(&order.id),
                            &mut tx,
                        )
                        .await?;
                    if order.fail {
                        return Err(MessagingError::InvalidMessage(format!(
                            "order {} cannot be placed",
                            order.id
                        )));
                    }
                    Ok(())
                }
                .await;
                match written {
                    Ok(()) => {
                        txs.commit(&mut tx).await?;
                        Ok(order.id)
                    }
                    Err(e) => {
                        txs.abort(&mut tx).await;
                        Err(e)
                    }
                }
            }
        })
        .event::<OrderPlaced, _, _>(|placed, ctx| async move {
            let mut tx = ctx.tx.lock().await;
            sqlx::query("UPDATE orders SET status = 'CONFIRMED' WHERE id = $1")
                .bind(&placed.id)
                .execute(tx.connection::<Postgres>())
                .await
                .map_err(storage)?;
            Ok(())
        })
}
