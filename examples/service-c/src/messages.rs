//! Wire types shared with the Kotlin `examples-api` (same names, same fields, same `@MessageType`s).

use chrono::{DateTime, Utc};
use kraken_messaging::{Command, Event, Query, StreamingQuery, message_type};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Shout {
    pub text: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Shouted {
    pub text: String,
}
message_type!(Shout, "examples.Shout");
impl Command for Shout {
    type Reply = Shouted;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Reverse {
    pub text: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Reversed {
    pub text: String,
}
message_type!(Reverse, "examples.Reverse");
impl Query for Reverse {
    type Reply = Reversed;
}

/// A shared query type (ADR 0017): service-b (Kotlin) owns it too, so a point query goes to one of
/// them and a scatter-gather asks both.
#[derive(Serialize, Deserialize, Debug)]
pub struct Inventory {
    pub item: String,
}
/// The answering service and instance (the container's host name), a fixed count per service and
/// how many Inventory queries the instance answered so far.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct InventoryLevel {
    pub service: String,
    pub instance: String,
    pub item: String,
    pub count: i64,
    pub answered: i64,
}
message_type!(Inventory, "examples.Inventory");
impl Query for Inventory {
    type Reply = InventoryLevel;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TextShouted {
    pub text: String,
    pub at: DateTime<Utc>,
}
message_type!(TextShouted, "examples.TextShouted");
impl Event for TextShouted {}

#[derive(Serialize, Deserialize, Debug)]
pub struct TextReversed {
    pub text: String,
    pub at: DateTime<Utc>,
}
message_type!(TextReversed, "examples.TextReversed");
impl Event for TextReversed {}

/// Published by service-b (Kotlin); C only listens.
#[derive(Serialize, Deserialize, Debug)]
pub struct GreetingDelivered {
    pub id: String,
    pub name: String,
    pub at: DateTime<Utc>,
}
message_type!(GreetingDelivered, "examples.GreetingDelivered");
impl Event for GreetingDelivered {}

/// A shared streaming query type (ADR 0017, P4b): service-b (Kotlin) owns it too, so a stream goes
/// to one instance of either. `fail_at > 0` makes the handler fail on reaching that row.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Rows {
    pub count: i32,
    #[serde(default)]
    pub fail_at: i32,
}
/// One element of a `Rows` stream: the answering service and instance (the container's host
/// name) and the row number `n` (1..=count).
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Row {
    pub service: String,
    pub instance: String,
    pub n: i32,
}
message_type!(Rows, "examples.Rows");
impl StreamingQuery for Rows {
    type Item = Row;
}
