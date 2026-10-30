//! Wire types shared with the Kotlin `examples-api` (same names, same fields, same `@MessageType`s).

use chrono::{DateTime, Utc};
use kraken_messaging::{Command, Event, Query, message_type};
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
