//! Wire types shared with the Kotlin `examples-api` (same names, same fields, same `@MessageType`s).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snodra_messaging::{Command, Event, Query, StreamingQuery, SubscriptionQuery, message_type};

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

/// service-b's query (Kotlin `examples-api`): how often a name was greeted. service-c sends it
/// through the hub from `GET /greetings/{name}/count`, the sandbox's one call from Rust to Kotlin.
#[derive(Serialize, Deserialize, Debug)]
pub struct CountGreetings {
    pub name: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct GreetingCount {
    pub name: String,
    pub count: i64,
}
message_type!(CountGreetings, "examples.CountGreetings");
impl Query for CountGreetings {
    type Reply = GreetingCount;
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

/// A shared subscription query type (ADR 0017, P4c): service-b (Kotlin) serves it too, so a
/// subscription gets the current value of one of them as its initial result and then the changes
/// of both.
#[derive(Serialize, Deserialize, Debug)]
pub struct WatchCounter {
    pub name: String,
}
/// The initial result of `WatchCounter`: the answering service and instance (the container's host
/// name) and the counter's current value.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Counter {
    pub service: String,
    pub instance: String,
    pub name: String,
    pub value: i64,
}
/// An update of `WatchCounter`: the counter `name` of `service` / `instance` now has `value`.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CounterChanged {
    pub service: String,
    pub instance: String,
    pub name: String,
    pub value: i64,
}
message_type!(WatchCounter, "examples.WatchCounter");
impl SubscriptionQuery for WatchCounter {
    type Initial = Counter;
    type Update = CounterChanged;
}

/// Owned by service-c only: increments its counter `name` and emits the change. `service` says
/// whose counter it is meant for; a handler that is not that service refuses it.
#[derive(Serialize, Deserialize, Debug)]
pub struct IncrementCounter {
    pub name: String,
    pub service: String,
}
message_type!(IncrementCounter, "examples.IncrementCounter");
impl Command for IncrementCounter {
    type Reply = Counter;
}

/// Shared with service-b: how many subscriptions the answering instance serves now, whatever
/// their type. `scope` is reserved (always "all").
#[derive(Serialize, Deserialize, Debug)]
pub struct OpenSubscriptions {
    #[serde(default)]
    pub scope: String,
}
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SubscriptionCount {
    pub service: String,
    pub instance: String,
    pub open: i64,
}
message_type!(OpenSubscriptions, "examples.OpenSubscriptions");
impl Query for OpenSubscriptions {
    type Reply = SubscriptionCount;
}

/// service-c's banner saga (sagas S2): print the shouted text as a banner on the fake printer.
/// Owned by service-c alone (its own idempotent adapter handles it; the saga relay delivers it
/// locally). `text` is what the printer prints and decides by (`webhook-*`, `topic-*`, `poll-*`,
/// `silent-*`, `crash-*`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PrintBanner {
    pub job_id: String,
    pub text: String,
}
message_type!(PrintBanner, "examples.PrintBanner");
impl Command for PrintBanner {
    type Reply = ();
}

/// The banner saga's poll of a `poll-*` job: attempt `attempt` (one printer call per attempt,
/// however often the relay sends it).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PollBannerJob {
    pub job_id: String,
    pub attempt: u32,
}
message_type!(PollBannerJob, "examples.PollBannerJob");
impl Command for PollBannerJob {
    type Reply = ();
}

/// The printer printed the banner (from the webhook, the printer's topic or a poll).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BannerPrinted {
    pub job_id: String,
    pub text: String,
}
message_type!(BannerPrinted, "examples.BannerPrinted");
impl Event for BannerPrinted {}

/// The banner saga gave up: `reason` is `TIMEOUT` or the `SagaCommandFailed` reason
/// (`UNKNOWN_OUTCOME`, `EXHAUSTED`, ...).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BannerFailed {
    pub job_id: String,
    pub text: String,
    pub reason: String,
}
message_type!(BannerFailed, "examples.BannerFailed");
impl Event for BannerFailed {}

/// An audit note, published by `POST /audit/notes` with `key` as its Kafka key (notes of one key
/// are handled in order) and handled by service-c's `audit` processor. Only service-c knows this
/// type (service-b's own `examples.AuditNote` is another one, handled by its own `audit`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShoutAudit {
    pub key: String,
    pub text: String,
    pub at: DateTime<Utc>,
}
message_type!(ShoutAudit, "examples.ShoutAudit");
impl Event for ShoutAudit {}

#[cfg(test)]
mod tests {
    use super::{Counter, CounterChanged, IncrementCounter, OpenSubscriptions, WatchCounter};
    use snodra_messaging::MessageType;

    #[test]
    fn the_counter_types_use_the_wire_names_and_fields_of_the_kotlin_examples_api() {
        assert_eq!(WatchCounter::TYPE, "examples.WatchCounter");
        assert_eq!(IncrementCounter::TYPE, "examples.IncrementCounter");
        assert_eq!(OpenSubscriptions::TYPE, "examples.OpenSubscriptions");

        let counter: Counter =
            serde_json::from_str(r#"{"service":"service-b","instance":"b1","name":"x","value":4}"#)
                .unwrap();
        assert_eq!(counter.value, 4);
        let changed = serde_json::to_value(CounterChanged {
            service: "service-c".into(),
            instance: "c1".into(),
            name: "x".into(),
            value: 5,
        })
        .unwrap();
        assert_eq!(
            changed,
            serde_json::json!({"service":"service-c","instance":"c1","name":"x","value":5})
        );
    }

    #[test]
    fn open_subscriptions_reads_with_or_without_its_reserved_scope() {
        let with: OpenSubscriptions = serde_json::from_str(r#"{"scope":"all"}"#).unwrap();
        let without: OpenSubscriptions = serde_json::from_str("{}").unwrap();
        assert_eq!(with.scope, "all");
        assert_eq!(without.scope, "");
    }
}
