//! Saga registration on the builder (S2-R1, S2-R2): the startup checks, which refuse before any
//! connection is made, and the wire shape of `SagaCommandFailed`.

use serde::{Deserialize, Serialize};
use serde_json::json;
use snodra_messaging::saga::{FailureReason, SagaCommandFailed};
use snodra_messaging::{Config, Event, MessageType, SnodraMessaging, SnodraMessagingBuilder};
use snodra_messaging::{MessagingError, SagaDefinition, message_type};

#[derive(Default, Serialize, Deserialize)]
struct Payment {
    order_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    order_id: String,
}
message_type!(OrderPlaced, "t.OrderPlaced");
impl Event for OrderPlaced {}

fn order_id(e: &OrderPlaced) -> Option<String> {
    Some(e.order_id.clone())
}

/// A builder that has not touched the network (the clients connect lazily), with a MongoDB store
/// that is never reached.
async fn builder() -> SnodraMessagingBuilder {
    let config = Config::new("svc", "static://127.0.0.1:1");
    let client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/db")
        .await
        .unwrap();
    let db = client.default_database().unwrap();
    SnodraMessaging::builder(config)
        .await
        .unwrap()
        .store(std::sync::Arc::new(
            snodra_messaging_mongo::MongoStore::new(client, db),
        ))
}

/// The error `start()` refuses with: the checks run before indexes or connections.
async fn refused(builder: SnodraMessagingBuilder) -> String {
    match builder.start().await {
        Ok(_) => panic!("start() accepted a saga that must be refused"),
        Err(MessagingError::InvalidMessage(message)) => message,
        Err(other) => panic!("expected a definition error, got {other:?}"),
    }
}

fn payment(d: SagaDefinition<Payment>) -> SagaDefinition<Payment> {
    d.start_on::<OrderPlaced>("orderId", order_id, |s, e, _| {
        s.order_id = Some(e.order_id.clone());
        Ok(())
    })
}

#[tokio::test]
async fn a_saga_without_a_start_handler_is_refused_naming_the_saga() {
    let message = refused(builder().await.saga::<Payment>("payment", |d| {
        d.on::<OrderPlaced>("orderId", order_id, |_, _, _| Ok(()))
    }))
    .await;
    assert!(message.contains("payment"), "{message}");
    assert!(message.contains("no start handler"), "{message}");
}

#[tokio::test]
async fn two_sagas_with_one_name_are_refused() {
    let message = refused(
        builder()
            .await
            .saga::<Payment>("payment", payment)
            .saga::<Payment>("payment", payment),
    )
    .await;
    assert!(message.contains("payment"), "{message}");
    assert!(message.contains("twice"), "{message}");
}

/// Serialises its `n` as `a` but reads it as `b`: a stored state it cannot read back.
#[derive(Default, Serialize, Deserialize)]
struct Lossy {
    #[serde(rename(serialize = "a", deserialize = "b"))]
    n: u32,
}

#[tokio::test]
async fn a_state_that_does_not_round_trip_is_refused_naming_the_saga() {
    let message = refused(builder().await.saga::<Lossy>("lossy", |d| {
        d.start_on::<OrderPlaced>("orderId", order_id, |_, _, _| Ok(()))
    }))
    .await;
    assert!(message.contains("lossy"), "{message}");
    assert!(message.contains("round-trip"), "{message}");
}

#[tokio::test]
async fn a_state_that_is_not_a_json_object_is_refused() {
    #[derive(Default, Serialize, Deserialize)]
    struct Counter(u32);
    let message = refused(builder().await.saga::<Counter>("counter", |d| {
        d.start_on::<OrderPlaced>("orderId", order_id, |_, _, _| Ok(()))
    }))
    .await;
    assert!(message.contains("counter"), "{message}");
    assert!(message.contains("JSON object"), "{message}");
}

#[tokio::test]
async fn ending_without_a_handler_before_it_is_refused() {
    let message = refused(
        builder()
            .await
            .saga::<Payment>("payment", |d| payment(d.ending())),
    )
    .await;
    assert!(message.contains("payment"), "{message}");
    assert!(message.contains("ending()"), "{message}");
}

#[tokio::test]
async fn two_deadline_handlers_of_one_name_are_refused() {
    let message = refused(builder().await.saga::<Payment>("payment", |d| {
        payment(d)
            .on_deadline::<()>("timeout", |_, _, _| Ok(()))
            .on_deadline::<()>("timeout", |_, _, _| Ok(()))
    }))
    .await;
    assert!(message.contains("payment.timeout"), "{message}");
}

#[test]
fn saga_command_failed_serialises_with_the_kotlin_field_names() {
    let failed = SagaCommandFailed {
        saga_id: "s-1".into(),
        command_type: "examples.PrintBanner".into(),
        idempotency_key: "s-1:1001".into(),
        reason: FailureReason::UnknownOutcome,
        message: "the printer hung up".into(),
    };
    assert_eq!(SagaCommandFailed::TYPE, "snodra.SagaCommandFailed");
    assert_eq!(
        serde_json::to_value(&failed).unwrap(),
        json!({
            "sagaId": "s-1",
            "commandType": "examples.PrintBanner",
            "idempotencyKey": "s-1:1001",
            "reason": "UNKNOWN_OUTCOME",
            "message": "the printer hung up",
        })
    );
}

#[test]
fn saga_command_failed_reads_what_kotlin_writes_and_every_reason_has_its_kotlin_name() {
    for (name, reason) in [
        ("NO_HANDLER", FailureReason::NoHandler),
        ("DENIED", FailureReason::Denied),
        ("INVALID", FailureReason::Invalid),
        ("FAILED", FailureReason::Failed),
        ("UNKNOWN_OUTCOME", FailureReason::UnknownOutcome),
        ("EXHAUSTED", FailureReason::Exhausted),
    ] {
        let text = format!(
            r#"{{"sagaId":"s","commandType":"t.C","idempotencyKey":"s:1","reason":"{name}","message":"m"}}"#
        );
        let parsed: SagaCommandFailed = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed.reason, reason, "{name}");
        assert_eq!(serde_json::to_string(&parsed).unwrap(), text, "{name}");
    }
}
