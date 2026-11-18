//! The shared layout (snodra-messaging-jdbc's tables, values and meanings), on both databases:
//!
//! - **golden values**: fixed inputs written through the store are read back by plain SQL as the
//!   exact stored values the Kotlin store writes (hashes computed independently with `shasum -a
//!   256`, timestamps as the UTC instant at millisecond precision, JSON columns, enum names);
//! - **cross-reading**: rows written by plain SQL in the Kotlin layout (as `JdbcMessageStore`
//!   writes them, hash literals included) are read by the store, found by their hashes, and keep
//!   the uniqueness the hashes carry (a second live saga with that start key is refused).

#![cfg(all(feature = "postgres", feature = "mysql"))]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use snodra_messaging::event_store::ReplayRange;
use snodra_messaging::saga::store::{
    Association, DeadlineStatus, DispatchKind, SagaDocument, SagaFailureDocument,
};
use snodra_messaging::store::{
    DeadLetter, DeadLetterError, DeadLetterReason, ExternalCall, ExternalCallStatus, MessageStore,
    OutboxEntry, ProcessedMarker, Tx,
};
use support::{Kind, Raw, Server, texts};

/// 2026-11-14T10:00:00.123Z.
const AT: i64 = 1_794_650_400_123;
const AT_TEXT: &str = "2026-11-14T10:00:00.123";

fn bool_text(raw: &Raw, value: bool) -> &'static str {
    match (raw.kind(), value) {
        (Kind::Postgres, true) => "true",
        (Kind::Postgres, false) => "false",
        (Kind::MySql, true) => "1",
        (Kind::MySql, false) => "0",
    }
}

fn json(text: &Option<String>) -> serde_json::Value {
    serde_json::from_str(text.as_deref().expect("a JSON column")).expect("JSON")
}

async fn golden_values(kind: Kind) {
    let server = Server::of(kind).await;
    let (store, raw) = server.store().await;
    let txs = store.tx();
    let ts = |c: &str| raw.ts(c);

    // Outbox: JSON columns, the instant, the counters.
    store
        .outbox()
        .append_now(OutboxEntry {
            id: "e-1".into(),
            type_name: "t.Happened".into(),
            key: Some("k1".into()),
            headers: HashMap::from([("traceparent".into(), "00-abc-01".into())]),
            payload: br#"{"n":9223372036854775807,"s":"x"}"#.to_vec(),
            created_at_ms: AT,
            seq: 42,
            attempts: 0,
        })
        .await
        .unwrap();
    let row = raw
        .row(&format!(
            "SELECT id, type, event_key, CAST(headers AS {{text}}), CAST(payload AS {{text}}), {}, \
             CAST(seq AS {{text}}), CAST(attempts AS {{text}}), {}, {}, last_error, owner, {} FROM snodra_outbox",
            ts("created_at"),
            ts("sent_at"),
            ts("failed_at"),
            ts("lease_until")
        ))
        .await;
    assert_eq!(
        &row[..3],
        &texts(&[Some("e-1"), Some("t.Happened"), Some("k1")])[..]
    );
    assert_eq!(
        json(&row[3]),
        serde_json::json!({"traceparent": "00-abc-01"})
    );
    assert_eq!(
        json(&row[4]),
        serde_json::json!({"n": 9_223_372_036_854_775_807_i64, "s": "x"}),
        "the payload as written, its 64-bit integer exact"
    );
    assert_eq!(
        &row[5..],
        &texts(&[
            Some(AT_TEXT),
            Some("42"),
            Some("0"),
            None,
            None,
            None,
            None,
            None
        ])[..]
    );

    // A marker: the hash of its id, the position as a number when decimal.
    let mut tx = txs.begin().await.unwrap();
    for (id, position) in [
        ("processor:default:e-1", "41"),
        ("processor:default:e-2", "x7"),
    ] {
        assert!(
            store
                .processed_events()
                .record(
                    &mut tx,
                    &ProcessedMarker {
                        id: id.into(),
                        type_name: "t.Happened".into(),
                        processed_at_ms: AT,
                        processor: Some("default".into()),
                        source: Some("kraken.events".into()),
                        segment: Some("2".into()),
                        position: Some(position.into()),
                    },
                )
                .await
                .unwrap()
        );
    }
    txs.commit(&mut tx).await.unwrap();
    let rows = raw
        .rows(&format!(
            "SELECT id_hash, id, type, {}, processor, source, segment, position, CAST(position_num AS {{text}}) \
             FROM snodra_processed_events ORDER BY id",
            ts("processed_at")
        ))
        .await;
    assert_eq!(
        rows,
        vec![
            texts(&[
                Some("24ca69a5b6e5e53de85d1bd8f13651ed593283ecfe1638c5eb04d81bafade4ff"),
                Some("processor:default:e-1"),
                Some("t.Happened"),
                Some(AT_TEXT),
                Some("default"),
                Some("kraken.events"),
                Some("2"),
                Some("41"),
                Some("41"),
            ]),
            texts(&[
                Some(&rows[1][0].clone().unwrap()),
                Some("processor:default:e-2"),
                Some("t.Happened"),
                Some(AT_TEXT),
                Some("default"),
                Some("kraken.events"),
                Some("2"),
                Some("x7"),
                None,
            ]),
        ]
    );

    // A saga: the start key's hash, the live one, the association's hash (key, 0x00, value).
    let saga = SagaDocument {
        id: "s-1".into(),
        type_name: "conformance.Order".into(),
        state: r#"{"total":1.50,"b":1,"a":2}"#.into(),
        associations: vec![Association::new("orderId", "o-1")],
        start_key: Some("conformance.Order|orderId=o-1".into()),
        ended: false,
        ended_at_ms: None,
        version: 0,
        created_at_ms: AT,
        updated_at_ms: AT,
    };
    let sagas = store.sagas();
    let mut tx = txs.begin().await.unwrap();
    sagas.sagas.insert(&mut tx, &saga).await.unwrap();
    txs.commit(&mut tx).await.unwrap();
    let start_hash = "83dc8dac44387920c04b298f2fa8a5685646f6b823ef12b3aefc8265260eb350";
    assert_eq!(
        raw.row(&format!(
            "SELECT id, type, state, start_key, start_key_hash, live_start_key_hash, CAST(ended AS {{text}}), \
             CAST(version AS {{text}}), {}, {} FROM snodra_sagas",
            ts("created_at"),
            ts("updated_at")
        ))
        .await,
        texts(&[
            Some("s-1"),
            Some("conformance.Order"),
            Some(r#"{"total":1.50,"b":1,"a":2}"#),
            Some("conformance.Order|orderId=o-1"),
            Some(start_hash),
            Some(start_hash),
            Some(bool_text(&raw, false)),
            Some("0"),
            Some(AT_TEXT),
            Some(AT_TEXT),
        ]),
        "the state kept as written (text), the start key hashed"
    );
    assert_eq!(
        raw.row("SELECT saga_id, CAST(idx AS {text}), assoc_key, assoc_value, assoc_hash FROM snodra_saga_associations")
            .await,
        texts(&[
            Some("s-1"),
            Some("0"),
            Some("orderId"),
            Some("o-1"),
            Some("97939626adbe4b238abf09acf51868ad89b5022cd2617dfad599d472374fc598"),
        ])
    );
    let ended = SagaDocument {
        ended: true,
        ended_at_ms: Some(AT + 1),
        version: 1,
        ..saga.clone()
    };
    let mut tx = txs.begin().await.unwrap();
    assert!(sagas.sagas.update(&mut tx, &ended, 0).await.unwrap());
    txs.commit(&mut tx).await.unwrap();
    assert_eq!(
        raw.row("SELECT live_start_key_hash, CAST(ended AS {text}) FROM snodra_sagas")
            .await,
        texts(&[None, Some(bool_text(&raw, true))]),
        "an ended saga holds its start key no more"
    );

    // Saga dispatch, deadline and failure: enum names, JSON headers, text payloads.
    let mut tx = txs.begin().await.unwrap();
    sagas
        .dispatch
        .record(
            &mut tx,
            &[snodra_messaging::saga::store::SagaDispatchDocument {
                id: "s-1:1000".into(),
                saga_id: "s-1".into(),
                seq: 1000,
                kind: DispatchKind::Command,
                type_name: "t.Ship".into(),
                payload: r#"{"order":"o-1"}"#.into(),
                headers: HashMap::from([("correlation-id".into(), "c-1".into())]),
                routing_key: Some("o-1".into()),
                created_at_ms: AT,
                dispatched_at_ms: None,
                attempts: 0,
                next_attempt_at_ms: None,
                failed_at_ms: None,
                failure_reason: None,
                failure_message: None,
                notice_pending: None,
                notice_attempts: None,
                notice_next_at_ms: None,
            }],
        )
        .await
        .unwrap();
    sagas
        .deadlines
        .insert(
            &mut tx,
            &snodra_messaging::saga::store::DeadlineDocument {
                id: "d-1".into(),
                saga_type: "conformance.Order".into(),
                saga_id: "s-1".into(),
                name: "expire".into(),
                payload: Some("{\"why\":1}".into()),
                payload_type: Some("t.Why".into()),
                due_at_ms: AT,
                status: DeadlineStatus::Scheduled,
                claimed_by: None,
                claimed_until_ms: None,
                finished_at_ms: None,
                correlation_id: Some("c-1".into()),
            },
        )
        .await
        .unwrap();
    sagas
        .failures
        .save(
            &mut tx,
            &SagaFailureDocument {
                id: "conformance.Order:e-1".into(),
                saga_type: "conformance.Order".into(),
                event_id: "e-1".into(),
                event_type: "t.Happened".into(),
                payload: "{}".into(),
                headers: HashMap::new(),
                error: "boom".into(),
                attempts: 3,
                first_failed_at_ms: AT,
                last_failed_at_ms: AT,
            },
        )
        .await
        .unwrap();
    txs.commit(&mut tx).await.unwrap();
    sagas
        .dispatch
        .mark_failed(
            "s-1:1000",
            &snodra_messaging::store::DispatchFailure {
                attempts: Some(5),
                reason: snodra_messaging::saga::FailureReason::NoHandler,
                message: "nobody".into(),
                failed_at_ms: AT,
                notice_at_ms: Some(AT),
            },
        )
        .await
        .unwrap();
    let row = raw
        .row(&format!(
            "SELECT kind, type, payload, CAST(headers AS {{text}}), routing_key, failure_reason, failure_message, \
             CAST(notice_pending AS {{text}}), CAST(notice_attempts AS {{text}}), {}, CAST(attempts AS {{text}}) \
             FROM snodra_saga_dispatch",
            ts("failed_at")
        ))
        .await;
    assert_eq!(json(&row[3]), serde_json::json!({"correlation-id": "c-1"}));
    assert_eq!(
        [&row[..3], &row[4..]].concat(),
        texts(&[
            Some("COMMAND"),
            Some("t.Ship"),
            Some(r#"{"order":"o-1"}"#),
            Some("o-1"),
            Some("NO_HANDLER"),
            Some("nobody"),
            Some(bool_text(&raw, true)),
            Some("0"),
            Some(AT_TEXT),
            Some("5"),
        ])
    );
    assert_eq!(
        raw.row(&format!(
            "SELECT status, payload, payload_type, correlation_id, {} FROM snodra_deadlines",
            ts("due_at")
        ))
        .await,
        texts(&[
            Some("SCHEDULED"),
            Some("{\"why\":1}"),
            Some("t.Why"),
            Some("c-1"),
            Some(AT_TEXT)
        ])
    );
    let row = raw
        .row("SELECT id_hash, id, error, CAST(attempts AS {text}) FROM snodra_saga_failures")
        .await;
    assert_eq!(
        row,
        texts(&[
            Some("6f9e551486be7b7ee25b5aeb11d7107dc0664f792343f45adbdbb5b1fd27a89c"),
            Some("conformance.Order:e-1"),
            Some("boom"),
            Some("3"),
        ])
    );

    // A dispatch lease: the saga, its holder, until when.
    assert!(
        sagas
            .dispatch
            .claim_lease("s-1", "relay-1", AT, AT + 30_000)
            .await
            .unwrap()
    );
    assert_eq!(
        raw.row(&format!(
            "SELECT saga_id, owner, {} FROM snodra_saga_dispatch_leases",
            ts("lease_until")
        ))
        .await,
        texts(&[
            Some("s-1"),
            Some("relay-1"),
            Some("2026-11-14T10:00:30.123")
        ])
    );

    // External call, dead letter, processor control.
    store
        .external_calls()
        .insert(&ExternalCall {
            key: "ingress:partner:k-1".into(),
            status: ExternalCallStatus::InProgress,
            reply: None,
            error: None,
            attempts: 1,
            updated_at_ms: AT,
            lease_until_ms: Some(AT + 30_000),
            owner: Some("token-1".into()),
        })
        .await
        .unwrap();
    assert_eq!(
        raw.row(&format!(
            "SELECT id_hash, id, status, owner, {} FROM snodra_external_calls",
            ts("lease_until")
        ))
        .await,
        texts(&[
            Some("10c62a7a5159f00f866d73eb9528c0345f671927a474e7decee37e9830a7b677"),
            Some("ingress:partner:k-1"),
            Some("IN_PROGRESS"),
            Some("token-1"),
            Some("2026-11-14T10:00:30.123"),
        ])
    );
    store
        .dead_letters()
        .park(&DeadLetter {
            id: "default:kraken.events:2:41".into(),
            processor: "default".into(),
            source: "kraken.events".into(),
            segment: "2".into(),
            position: "41".into(),
            key: Some("key-9".into()),
            event_id: Some("e-1".into()),
            event_type: Some("t.Happened".into()),
            headers: [("type".to_string(), "t.Happened".to_string())].into(),
            payload: b"{\"n\":1}".to_vec(),
            reason: DeadLetterReason::HandlerFailed,
            error: DeadLetterError::new("Storage", "down"),
            blocked_by: None,
            attempts: 3,
            parked_at_ms: AT,
            last_attempt_at_ms: AT,
            expire_at_ms: 4_102_444_800_000,
            replay: false,
        })
        .await
        .unwrap();
    assert_eq!(
        raw.row("SELECT event_key, event_key_hash, CAST(position_num AS {text}), payload, reason, error_type, CAST(replay AS {text}) FROM snodra_dead_letters")
            .await,
        texts(&[
            Some("key-9"),
            Some("1a4d5bcb1c7d0500b0f6566d599bf7a5492d0fded14c7bed41a682815d6bec92"),
            Some("41"),
            Some("{\"n\":1}"),
            Some("HANDLER_FAILED"),
            Some("Storage"),
            Some(bool_text(&raw, false)),
        ])
    );
    let control = store.processor_control();
    control
        .set_replay(
            "default",
            &[ReplayRange {
                source: "kraken.events".into(),
                segment: "2".into(),
                from: "0".into(),
                until: "10".into(),
            }],
            AT,
        )
        .await
        .unwrap();
    assert!(matches!(
        control
            .lock("default", "op-1", AT + 60_000, AT)
            .await
            .unwrap(),
        snodra_messaging::store::Locked::Taken
    ));
    let row = raw
        .row(&format!(
            "SELECT CAST(replay AS {{text}}), op_id, op_kind, {}, {} FROM snodra_processor_control",
            ts("op_lease_until"),
            ts("op_prepared_at")
        ))
        .await;
    assert_eq!(
        json(&row[0]),
        serde_json::json!([{"source": "kraken.events", "segment": "2", "from": "0", "until": "10"}])
    );
    assert_eq!(
        &row[1..],
        &texts(&[
            Some("op-1"),
            Some("reset"),
            Some("2026-11-14T10:01:00.123"),
            Some(AT_TEXT)
        ])[..]
    );
}

/// Rows written by plain SQL as `JdbcMessageStore` writes them, read through the store.
async fn rows_in_the_kotlin_layout_are_read(kind: Kind) {
    let server = Server::of(kind).await;
    let (store, raw) = server.store().await;
    let at = |s: &str| raw.at(s);
    let t0 = at("2026-11-14 10:00:00.123");
    let later = at("2100-01-01 00:00:00.000");
    raw.exec(&format!(
        "INSERT INTO snodra_outbox (id, type, event_key, headers, payload, created_at, seq, attempts) VALUES \
         ('k-out-1', 'k.Placed', 'o-9', CAST('{{\"correlation-id\":\"c-9\"}}' AS {{json}}), \
         CAST('{{\"order\":\"o-9\",\"n\":9223372036854775807}}' AS {{json}}), {t0}, 7, 0)"
    ))
    .await;
    raw.exec(&format!(
        "INSERT INTO snodra_processed_events (id_hash, id, type, processed_at) VALUES \
         ('2056755db6b35cbaf0017d25a8681be79cac8de742130fac32b57a009dd146db', 'processor:default:k-9', 'k.Placed', {t0})"
    ))
    .await;
    raw.exec(&format!(
        "INSERT INTO snodra_sagas (id, type, state, start_key, start_key_hash, ended, ended_at, version, created_at, updated_at) VALUES \
         ('k-saga-9', 'k.Order', '{{\"step\":1}}', 'k.Order|orderId=o-9', \
         '0dab79facaf83924c17013b876dbc238367c56c3919eeaa668a2ff901e10d5bc', FALSE, NULL, 3, {t0}, {t0})"
    ))
    .await;
    raw.exec(
        "INSERT INTO snodra_saga_associations (saga_id, idx, assoc_key, assoc_value, assoc_hash) VALUES \
         ('k-saga-9', 0, 'orderId', 'o-9', '4e60ed4e2a89e62146b590fdad8e1664c2deb69308d7cd871ee66652b647976f')",
    )
    .await;
    raw.exec(&format!(
        "INSERT INTO snodra_external_calls (id_hash, id, status, reply, error, attempts, updated_at, lease_until, owner) VALUES \
         ('08f5a2bd2dc39708f9b2d021a8566fde1042190edef74723c66b1355830998cb', 'ingress:partner:k-9', 'SUCCEEDED', \
         '{{\"ok\":true}}', NULL, 1, {t0}, NULL, NULL)"
    ))
    .await;
    raw.exec(&format!(
        "INSERT INTO snodra_saga_failures (id_hash, id, saga_type, event_id, event_type, payload, headers, error, attempts, first_failed_at, last_failed_at) VALUES \
         ('98afaf05b6b25b2b9fe8303da05d523e91c475582ce82bffede01c564325d2ae', 'k.Order:e-9', 'k.Order', 'e-9', 'k.Placed', '{{}}', \
         CAST('{{\"userId\":\"u-1\"}}' AS {{json}}), 'boom', 2, {t0}, {t0})"
    ))
    .await;
    raw.exec(&format!(
        "INSERT INTO snodra_dead_letters (id, processor, source, segment, position, position_num, event_key, event_key_hash, \
         event_id, event_type, headers, payload, payload_binary, reason, error_type, error_message, blocked_by, attempts, \
         parked_at, last_attempt_at, expire_at, replay) VALUES \
         ('default:kraken.events:1:9', 'default', 'kraken.events', '1', '9', 9, 'key-9', \
         '1a4d5bcb1c7d0500b0f6566d599bf7a5492d0fded14c7bed41a682815d6bec92', 'e-9', 'k.Placed', \
         CAST('{{\"type\":\"k.Placed\"}}' AS {{json}}), '{{\"n\":9}}', NULL, 'HANDLER_FAILED', 'IllegalStateException', 'down', NULL, 3, \
         {t0}, {t0}, {later}, FALSE)"
    ))
    .await;
    raw.exec(&format!(
        "INSERT INTO snodra_processor_control (processor, paused, paused_at, paused_by, op_id, op_kind, op_lease_until, op_prepared_at, replay, updated_at) VALUES \
         ('default', TRUE, {t0}, 'ctl-1', 'op-9', 'reset', {later}, {t0}, \
         CAST('[{{\"source\":\"kraken.events\",\"segment\":\"1\",\"from\":\"5\",\"until\":\"9\"}}]' AS {{json}}), {t0})"
    ))
    .await;
    raw.exec(&format!(
        "INSERT INTO snodra_deadlines (id, saga_type, saga_id, name, payload, payload_type, due_at, correlation_id, status, claimed_by, claimed_until, finished_at) VALUES \
         ('k-d-9', 'k.Order', 'k-saga-9', 'expire', NULL, NULL, {t0}, 'c-9', 'SCHEDULED', NULL, NULL, NULL)"
    ))
    .await;

    let claimed = store
        .outbox()
        .claim("relay-r", Duration::from_secs(30), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let entry = &claimed[0];
    assert_eq!(
        (
            entry.id.as_str(),
            entry.type_name.as_str(),
            entry.key.as_deref(),
            entry.seq
        ),
        ("k-out-1", "k.Placed", Some("o-9"), 7)
    );
    assert_eq!(entry.created_at_ms, AT);
    assert_eq!(
        entry.headers,
        HashMap::from([("correlation-id".into(), "c-9".into())])
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&entry.payload).unwrap(),
        serde_json::json!({"order": "o-9", "n": 9_223_372_036_854_775_807_i64})
    );

    assert!(
        store
            .processed_events()
            .exists("processor:default:k-9")
            .await
            .unwrap(),
        "a Kotlin marker, found by its hash"
    );
    let txs = store.tx();
    let sagas = store.sagas();
    let mut tx = Tx::none();
    let live = sagas
        .sagas
        .find_live(&mut tx, "k.Order", "orderId", "o-9")
        .await
        .unwrap();
    assert_eq!(
        live.len(),
        1,
        "a Kotlin saga, found by its association's hash"
    );
    assert_eq!(live[0].version, 3);
    assert_eq!(live[0].state, "{\"step\":1}");
    assert_eq!(
        live[0].associations,
        vec![Association::new("orderId", "o-9")]
    );
    let mut tx = txs.begin().await.unwrap();
    let second = sagas
        .sagas
        .insert(
            &mut tx,
            &SagaDocument {
                id: "rust-saga-9".into(),
                version: 0,
                ..live[0].clone()
            },
        )
        .await;
    txs.abort(&mut tx).await;
    assert!(
        second.as_ref().is_err_and(|e| e.is_conflict()),
        "the Kotlin saga holds the start key for Rust too: {second:?}"
    );

    let call = store
        .external_calls()
        .find("ingress:partner:k-9")
        .await
        .unwrap()
        .expect("a Kotlin external call");
    assert_eq!(call.status, ExternalCallStatus::Succeeded);
    assert_eq!(call.reply.as_deref(), Some("{\"ok\":true}"));
    let mut tx = Tx::none();
    let failure = sagas
        .failures
        .find(&mut tx, "k.Order:e-9")
        .await
        .unwrap()
        .expect("a Kotlin saga failure");
    assert_eq!(
        failure.headers,
        HashMap::from([("userId".into(), "u-1".into())])
    );
    assert_eq!((failure.attempts, failure.last_failed_at_ms), (2, AT));

    let letters = store.dead_letters();
    assert_eq!(
        letters.head("default", "key-9").await.unwrap().as_deref(),
        Some("default:kraken.events:1:9"),
        "a Kotlin letter, found by its key's hash"
    );
    let letter = letters
        .get("default:kraken.events:1:9")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(letter.payload, b"{\"n\":9}");
    assert_eq!(letter.error.type_name, "IllegalStateException");
    assert_eq!(letter.parked_at_ms, AT);

    let state = store.processor_control().get("default").await.unwrap();
    assert!(state.paused);
    assert_eq!(state.paused_by.as_deref(), Some("ctl-1"));
    assert_eq!(
        state.operation.as_ref().map(|o| o.id.as_str()),
        Some("op-9")
    );
    assert_eq!(
        state.replay,
        vec![ReplayRange {
            source: "kraken.events".into(),
            segment: "1".into(),
            from: "5".into(),
            until: "9".into(),
        }]
    );

    let now = snodra_messaging::time::now_ms();
    let due = sagas
        .deadlines
        .claim_due("scheduler-r", now, now + 30_000)
        .await
        .unwrap()
        .expect("the Kotlin deadline");
    assert_eq!(
        (due.id.as_str(), due.correlation_id.as_deref()),
        ("k-d-9", Some("c-9"))
    );

    // The saga-side outbox as Kotlin writes it: a pending command (no notice: NULLs) carrying the
    // trace of its cause, and a command given up on with its failure notice pending.
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    raw.exec(&format!(
        "INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, routing_key, created_at, \
         dispatched_at, attempts, next_attempt_at, failed_at, failure_reason, failure_message, notice_pending, \
         notice_attempts, notice_next_at) VALUES \
         ('k-saga-9:4000', 'k-saga-9', 4000, 'COMMAND', 'k.Ship', '{{\"order\":\"o-9\"}}', \
         CAST('{{\"correlation-id\":\"c-9\",\"traceparent\":\"{traceparent}\"}}' AS {{json}}), 'o-9', {t0}, \
         NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         ('k-saga-8:3000', 'k-saga-8', 3000, 'COMMAND', 'k.Bill', '{{}}', CAST('{{}}' AS {{json}}), NULL, {t0}, \
         NULL, 5, NULL, {t0}, 'NO_HANDLER', 'nobody handles k.Bill', TRUE, 0, NULL)"
    ))
    .await;
    let pending = sagas
        .dispatch
        .next_pending("k-saga-9")
        .await
        .unwrap()
        .expect("the Kotlin dispatch entry");
    assert_eq!(
        (
            pending.kind,
            pending.type_name.as_str(),
            pending.seq,
            pending.routing_key.as_deref()
        ),
        (DispatchKind::Command, "k.Ship", 4000, Some("o-9"))
    );
    assert_eq!(pending.payload, "{\"order\":\"o-9\"}");
    assert_eq!(
        pending.headers,
        HashMap::from([
            ("correlation-id".into(), "c-9".into()),
            ("traceparent".into(), traceparent.into()),
        ])
    );
    assert_eq!(
        (
            pending.notice_pending,
            pending.notice_attempts,
            pending.notice_next_at_ms
        ),
        (None, None, None),
        "no notice: NULLs"
    );
    assert_eq!(pending.created_at_ms, AT);
    let notice = sagas
        .dispatch
        .claim_due_notice(now, now + 30_000)
        .await
        .unwrap()
        .expect("the Kotlin failure notice");
    assert_eq!(notice.id, "k-saga-8:3000");
    assert_eq!(
        notice.failure_reason,
        Some(snodra_messaging::saga::FailureReason::NoHandler)
    );
    assert_eq!(
        notice.failure_message.as_deref(),
        Some("nobody handles k.Bill")
    );
    assert_eq!(
        (
            notice.notice_pending,
            notice.notice_attempts,
            notice.failed_at_ms,
            notice.attempts
        ),
        (Some(true), Some(0), Some(AT), 5)
    );
    assert_eq!(
        sagas.dispatch.pending().await.unwrap(),
        1,
        "the given-up entry is not pending"
    );

    // Dispatch leases as Kotlin writes them: a running one holds, an expired one is taken over.
    raw.exec(&format!(
        "INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES \
         ('k-saga-9', 'kotlin-relay', {later}), ('k-saga-8', 'kotlin-relay', {t0})"
    ))
    .await;
    assert!(
        !sagas
            .dispatch
            .claim_lease("k-saga-9", "rust-relay", now, now + 30_000)
            .await
            .unwrap(),
        "held by the Kotlin relay"
    );
    assert!(
        sagas
            .dispatch
            .claim_lease("k-saga-8", "rust-relay", now, now + 30_000)
            .await
            .unwrap(),
        "the Kotlin relay's lease passed"
    );
    assert_eq!(
        raw.row("SELECT owner FROM snodra_saga_dispatch_leases WHERE saga_id = 'k-saga-8'")
            .await,
        texts(&[Some("rust-relay")])
    );
}

#[tokio::test]
async fn postgres_stores_the_kotlin_values() {
    golden_values(Kind::Postgres).await;
}

#[tokio::test]
async fn mysql_stores_the_kotlin_values() {
    golden_values(Kind::MySql).await;
}

#[tokio::test]
async fn postgres_reads_rows_in_the_kotlin_layout() {
    rows_in_the_kotlin_layout_are_read(Kind::Postgres).await;
}

#[tokio::test]
async fn mysql_reads_rows_in_the_kotlin_layout() {
    rows_in_the_kotlin_layout_are_read(Kind::MySql).await;
}
