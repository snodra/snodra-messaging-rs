//! The SQL store's behaviour beyond the kit, on both databases: the service's transaction, the bounds, the schema, the purge, and the concurrency the
//! MySQL lock rules are there for.

#![cfg(all(feature = "postgres", feature = "mysql"))]

mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use snodra_messaging::Config;
use snodra_messaging::error::MessagingError;
use snodra_messaging::saga::store::{Association, SagaDocument, SagaFailureDocument};
use snodra_messaging::store::{
    DeadLetter, DeadLetterError, DeadLetterReason, Locked, MessageStore, OutboxEntry,
    ProcessedMarker, StoreError, Tx,
};
use snodra_messaging::time::now_ms;
use snodra_messaging_sqlx::{SqlxSettings, SqlxStore, SqlxTxExt};
use support::{Kind, Raw, Server};

fn entry(id: &str) -> OutboxEntry {
    OutboxEntry {
        id: id.into(),
        type_name: "t.Happened".into(),
        key: None,
        headers: HashMap::new(),
        payload: br#"{"n":1}"#.to_vec(),
        created_at_ms: now_ms(),
        seq: 1,
        attempts: 0,
    }
}

fn marker(id: &str) -> ProcessedMarker {
    ProcessedMarker {
        id: id.into(),
        type_name: "t.Happened".into(),
        processed_at_ms: now_ms(),
        processor: None,
        source: None,
        segment: None,
        position: None,
    }
}

fn saga(id: &str, key: &str) -> SagaDocument {
    SagaDocument {
        id: id.into(),
        type_name: "t.Order".into(),
        state: "{}".into(),
        associations: vec![Association::new("orderId", key)],
        start_key: Some(format!("t.Order|orderId={key}")),
        ended: false,
        ended_at_ms: None,
        version: 0,
        created_at_ms: now_ms(),
        updated_at_ms: now_ms(),
    }
}

fn is_invalid(r: &Result<impl std::fmt::Debug, StoreError>) -> bool {
    matches!(
        r,
        Err(StoreError::Failed(MessagingError::InvalidMessage(_)))
    )
}

/// A business row written in the handler's transaction (`SqlxTxExt`) and the outbox row commit
/// together and roll back together; a dropped transaction leaves nothing; a write once the
/// transaction ended commits on its own; another store's transaction is refused.
async fn transactions_with_the_service(kind: Kind) {
    let server = Server::of(kind).await;
    let (store, raw) = server.store().await;
    raw.exec("CREATE TABLE orders (id VARCHAR(64) NOT NULL PRIMARY KEY)")
        .await;
    let txs = store.tx();
    let outbox = store.outbox();
    let order = async |tx: &mut Tx, id: &str| match kind {
        Kind::Postgres => {
            sqlx::query("INSERT INTO orders (id) VALUES ($1)")
                .bind(id)
                .execute(tx.connection::<sqlx::Postgres>())
                .await
                .unwrap();
        }
        Kind::MySql => {
            sqlx::query("INSERT INTO orders (id) VALUES (?)")
                .bind(id)
                .execute(tx.connection::<sqlx::MySql>())
                .await
                .unwrap();
        }
    };
    let both = async |raw: &Raw, id: &str| {
        (
            raw.rows(&format!("SELECT id FROM orders WHERE id = '{id}'"))
                .await
                .len(),
            raw.rows(&format!("SELECT id FROM snodra_outbox WHERE id = '{id}'"))
                .await
                .len(),
        )
    };

    let mut tx = txs.begin().await.unwrap();
    order(&mut tx, "o-commit").await;
    outbox.append(&mut tx, entry("o-commit")).await.unwrap();
    assert_eq!(
        both(&raw, "o-commit").await,
        (0, 0),
        "nothing visible before the commit"
    );
    txs.commit(&mut tx).await.unwrap();
    assert_eq!(both(&raw, "o-commit").await, (1, 1), "committed together");

    let mut tx = txs.begin().await.unwrap();
    order(&mut tx, "o-abort").await;
    outbox.append(&mut tx, entry("o-abort")).await.unwrap();
    txs.abort(&mut tx).await;
    assert_eq!(both(&raw, "o-abort").await, (0, 0), "rolled back together");

    {
        let mut tx = txs.begin().await.unwrap();
        order(&mut tx, "o-drop").await;
        outbox.append(&mut tx, entry("o-drop")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        both(&raw, "o-drop").await,
        (0, 0),
        "a dropped transaction leaves nothing"
    );

    let mut tx = txs.begin().await.unwrap();
    txs.commit(&mut tx).await.unwrap();
    outbox.append(&mut tx, entry("o-after")).await.unwrap();
    assert_eq!(
        both(&raw, "o-after").await,
        (0, 1),
        "after the commit: on its own"
    );
    assert!(
        tx.try_connection::<sqlx::Postgres>().is_none()
            && tx.try_connection::<sqlx::MySql>().is_none()
    );

    let mut foreign = Tx::new(String::from("another store's"));
    let refused = outbox.append(&mut foreign, entry("o-foreign")).await;
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.to_string().contains("not one of its own")),
        "{refused:?}"
    );
}

/// Each bound accepted at the bound (multi-byte characters) and refused one over it with nothing
/// written; values without a bound (hashed) of 10 000 characters; U+0000 refused; free text cut
/// to 16 KiB; trailing spaces matter (byte comparison).
async fn bounds(kind: Kind) {
    let server = Server::of(kind).await;
    let (store, raw) = server.store().await;
    let txs = store.tx();
    let outbox = store.outbox();

    for (field, ok, over) in [
        ("an outbox id", "é".repeat(255), "é".repeat(256)),
        ("an outbox id (4 bytes)", "😀".repeat(255), "😀".repeat(256)),
    ] {
        outbox
            .append_now(entry(&ok))
            .await
            .unwrap_or_else(|e| panic!("{field}: {e}"));
        assert!(
            is_invalid(&outbox.append_now(entry(&over)).await),
            "{field}"
        );
    }
    let mut long_type = entry("e-type");
    long_type.type_name = "t".repeat(256);
    assert!(is_invalid(&outbox.append_now(long_type).await));
    assert_eq!(
        raw.count("snodra_outbox").await,
        2,
        "nothing written over a bound"
    );

    let control = store.processor_control();
    control
        .set_paused(&"😀".repeat(64), true, "local", now_ms())
        .await
        .unwrap();
    assert!(is_invalid(
        &control
            .set_paused(&"😀".repeat(65), true, "local", now_ms())
            .await
    ));
    assert_eq!(raw.count("snodra_processor_control").await, 1);

    let letter = |processor: &str, source: &str, segment: &str, position: &str| DeadLetter {
        id: snodra_messaging::store::letter_id(processor, source, segment, position),
        processor: processor.into(),
        source: source.into(),
        segment: segment.into(),
        position: position.into(),
        key: Some("k".repeat(10_000)),
        event_id: None,
        event_type: None,
        headers: Default::default(),
        payload: b"{}".to_vec(),
        reason: DeadLetterReason::HandlerFailed,
        error: DeadLetterError::new("Storage", "down"),
        blocked_by: None,
        attempts: 1,
        parked_at_ms: now_ms(),
        last_attempt_at_ms: now_ms(),
        expire_at_ms: now_ms() + 86_400_000,
        replay: false,
    };
    let longest = letter(
        &"p".repeat(64),
        &"s".repeat(255),
        &"1".repeat(128),
        &"2".repeat(128),
    );
    assert_eq!(longest.id.chars().count(), 578);
    store.dead_letters().park(&longest).await.unwrap();
    assert!(is_invalid(
        &store
            .dead_letters()
            .park(&letter("p", &"s".repeat(256), "1", "2"))
            .await
    ));
    assert_eq!(
        store
            .dead_letters()
            .head(&"p".repeat(64), &"k".repeat(10_000))
            .await
            .unwrap(),
        Some(longest.id.clone()),
        "a 10 000-character key, found by its hash"
    );

    // Unbounded values go through their hashes.
    let long = "m".repeat(10_000);
    let mut tx = txs.begin().await.unwrap();
    assert!(
        store
            .processed_events()
            .record(&mut tx, &marker(&long))
            .await
            .unwrap()
    );
    let mut started = saga("s-long", &"v".repeat(10_000));
    started.start_key = Some(format!("t.Order|orderId={}", "v".repeat(10_000)));
    store.sagas().sagas.insert(&mut tx, &started).await.unwrap();
    txs.commit(&mut tx).await.unwrap();
    assert!(store.processed_events().exists(&long).await.unwrap());
    let mut tx = Tx::none();
    assert_eq!(
        store
            .sagas()
            .sagas
            .find_live(&mut tx, "t.Order", "orderId", &"v".repeat(10_000))
            .await
            .unwrap()
            .len(),
        1
    );
    let call = snodra_messaging::store::ExternalCall {
        key: "i".repeat(10_000),
        status: snodra_messaging::store::ExternalCallStatus::Succeeded,
        reply: Some("{}".into()),
        error: None,
        attempts: 1,
        updated_at_ms: now_ms(),
        lease_until_ms: None,
        owner: None,
    };
    assert!(store.external_calls().insert(&call).await.unwrap());
    assert!(!store.external_calls().insert(&call).await.unwrap(), "once");

    // An instant no timestamp holds is refused, not stored as 1970.
    let mut far = entry("e-far");
    far.created_at_ms = i64::MAX;
    let refused = outbox.append_now(far).await;
    assert!(
        is_invalid(&refused) && refused.unwrap_err().to_string().contains("out of range"),
        "an out-of-range instant"
    );

    // U+0000.
    assert!(is_invalid(&outbox.append_now(entry("e\0")).await));
    let mut nul_header = entry("e-h");
    nul_header.headers.insert("k".into(), "a\0".into());
    assert!(is_invalid(&outbox.append_now(nul_header).await));
    let mut nul_payload = entry("e-p");
    nul_payload.payload = br#"{"s":"a\u0000"}"#.to_vec();
    assert!(is_invalid(&outbox.append_now(nul_payload).await));
    let mut tx = txs.begin().await.unwrap();
    assert!(is_invalid(
        &store
            .processed_events()
            .record(&mut tx, &marker("m\0"))
            .await
    ));
    let mut nul_state = saga("s-nul", "n-1");
    nul_state.state = "{\"a\":\"\0\"}".into();
    assert!(is_invalid(
        &store.sagas().sagas.insert(&mut tx, &nul_state).await
    ));
    let mut nul_assoc = saga("s-nul2", "n-2");
    nul_assoc.associations = vec![Association::new("orderId", "a\0")];
    assert!(is_invalid(
        &store.sagas().sagas.insert(&mut tx, &nul_assoc).await
    ));
    txs.abort(&mut tx).await;

    // Free text cut to 16 KiB of UTF-8.
    outbox.append_now(entry("e-cut")).await.unwrap();
    outbox
        .mark_failed("e-cut", &"é".repeat(10_000))
        .await
        .unwrap();
    let error = raw
        .row("SELECT last_error FROM snodra_outbox WHERE id = 'e-cut'")
        .await[0]
        .clone()
        .unwrap();
    assert_eq!(error.len(), 16_384);

    // Trailing spaces and case: different values.
    for id in ["e-1", "e-1 ", "E-1"] {
        outbox.append_now(entry(id)).await.unwrap();
    }
    let mut tx = txs.begin().await.unwrap();
    for id in ["m-1", "m-1 "] {
        assert!(
            store
                .processed_events()
                .record(&mut tx, &marker(id))
                .await
                .unwrap(),
            "{id:?}"
        );
    }
    txs.commit(&mut tx).await.unwrap();
    for p in ["p-1", "p-1 "] {
        control
            .set_paused(p, true, "local", now_ms())
            .await
            .unwrap();
    }
    assert_eq!(
        raw.rows("SELECT processor FROM snodra_processor_control WHERE processor LIKE 'p-1%'")
            .await
            .len(),
        2
    );
}

/// Instances starting together apply the schema once; a database at V1 is only read; the file
/// re-runs over itself; a later version is accepted; with init off a missing schema fails the
/// startup, naming the file; the server checks.
async fn schema(kind: Kind) {
    let server = Server::of(kind).await;
    let url = server.database().await;
    let (a, raw) = server.open(&url, SqlxSettings::default()).await;
    let (b, _) = server.open(&url, SqlxSettings::default()).await;
    let refused = a.require_schema().await.unwrap_err().to_string();
    assert!(
        refused.contains("schema is at version 0, it needs 1 or later")
            && refused.contains(&format!(
                "db/snodra/{}/V1__snodra.sql",
                if kind == Kind::Postgres {
                    "postgresql"
                } else {
                    "mysql"
                }
            )),
        "{refused}"
    );
    let (ra, rb) = tokio::join!(a.apply_schema(), b.apply_schema());
    ra.unwrap();
    rb.unwrap();
    assert_eq!(
        raw.rows("SELECT CAST(version AS {text}) FROM snodra_schema")
            .await
            .len(),
        1
    );
    a.apply_schema().await.unwrap();
    a.require_schema().await.unwrap();
    // The file runs again over itself.
    let script = match kind {
        Kind::Postgres => snodra_messaging_sqlx::schema::POSTGRESQL_V1,
        Kind::MySql => snodra_messaging_sqlx::schema::MYSQL_V1,
    };
    raw.exec(script).await;
    raw.exec("INSERT INTO snodra_schema (version) VALUES (2)")
        .await;
    a.require_schema()
        .await
        .expect("a later version is accepted");
    a.apply_schema().await.expect("and only read");

    // init off at startup: the core asks the store to check.
    let empty = server.database().await;
    let (fresh, _) = server.open(&empty, SqlxSettings::default()).await;
    let mut config = Config::new("schema-check", "static://127.0.0.1:1");
    config.hub_enabled = false;
    config.outbox_enabled = false;
    config.store_init_schema = false;
    let started = snodra_messaging::SnodraMessaging::builder(config)
        .await
        .unwrap()
        .store(Arc::new(fresh))
        .start()
        .await;
    assert!(
        started
            .err()
            .is_some_and(|e| e.to_string().contains("schema is at version 0")),
        "a missing schema fails the startup"
    );

    // A user who cannot read snodra_schema gets that error, not "version 0".
    let db = support::database_name(&url).to_string();
    match kind {
        Kind::Postgres => {
            raw.exec("CREATE ROLE kreader LOGIN PASSWORD 'kreader'")
                .await;
        }
        Kind::MySql => {
            raw.exec("CREATE USER 'kreader'@'%' IDENTIFIED BY 'kreader'")
                .await;
            raw.exec(&format!(
                "GRANT SELECT ON {db}.snodra_outbox TO 'kreader'@'%'"
            ))
            .await;
        }
    }
    let (reader, _) = server
        .open(
            &support::as_user(&url, "kreader", "kreader"),
            SqlxSettings::default(),
        )
        .await;
    let refused = reader.require_schema().await.unwrap_err().to_string();
    assert!(
        refused.contains("cannot read the schema version") && !refused.contains("version 0"),
        "{refused}"
    );

    // A pool of one connection is refused.
    let single = match kind {
        Kind::Postgres => SqlxStore::postgres(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .unwrap(),
        ),
        Kind::MySql => SqlxStore::mysql(
            sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .unwrap(),
        ),
    };
    let refused = single.require_schema().await.unwrap_err().to_string();
    assert!(refused.contains("at least 2"), "{refused}");

    if kind == Kind::MySql {
        let lax = SqlxStore::mysql(
            sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(2)
                .after_connect(|c, _| {
                    Box::pin(async move {
                        sqlx::raw_sql("SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'")
                            .execute(c)
                            .await
                            .map(|_| ())
                    })
                })
                .connect(&url)
                .await
                .unwrap(),
        );
        let refused = lax.apply_schema().await.unwrap_err().to_string();
        assert!(refused.contains("strict sql_mode"), "{refused}");
    }
}

/// Every table's retention, nothing pending or live purged.
async fn purge(kind: Kind) {
    let server = Server::of(kind).await;
    let (store, raw) = server.store().await;
    let old = raw.at("2020-01-01 00:00:00.000");
    let fresh_text = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string();
    let fresh = raw.at(&fresh_text);
    let future = raw.at("2100-01-01 00:00:00.000");
    // Each old row goes; the pending, live, scheduled, fresh and held ones stay.
    for sql in [
        format!("INSERT INTO snodra_outbox (id, type, headers, payload, created_at, seq, attempts, sent_at) VALUES ('o-old', 't', CAST('{{}}' AS {{json}}), CAST('{{}}' AS {{json}}), {old}, 1, 0, {old})"),
        format!("INSERT INTO snodra_outbox (id, type, headers, payload, created_at, seq, attempts) VALUES ('o-pending', 't', CAST('{{}}' AS {{json}}), CAST('{{}}' AS {{json}}), {old}, 1, 0)"),
        format!("INSERT INTO snodra_outbox (id, type, headers, payload, created_at, seq, attempts, failed_at) VALUES ('o-failed', 't', CAST('{{}}' AS {{json}}), CAST('{{}}' AS {{json}}), {old}, 1, 0, {old})"),
        format!("INSERT INTO snodra_processed_events (id_hash, id, type, processed_at) VALUES ('{}', 'm-old', 't', {old})", "a".repeat(64)),
        format!("INSERT INTO snodra_processed_events (id_hash, id, type, processed_at) VALUES ('{}', 'm-new', 't', {fresh})", "b".repeat(64)),
        format!("INSERT INTO snodra_external_calls (id_hash, id, status, attempts, updated_at) VALUES ('{}', 'c-old', 'SUCCEEDED', 1, {old})", "c".repeat(64)),
        format!("INSERT INTO snodra_sagas (id, type, state, ended, ended_at, version, created_at, updated_at) VALUES ('s-ended', 't', '{{}}', TRUE, {old}, 1, {old}, {old})"),
        format!("INSERT INTO snodra_sagas (id, type, state, ended, version, created_at, updated_at) VALUES ('s-live', 't', '{{}}', FALSE, 1, {old}, {old})"),
        "INSERT INTO snodra_saga_associations (saga_id, idx, assoc_key, assoc_value, assoc_hash) VALUES ('s-ended', 0, 'k', 'v', 'h')".to_string(),
        format!("INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, created_at, attempts, dispatched_at) VALUES ('d-sent', 's', 1, 'EVENT', 't', '{{}}', CAST('{{}}' AS {{json}}), {old}, 0, {old})"),
        format!("INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, created_at, attempts, failed_at) VALUES ('d-failed', 's', 2, 'COMMAND', 't', '{{}}', CAST('{{}}' AS {{json}}), {old}, 0, {old})"),
        format!("INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, created_at, attempts) VALUES ('d-pending', 's', 3, 'COMMAND', 't', '{{}}', CAST('{{}}' AS {{json}}), {old}, 0)"),
        format!("INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES ('s-gone', 'r', {old})"),
        format!("INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES ('s-held', 'r', {future})"),
        format!("INSERT INTO snodra_deadlines (id, saga_type, saga_id, name, due_at, correlation_id, status, finished_at) VALUES ('dl-fired', 't', 's', 'n', {old}, 'c', 'FIRED', {old})"),
        format!("INSERT INTO snodra_deadlines (id, saga_type, saga_id, name, due_at, correlation_id, status) VALUES ('dl-scheduled', 't', 's', 'n', {old}, 'c', 'SCHEDULED')"),
        format!("INSERT INTO snodra_saga_failures (id_hash, id, saga_type, event_id, event_type, payload, headers, error, attempts, first_failed_at, last_failed_at) VALUES ('{}', 'f', 't', 'e', 't', '{{}}', CAST('{{}}' AS {{json}}), 'x', 1, {old}, {old})", "f".repeat(64)),
        format!("INSERT INTO snodra_dead_letters (id, processor, source, segment, position, headers, reason, error_type, error_message, attempts, parked_at, last_attempt_at, expire_at) VALUES ('l-expired', 'p', 's', '0', '1', CAST('{{}}' AS {{json}}), 'INVALID', 'x', 'x', 1, {old}, {old}, {old})"),
        format!("INSERT INTO snodra_dead_letters (id, processor, source, segment, position, headers, reason, error_type, error_message, attempts, parked_at, last_attempt_at, expire_at) VALUES ('l-live', 'p', 's', '0', '2', CAST('{{}}' AS {{json}}), 'INVALID', 'x', 'x', 1, {old}, {old}, {future})"),
    ] {
        raw.exec(&sql).await;
    }
    let config = Config::new("purge", "static://127.0.0.1:1");
    let purged = store.purge_expired(&config).await.unwrap();
    assert_eq!(
        purged, 10,
        "outbox, marker, call, saga, 2 dispatch, lease, deadline, failure, letter"
    );
    for (table, left) in [
        ("snodra_outbox", 2),
        ("snodra_processed_events", 1),
        ("snodra_external_calls", 0),
        ("snodra_sagas", 1),
        ("snodra_saga_associations", 0),
        ("snodra_saga_dispatch", 1),
        ("snodra_saga_dispatch_leases", 1),
        ("snodra_deadlines", 1),
        ("snodra_saga_failures", 0),
        ("snodra_dead_letters", 1),
    ] {
        assert_eq!(raw.count(table).await, left, "{table}");
    }
    assert_eq!(store.purge_expired(&config).await.unwrap(), 0);
}

/// The MySQL lock rules (also run on PostgreSQL): an open claim does not hide another relay's
/// entries; two locks of a new processor give one holder; starts of different keys held open
/// together never conflict; takeovers of an expired lease give one owner; concurrent saves of
/// one failure both succeed.
async fn concurrency(kind: Kind) {
    let server = Server::of(kind).await;
    let (store, raw) = server.store().await;
    let outbox = store.outbox();
    for (i, id) in ["c-1", "c-2", "c-3"].into_iter().enumerate() {
        let mut e = entry(id);
        e.created_at_ms = now_ms() - 10_000 + i as i64;
        outbox.append_now(e).await.unwrap();
    }
    // B holds c-1 (its send failed); another claimer's open transaction holds c-2.
    let b = outbox
        .claim("relay-b", Duration::from_secs(60), 1)
        .await
        .unwrap();
    assert_eq!(b[0].id, "c-1");
    let holder = match &raw {
        Raw::Pg(p) => {
            let mut t = p.begin().await.unwrap();
            sqlx::raw_sql("SELECT id FROM snodra_outbox WHERE id = 'c-2' FOR UPDATE")
                .execute(&mut *t)
                .await
                .unwrap();
            Box::new(t) as Box<dyn std::any::Any + Send>
        }
        Raw::My(p) => {
            let mut t = p.begin().await.unwrap();
            sqlx::raw_sql("SELECT id FROM snodra_outbox WHERE id = 'c-2' FOR UPDATE")
                .execute(&mut *t)
                .await
                .unwrap();
            Box::new(t) as Box<dyn std::any::Any + Send>
        }
    };
    let again = outbox
        .claim("relay-b", Duration::from_secs(60), 10)
        .await
        .unwrap();
    assert_eq!(
        again.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        ["c-1", "c-3"],
        "its own entry and the free one; the locked one skipped"
    );
    drop(holder);

    let control = store.processor_control();
    for round in 0..10 {
        let p = format!("new-{round}");
        let (x, y) = tokio::join!(
            control.lock(&p, "op-x", now_ms() + 60_000, now_ms()),
            control.lock(&p, "op-y", now_ms() + 60_000, now_ms())
        );
        let taken = [x.unwrap(), y.unwrap()]
            .iter()
            .filter(|l| **l == Locked::Taken)
            .count();
        assert_eq!(taken, 1, "round {round}: one holder");
    }

    let txs = store.tx();
    let mut open = Vec::new();
    for i in 0..16 {
        let mut tx = txs.begin().await.unwrap();
        store
            .sagas()
            .sagas
            .insert(&mut tx, &saga(&format!("start-{i}"), &format!("key-{i}")))
            .await
            .unwrap_or_else(|e| panic!("start {i}: {e}"));
        open.push(tx);
    }
    for mut tx in open {
        txs.commit(&mut tx).await.unwrap();
    }

    let dispatch = store.sagas().dispatch;
    for round in 0..40 {
        let saga_id = format!("lease-{round}");
        let now = now_ms();
        if round < 20 {
            assert!(
                dispatch
                    .claim_lease(&saga_id, "old", now - 120_000, now - 60_000)
                    .await
                    .unwrap()
            );
        }
        let (x, y) = tokio::join!(
            dispatch.claim_lease(&saga_id, "x", now, now + 60_000),
            dispatch.claim_lease(&saga_id, "y", now, now + 60_000)
        );
        let owners = [x.unwrap(), y.unwrap()].iter().filter(|t| **t).count();
        assert_eq!(owners, 1, "round {round}: one owner");
    }

    let failures = store.sagas().failures;
    let failure = |attempts| SagaFailureDocument {
        id: "t.Order:e-race".into(),
        saga_type: "t.Order".into(),
        event_id: "e-race".into(),
        event_type: "t.E".into(),
        payload: "{}".into(),
        headers: HashMap::new(),
        error: "x".into(),
        attempts,
        first_failed_at_ms: now_ms(),
        last_failed_at_ms: now_ms(),
    };
    for round in 0..10 {
        let id = format!("t.Order:e-race-{round}");
        let one = SagaFailureDocument {
            id: id.clone(),
            ..failure(1)
        };
        let two = SagaFailureDocument {
            id: id.clone(),
            ..failure(2)
        };
        let save = async |doc: &SagaFailureDocument| {
            let mut tx = txs.begin().await?;
            failures.save(&mut tx, doc).await?;
            txs.commit(&mut tx).await
        };
        let (x, y) = tokio::join!(save(&one), save(&two));
        x.unwrap_or_else(|e| panic!("round {round}: {e}"));
        y.unwrap_or_else(|e| panic!("round {round}: {e}"));
        let (x, y) = tokio::join!(save(&two), save(&one));
        x.unwrap_or_else(|e| panic!("re-save round {round}: {e}"));
        y.unwrap_or_else(|e| panic!("re-save round {round}: {e}"));
    }
}

/// What a handler does with sqlx's own nested transactions on the connection it is given
/// (`SqlxTxExt::connection`): savepoints inside the store's transaction, never a transaction of
/// their own. (a) an inner transaction committed, then the handler fails: nothing remains; (b) an
/// inner transaction rolled back, the handler succeeds: the outer rows commit, the inner ones
/// not; (c) a nested transaction left open: the commit fails and nothing is committed; (d) the
/// connection says it is in a transaction.
macro_rules! nested_transactions {
    ($name:ident, $db:ty, $kind:expr, $p:literal) => {
        async fn $name() {
            use sqlx::Connection;
            let server = Server::of($kind).await;
            let (store, raw) = server.store().await;
            raw.exec("CREATE TABLE orders (id VARCHAR(64) NOT NULL PRIMARY KEY)")
                .await;
            let txs = store.tx();
            let outbox = store.outbox();
            let processed = store.processed_events();
            let count = async |table: &str, column: &str, id: &str| {
                raw.rows(&format!(
                    "SELECT {column} FROM {table} WHERE {column} = '{id}'"
                ))
                .await
                .len()
            };
            let insert = concat!("INSERT INTO orders (id) VALUES (", $p, ")");

            // (d) and (a)
            let mut tx = txs.begin().await.unwrap();
            assert!(
                tx.connection::<$db>().is_in_transaction(),
                "(d) in a transaction"
            );
            outbox.append(&mut tx, entry("n-a")).await.unwrap();
            assert!(processed.record(&mut tx, &marker("m-a")).await.unwrap());
            let mut inner = tx.connection::<$db>().begin().await.unwrap();
            sqlx::query(insert)
                .bind("n-a")
                .execute(&mut *inner)
                .await
                .unwrap();
            inner.commit().await.unwrap();
            assert!(
                tx.connection::<$db>().is_in_transaction(),
                "still in the store's transaction"
            );
            txs.abort(&mut tx).await; // the handler failed
            assert_eq!(
                (
                    count("orders", "id", "n-a").await,
                    count("snodra_outbox", "id", "n-a").await,
                    count("snodra_processed_events", "id", "m-a").await
                ),
                (0, 0, 0),
                "(a) an inner commit commits nothing of the outer transaction"
            );

            // (b)
            let mut tx = txs.begin().await.unwrap();
            outbox.append(&mut tx, entry("n-b")).await.unwrap();
            let mut inner = tx.connection::<$db>().begin().await.unwrap();
            sqlx::query(insert)
                .bind("n-b-inner")
                .execute(&mut *inner)
                .await
                .unwrap();
            inner.rollback().await.unwrap();
            sqlx::query(insert)
                .bind("n-b-outer")
                .execute(tx.connection::<$db>())
                .await
                .unwrap();
            txs.commit(&mut tx).await.unwrap();
            assert_eq!(
                (
                    count("orders", "id", "n-b-outer").await,
                    count("snodra_outbox", "id", "n-b").await,
                    count("orders", "id", "n-b-inner").await
                ),
                (1, 1, 0),
                "(b) the inner rollback undoes the inner rows only"
            );

            // (c)
            let mut tx = txs.begin().await.unwrap();
            outbox.append(&mut tx, entry("n-c")).await.unwrap();
            let mut inner = tx.connection::<$db>().begin().await.unwrap();
            sqlx::query(insert)
                .bind("n-c")
                .execute(&mut *inner)
                .await
                .unwrap();
            std::mem::forget(inner); // left open
            let committed = txs.commit(&mut tx).await;
            assert!(
                committed
                    .as_ref()
                    .is_err_and(|e| e.to_string().contains("nested transaction")),
                "(c) the commit fails: {committed:?}"
            );
            assert_eq!(
                (
                    count("orders", "id", "n-c").await,
                    count("snodra_outbox", "id", "n-c").await
                ),
                (0, 0),
                "(c) nothing committed"
            );
        }
    };
}

nested_transactions!(nested_postgres, sqlx::Postgres, Kind::Postgres, "$1");
nested_transactions!(nested_mysql, sqlx::MySql, Kind::MySql, "?");

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_nested_transactions_are_savepoints_of_the_stores() {
    nested_postgres().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_nested_transactions_are_savepoints_of_the_stores() {
    nested_mysql().await;
}

macro_rules! both {
    ($check:ident, $pg:ident, $my:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $pg() {
            $check(Kind::Postgres).await;
        }
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $my() {
            $check(Kind::MySql).await;
        }
    };
}

both!(
    transactions_with_the_service,
    postgres_business_rows_and_the_outbox_commit_together,
    mysql_business_rows_and_the_outbox_commit_together
);
both!(
    bounds,
    postgres_bounds_hashes_and_nul,
    mysql_bounds_hashes_and_nul
);
both!(
    schema,
    postgres_schema_init_and_check,
    mysql_schema_init_and_check
);
both!(
    purge,
    postgres_purge_keeps_what_is_pending_or_live,
    mysql_purge_keeps_what_is_pending_or_live
);
both!(
    concurrency,
    postgres_concurrent_claims_and_starts,
    mysql_concurrent_claims_and_starts
);

/// A MySQL saga start sets `innodb_lock_wait_timeout` to 1 s for its insert: a start dropped
/// while it waits (its future dropped, then its transaction aborted or dropped) never hands that
/// session to the next user of the pool: the connection is closed instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_a_dropped_start_never_leaves_its_lock_wait_on_a_pooled_connection() {
    let server = Server::mysql().await;
    let url = server.database().await;
    let (holder, _) = server.open(&url, SqlxSettings::default()).await;
    holder.apply_schema().await.unwrap();
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let store = SqlxStore::mysql(pool.clone());
    // Another transaction's live start of the key: the store's insert waits for it.
    let mut held = holder.tx().begin().await.unwrap();
    holder
        .sagas()
        .sagas
        .insert(&mut held, &saga("holder", "k-wait"))
        .await
        .unwrap();
    for abort in [true, false] {
        let mut tx = store.tx().begin().await.unwrap();
        let waited = tokio::time::timeout(
            Duration::from_millis(300),
            store
                .sagas()
                .sagas
                .insert(&mut tx, &saga("waiter", "k-wait")),
        )
        .await;
        assert!(waited.is_err(), "dropped while it waited");
        if abort {
            store.tx().abort(&mut tx).await;
        } else {
            drop(tx);
        }
        let mut seen = Vec::new();
        let (mut a, mut b) = (pool.acquire().await.unwrap(), pool.acquire().await.unwrap());
        for c in [&mut a, &mut b] {
            let row = sqlx::query("SELECT CAST(@@SESSION.innodb_lock_wait_timeout AS CHAR) AS t")
                .fetch_one(&mut **c)
                .await
                .unwrap();
            seen.push(sqlx::Row::get::<String, _>(&row, "t"));
        }
        assert_eq!(
            seen,
            ["50", "50"],
            "aborted: {abort}: every pooled session has the default"
        );
    }
    holder.tx().abort(&mut held).await;
}

/// The id of the session behind the transaction's connection.
async fn session_id(kind: Kind, tx: &mut Tx) -> String {
    match kind {
        Kind::Postgres => sqlx::query_scalar("SELECT CAST(pg_backend_pid() AS TEXT)")
            .fetch_one(tx.connection::<sqlx::Postgres>())
            .await
            .unwrap(),
        Kind::MySql => sqlx::query_scalar("SELECT CAST(CONNECTION_ID() AS CHAR)")
            .fetch_one(tx.connection::<sqlx::MySql>())
            .await
            .unwrap(),
    }
}

/// Waits until the server no longer lists the session `id`: its connection was closed, not
/// returned to the pool.
async fn wait_session_gone(raw: &Raw, id: &str) {
    let table = match raw.kind() {
        Kind::Postgres => format!("pg_stat_activity WHERE pid = {id}"),
        Kind::MySql => format!("information_schema.processlist WHERE id = {id}"),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while raw.count(&table).await != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session {id} is still open: its connection went back to the pool"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// An abort with a nested transaction left open on the connection: the store's transaction is
/// rolled back, and the connection is closed (its session ends), never handed back to the pool.
async fn abort_with_a_leaked_nested_transaction(kind: Kind) {
    use sqlx::Connection;
    let server = Server::of(kind).await;
    let (store, raw) = server.store().await;
    let txs = store.tx();
    let mut tx = txs.begin().await.unwrap();
    let session = session_id(kind, &mut tx).await;
    store
        .outbox()
        .append(&mut tx, entry("l-abort"))
        .await
        .unwrap();
    match kind {
        Kind::Postgres => {
            std::mem::forget(tx.connection::<sqlx::Postgres>().begin().await.unwrap())
        }
        Kind::MySql => std::mem::forget(tx.connection::<sqlx::MySql>().begin().await.unwrap()),
    }
    txs.abort(&mut tx).await;
    wait_session_gone(&raw, &session).await;
    assert_eq!(
        raw.count("snodra_outbox WHERE id = 'l-abort'").await,
        0,
        "rolled back"
    );
}

both!(
    abort_with_a_leaked_nested_transaction,
    postgres_an_abort_with_a_nested_transaction_left_open_closes_the_connection,
    mysql_an_abort_with_a_nested_transaction_left_open_closes_the_connection
);

/// PostgreSQL: a statement that failed on the handler's connection aborts the whole transaction,
/// and a `COMMIT` then is answered with a rollback that sqlx reports as a success. The store's
/// commit finds it and fails: nothing is committed, the connection is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_a_commit_after_a_failed_statement_on_the_connection_fails() {
    let server = Server::postgres().await;
    let (store, raw) = server.store().await;
    raw.exec("CREATE TABLE orders (id VARCHAR(64) NOT NULL PRIMARY KEY)")
        .await;
    let txs = store.tx();
    let mut tx = txs.begin().await.unwrap();
    let session = session_id(Kind::Postgres, &mut tx).await;
    sqlx::query("INSERT INTO orders (id) VALUES ('f-1')")
        .execute(tx.connection::<sqlx::Postgres>())
        .await
        .unwrap();
    store.outbox().append(&mut tx, entry("f-1")).await.unwrap();
    assert!(
        store
            .processed_events()
            .record(&mut tx, &marker("m-f-1"))
            .await
            .unwrap()
    );
    let failed = sqlx::query("SELECT 1 / 0")
        .execute(tx.connection::<sqlx::Postgres>())
        .await;
    assert!(failed.is_err(), "the statement fails (and is ignored)");
    let committed = txs.commit(&mut tx).await;
    assert!(
        committed
            .as_ref()
            .is_err_and(|e| e.to_string().contains("aborted")),
        "the commit fails: {committed:?}"
    );
    assert_eq!(
        (
            raw.count("orders WHERE id = 'f-1'").await,
            raw.count("snodra_outbox WHERE id = 'f-1'").await,
            raw.count("snodra_processed_events WHERE id = 'm-f-1'")
                .await,
        ),
        (0, 0, 0),
        "nothing committed"
    );
    wait_session_gone(&raw, &session).await;
}

/// MySQL has no aborted transaction: a failed statement is undone alone and the transaction goes
/// on, so the rest of it commits (as the README says).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_a_failed_statement_on_the_connection_is_undone_alone() {
    let server = Server::mysql().await;
    let (store, raw) = server.store().await;
    raw.exec("CREATE TABLE orders (id VARCHAR(64) NOT NULL PRIMARY KEY)")
        .await;
    let txs = store.tx();
    let mut tx = txs.begin().await.unwrap();
    sqlx::query("INSERT INTO orders (id) VALUES ('f-1')")
        .execute(tx.connection::<sqlx::MySql>())
        .await
        .unwrap();
    store.outbox().append(&mut tx, entry("f-1")).await.unwrap();
    let failed = sqlx::query("INSERT INTO orders (id) VALUES ('f-1')")
        .execute(tx.connection::<sqlx::MySql>())
        .await;
    assert!(failed.is_err(), "a duplicate key (ignored)");
    txs.commit(&mut tx).await.unwrap();
    assert_eq!(
        (
            raw.count("orders WHERE id = 'f-1'").await,
            raw.count("snodra_outbox WHERE id = 'f-1'").await,
        ),
        (1, 1),
        "the rest of the transaction commits"
    );
}

/// A handler that runs a failing statement on its connection, ignores the error and returns `Ok`
/// (PostgreSQL): the record is not acknowledged on that attempt (its commit fails, nothing of it
/// is committed) and is handled again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_a_handler_that_ignores_a_failed_statement_is_not_acknowledged() {
    use std::sync::atomic::{AtomicU32, Ordering};

    use snodra_messaging::testing::InMemoryStores;
    use snodra_messaging::{Event, SnodraMessaging, message_type};

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Placed {
        id: String,
    }
    message_type!(Placed, "swallow.Placed");
    impl Event for Placed {}

    let server = Server::postgres().await;
    let (store, raw) = server.store().await;
    raw.exec("CREATE TABLE orders (id VARCHAR(64) NOT NULL PRIMARY KEY)")
        .await;
    let stores = InMemoryStores::new();
    let mut config = stores.config("swallow");
    config.consumer_max_attempts = 3;
    let attempts = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&attempts);
    let app = SnodraMessaging::builder(config)
        .await
        .unwrap()
        .store(Arc::new(store))
        .event_store(Arc::new(stores.event_store().clone()))
        .event::<Placed, _, _>(move |placed, ctx| {
            let counted = Arc::clone(&counted);
            async move {
                let attempt = counted.fetch_add(1, Ordering::SeqCst) + 1;
                let mut tx = ctx.tx.lock().await;
                let conn = tx.connection::<sqlx::Postgres>();
                sqlx::query("INSERT INTO orders (id) VALUES ($1)")
                    .bind(format!("{}-{attempt}", placed.id))
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| MessagingError::Storage(e.to_string()))?;
                if attempt == 1 {
                    // Fails, and the handler goes on as if it had not.
                    let _ = sqlx::query("SELECT 1 / 0").execute(&mut *conn).await;
                }
                Ok(())
            }
        })
        .start()
        .await
        .unwrap();

    let id = stores
        .publish(&Placed { id: "o".into() }, Some("o"))
        .await
        .unwrap();
    let marker = format!("snodra_processed_events WHERE id = 'processor:default:{id}'");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while raw.count(&marker).await == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the record was never handled with a commit (attempts: {})",
            attempts.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "handled again after the failed commit"
    );
    assert_eq!(
        (
            raw.count("orders WHERE id = 'o-1'").await,
            raw.count("orders WHERE id = 'o-2'").await,
        ),
        (0, 1),
        "the first attempt committed nothing"
    );
    app.shutdown().await;
}
