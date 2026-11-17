//! Shared helpers for integration tests.
#![allow(dead_code)]

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::json;

pub const TEST_KEY_PEM: &[u8] = include_bytes!("../fixtures/test_key.pem");
pub const OTHER_KEY_PEM: &[u8] = include_bytes!("../fixtures/other_key.pem");
pub const JWKS_JSON: &str = include_str!("../fixtures/jwks.json");
pub const ISSUER: &str = "https://issuer.test/realms/snodra";

pub fn now() -> u64 {
    jsonwebtoken::get_current_timestamp()
}

/// RS256 token signed by the fixture key (`kid = k1`) with Keycloak-shaped claims.
pub fn token(subject: &str, roles: &[&str], issuer: &str, exp: u64) -> String {
    sign(TEST_KEY_PEM, Some("k1"), subject, roles, issuer, exp)
}

pub fn sign(
    pem: &[u8],
    kid: Option<&str>,
    subject: &str,
    roles: &[&str],
    issuer: &str,
    exp: u64,
) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = kid.map(str::to_string);
    let claims = json!({
        "sub": subject,
        "iss": issuer,
        "exp": exp,
        "iat": now() - 10,
        "realm_access": { "roles": roles },
    });
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(pem).expect("pem"),
    )
    .expect("sign")
}

/// Signed token with an `aud` claim, as Keycloak issues them.
pub fn token_with_audience(subject: &str, roles: &[&str], audience: &[&str]) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("k1".into());
    let claims = json!({"sub": subject, "iss": ISSUER, "exp": now() + 300, "aud": audience, "realm_access": {"roles": roles}});
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(TEST_KEY_PEM).unwrap(),
    )
    .unwrap()
}

/// `alg: none` token: two base64url segments and an empty signature.
pub fn unsigned_token(subject: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
    let claims = URL_SAFE_NO_PAD
        .encode(json!({"sub": subject, "realm_access": {"roles": ["snodra-admin"]}}).to_string());
    format!("{header}.{claims}.")
}

/// A signed token whose claims lack `realm_access`.
pub fn token_without_realm_access(subject: &str) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("k1".into());
    let claims = json!({"sub": subject, "iss": ISSUER, "exp": now() + 300});
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(TEST_KEY_PEM).unwrap(),
    )
    .unwrap()
}

pub mod event_store;
pub mod fake_hub;
pub mod idp;
pub mod logs;
pub mod metrics;
pub mod order_saga;
pub mod pki;
pub mod saga;

/// Opt-in logging for debugging a test: `RUST_LOG=debug cargo test -- --nocapture`. The same
/// global subscriber as the log captures' ([`logs::install_global`]), so neither turns the other
/// off.
pub fn init_tracing() {
    logs::install_global();
}

/// Integration tests need Docker; they skip (with a notice) when it is absent.
pub fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

pub mod containers;

/// The MongoDB message store of the database `uri` names (a client of its own), its indexes in
/// place: what a service built on the MongoDB add-on hands the builder.
pub async fn mongo_store(uri: &str) -> std::sync::Arc<dyn snodra_messaging::store::MessageStore> {
    let db = mongo_db(uri).await;
    let store = snodra_messaging_mongo::MongoStore::new(db.client().clone(), db);
    store
        .ensure_indexes(&snodra_messaging_mongo::MongoIndexSettings::default())
        .await
        .expect("indexes");
    std::sync::Arc::new(store)
}

/// The database `uri` names, for a test that reads what the store wrote.
pub async fn mongo_db(uri: &str) -> mongodb::Database {
    mongodb::Client::with_uri_str(uri)
        .await
        .expect("mongo client")
        .default_database()
        .expect("the uri names a database")
}

/// A MongoDB message store whose server is never reached (the driver connects lazily): for the
/// checks that refuse before any store call.
pub async fn mongo_store_unreached() -> std::sync::Arc<dyn snodra_messaging::store::MessageStore> {
    let client =
        mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/x?serverSelectionTimeoutMS=200")
            .await
            .expect("mongo client");
    let db = client.database("x");
    std::sync::Arc::new(snodra_messaging_mongo::MongoStore::new(client, db))
}

/// The MongoDB message store of the database `uri` names, without its indexes: nothing is created
/// in the database until the library writes there.
pub async fn mongo_store_bare(
    uri: &str,
) -> std::sync::Arc<dyn snodra_messaging::store::MessageStore> {
    let db = mongo_db(uri).await;
    std::sync::Arc::new(snodra_messaging_mongo::MongoStore::new(
        db.client().clone(),
        db,
    ))
}

/// The Kafka event store at `bootstrap` (the Kafka add-on), as a service hands the builder.
pub fn kafka_events(
    bootstrap: &str,
) -> std::sync::Arc<dyn snodra_messaging::event_store::EventStore> {
    std::sync::Arc::new(
        snodra_messaging_kafka::KafkaEventStore::new(snodra_messaging_kafka::KafkaConfig::new(
            bootstrap,
        ))
        .expect("kafka event store"),
    )
}

/// The in-memory event store.
pub fn memory_events() -> std::sync::Arc<dyn snodra_messaging::event_store::EventStore> {
    std::sync::Arc::new(snodra_messaging::event_store::memory::MemoryEventStore::default())
}
