//! Shared helpers for integration tests.
#![allow(dead_code)]

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::json;

pub const TEST_KEY_PEM: &[u8] = include_bytes!("../fixtures/test_key.pem");
pub const OTHER_KEY_PEM: &[u8] = include_bytes!("../fixtures/other_key.pem");
pub const JWKS_JSON: &str = include_str!("../fixtures/jwks.json");
pub const ISSUER: &str = "https://issuer.test/realms/kraken";

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
        .encode(json!({"sub": subject, "realm_access": {"roles": ["kraken-admin"]}}).to_string());
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

pub mod fake_hub;

/// Opt-in logging for debugging a test: `RUST_LOG=debug cargo test -- --nocapture`.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
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
