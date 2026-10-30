//! RS256 verification against the realm's JWKS, cached in memory and refreshed on unknown key ids.

use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::{Map, Value};
use tokio::sync::RwLock;

use crate::error::MessagingError;

const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct Cache {
    fetched_at: Option<Instant>,
    /// When an unknown `kid` last forced a refetch; those refetches are throttled, the initial load is not.
    last_forced_refresh: Option<Instant>,
    keys: JwkSet,
}

/// Verifies forwarded tokens the way the Kotlin `JwtDecoder` does: signature against the JWKS,
/// `exp`, and `iss`.
#[derive(Clone, Debug)]
pub struct JwksVerifier {
    jwks_url: String,
    issuer: String,
    http: reqwest::Client,
    cache: Arc<RwLock<Cache>>,
}

impl JwksVerifier {
    pub fn new(jwks_url: String, issuer: String) -> JwksVerifier {
        JwksVerifier {
            jwks_url,
            issuer,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
            cache: Arc::new(RwLock::new(Cache {
                fetched_at: None,
                last_forced_refresh: None,
                keys: JwkSet { keys: vec![] },
            })),
        }
    }

    /// Verified claims, or `Unauthenticated` with a short reason (never the token).
    pub async fn verify(&self, token: &str) -> Result<Map<String, Value>, MessagingError> {
        let header = decode_header(token).map_err(|_| unauthenticated("unreadable token"))?;
        if header.alg != Algorithm::RS256 {
            return Err(unauthenticated("unsupported token algorithm"));
        }
        let kid = header
            .kid
            .ok_or_else(|| unauthenticated("token has no kid"))?;
        let key = match self.key_for(&kid).await? {
            Some(key) => key,
            None => {
                self.refresh(true).await?;
                self.key_for(&kid)
                    .await?
                    .ok_or_else(|| unauthenticated("unknown signing key"))?
            }
        };
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.issuer]);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        // Keycloak tokens carry `aud` ("account", client ids); like Spring's JwtDecoder we do not validate it.
        validation.validate_aud = false;
        validation.set_required_spec_claims(&["exp", "iss"]);
        let data = decode::<Map<String, Value>>(token, &key, &validation).map_err(|e| {
            tracing::debug!(error = %e, "forwarded token rejected");
            unauthenticated("invalid token")
        })?;
        Ok(data.claims)
    }

    async fn key_for(&self, kid: &str) -> Result<Option<DecodingKey>, MessagingError> {
        if self.cache.read().await.fetched_at.is_none() {
            self.refresh(false).await?;
        }
        let cache = self.cache.read().await;
        match cache.keys.find(kid) {
            Some(jwk) => Ok(Some(
                DecodingKey::from_jwk(jwk).map_err(|_| unauthenticated("unusable signing key"))?,
            )),
            None => Ok(None),
        }
    }

    /// Fetches the JWKS outside the lock. A `throttled` refresh (unknown kid) runs at most once per
    /// minute; a fetch failure is `Unauthenticated` for the caller (the detail is logged, never sent).
    async fn refresh(&self, throttled: bool) -> Result<(), MessagingError> {
        {
            let mut cache = self.cache.write().await;
            if throttled {
                if cache
                    .last_forced_refresh
                    .is_some_and(|t| t.elapsed() < MIN_REFRESH_INTERVAL)
                {
                    return Ok(());
                }
                cache.last_forced_refresh = Some(Instant::now());
            } else if cache.fetched_at.is_some() {
                return Ok(()); // another task loaded the keys while we waited for the lock
            }
        }
        let fetched = self
            .http
            .get(&self.jwks_url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| e.to_string());
        let keys: JwkSet = match fetched {
            Ok(response) => match response.json().await {
                Ok(keys) => keys,
                Err(e) => {
                    tracing::warn!("JWKS parse failed: {e}");
                    return Err(unauthenticated("signing keys unavailable"));
                }
            },
            Err(e) => {
                tracing::warn!("JWKS fetch failed: {e}");
                return Err(unauthenticated("signing keys unavailable"));
            }
        };
        let mut cache = self.cache.write().await;
        cache.keys = keys;
        cache.fetched_at = Some(Instant::now());
        Ok(())
    }
}

fn unauthenticated(reason: &str) -> MessagingError {
    MessagingError::Unauthenticated(reason.to_string())
}
