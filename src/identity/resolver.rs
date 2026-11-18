//! Turns the forwarded `authorization` header into an [`Identity`].

use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Map, Value};

use super::{Identity, JwksVerifier, RolesClaim};
use crate::error::MessagingError;

/// With a verifier: verified identities (`verified = true`), invalid tokens rejected. Without one:
/// the claims of a *signed* token are read without checking the signature (`verified = false`),
/// warned once per process; unsigned (`alg: none`) and encrypted (five-segment) tokens are rejected.
/// Absence is never rejected: the caller is anonymous.
#[derive(Debug)]
pub struct IdentityResolver {
    verifier: Option<JwksVerifier>,
    warned: AtomicBool,
    roles_claim: RolesClaim,
}

impl IdentityResolver {
    pub fn new(verifier: Option<JwksVerifier>) -> Self {
        IdentityResolver {
            verifier,
            warned: AtomicBool::new(false),
            roles_claim: RolesClaim::default(),
        }
    }

    /// Reads roles from `value` (a dotted claim path, or `scope`) instead of `roles`.
    pub fn with_roles_claim(mut self, value: &str) -> Self {
        self.roles_claim = RolesClaim::parse(value);
        self
    }

    pub async fn resolve(
        &self,
        authorization: Option<&str>,
    ) -> Result<Option<Identity>, MessagingError> {
        let Some(raw) = authorization.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        let token = raw
            .strip_prefix("Bearer ")
            .or_else(|| raw.strip_prefix("bearer "))
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                MessagingError::Unauthenticated("malformed authorization header".into())
            })?;
        match &self.verifier {
            Some(verifier) => Ok(Some(Identity::from_claims(
                &verifier.verify(token).await?,
                true,
                &self.roles_claim,
            ))),
            None => {
                if !self.warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "forwarded identities are trusted without verification: no SNODRA_JWKS_URL configured"
                    );
                }
                Ok(Some(Identity::from_claims(
                    &unverified_claims(token)?,
                    false,
                    &self.roles_claim,
                )))
            }
        }
    }
}

/// Claims of a signed JWS without signature verification. Rejects `alg: none` and non-JWS shapes.
fn unverified_claims(token: &str) -> Result<Map<String, Value>, MessagingError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() == 5 {
        return Err(MessagingError::Unauthenticated("unsupported token".into()));
    }
    if parts.len() != 3 {
        return Err(MessagingError::Unauthenticated("unreadable token".into()));
    }
    let header: Map<String, Value> = decode_segment(parts[0])?;
    let alg = header.get("alg").and_then(Value::as_str).unwrap_or("");
    if alg.eq_ignore_ascii_case("none") || parts[2].is_empty() {
        return Err(MessagingError::Unauthenticated("unsigned token".into()));
    }
    decode_segment(parts[1])
}

fn decode_segment(segment: &str) -> Result<Map<String, Value>, MessagingError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(segment.trim_end_matches('='))
        .map_err(|_| MessagingError::Unauthenticated("unreadable token".into()))?;
    serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .ok_or_else(|| MessagingError::Unauthenticated("unreadable token".into()))
}
