//! Turns the forwarded `authorization` header into an [`Identity`].

use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;

use super::{Identity, JwksVerifier, RolesClaim};
use crate::error::MessagingError;

/// With a verifier (the service's JWKS): verified identities, invalid tokens rejected
/// (`UNAUTHENTICATED`). Without one a token is never read, so it is never an identity: the caller
/// is anonymous, whatever the token claims, and the token is only forwarded on by the requests the
/// handler sends (`MessageContext::authorization`); noted once per process at `info`. A handler
/// with a guard cannot be registered without a JWKS
/// ([`SnodraMessagingBuilder::start`](crate::SnodraMessagingBuilder::start) refuses it), so no
/// handler acts on claims nobody checked. Either way an absent header is an anonymous caller, and
/// a header that is not `Bearer <token>` is `UNAUTHENTICATED`; without a JWKS so is a token that is
/// not a well-formed signed JWS ([`check_shape`]), judged by its shape alone.
#[derive(Debug)]
pub struct IdentityResolver {
    verifier: Option<JwksVerifier>,
    noted: AtomicBool,
    roles_claim: RolesClaim,
}

impl IdentityResolver {
    pub fn new(verifier: Option<JwksVerifier>) -> Self {
        IdentityResolver {
            verifier,
            noted: AtomicBool::new(false),
            roles_claim: RolesClaim::default(),
        }
    }

    /// Reads roles from `value` (a dotted claim path, or `scope`) instead of `roles`.
    pub fn with_roles_claim(mut self, value: &str) -> Self {
        self.roles_claim = RolesClaim::parse(value);
        self
    }

    /// Whether forwarded tokens are verified (a JWKS is configured).
    pub fn verifies(&self) -> bool {
        self.verifier.is_some()
    }

    pub async fn resolve(
        &self,
        authorization: Option<&str>,
    ) -> Result<Option<Identity>, MessagingError> {
        let Some(raw) = authorization.filter(|s| !s.trim().is_empty()) else {
            return Ok(None);
        };
        let token = bearer_token(raw).ok_or_else(|| {
            MessagingError::Unauthenticated("malformed authorization header".into())
        })?;
        match &self.verifier {
            Some(verifier) => Ok(Some(Identity::from_claims(
                &verifier.verify(token).await?,
                true,
                &self.roles_claim,
            ))),
            None => {
                check_shape(token)?;
                if !self.noted.swap(true, Ordering::Relaxed) {
                    tracing::info!(
                        "no SNODRA_JWKS_URL: forwarded tokens are not verified, so their callers are anonymous (the tokens are forwarded on unread)"
                    );
                }
                Ok(None)
            }
        }
    }
}

/// What a service without a JWKS can judge of a token: its shape. A well-formed signed JWS has
/// three segments, a header and a claims segment that are base64url JSON objects, an `alg` other
/// than `none` and a signature; anything else (an encrypted, five-segment token included) is
/// `UNAUTHENTICATED`. The claims are parsed for their shape only, never used.
fn check_shape(token: &str) -> Result<(), MessagingError> {
    let refused = |why: &str| MessagingError::Unauthenticated(why.to_string());
    let parts: Vec<&str> = token.splitn(6, '.').collect();
    if parts.len() == 5 {
        return Err(refused("unsupported token"));
    }
    if parts.len() != 3 {
        return Err(refused("unreadable token"));
    }
    let header = json_object(parts[0]).ok_or_else(|| refused("unreadable token"))?;
    // A header whose `alg` is there but not a string is unreadable.
    let alg = match header.get("alg") {
        None => "",
        Some(Value::String(alg)) => alg.as_str(),
        Some(_) => return Err(refused("unreadable token")),
    };
    if alg.eq_ignore_ascii_case("none") || parts[2].is_empty() {
        return Err(refused("unsigned token"));
    }
    json_object(parts[1]).ok_or_else(|| refused("unreadable token"))?;
    Ok(())
}

/// A base64url segment (padding tolerated) holding a JSON object.
fn json_object(segment: &str) -> Option<serde_json::Map<String, Value>> {
    let bytes = URL_SAFE_NO_PAD.decode(segment.trim_end_matches('=')).ok()?;
    match serde_json::from_slice::<Value>(&bytes).ok()? {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

/// The scheme of a bearer header, matched in any letter case at the very start of the value.
const BEARER: &str = "Bearer ";

/// The token of a `Bearer <token>` header: the scheme in any letter case at the very start of
/// the (untrimmed) value, the token what follows it, trimmed; `None` for any other shape
/// (leading whitespace, another scheme, no token).
fn bearer_token(header: &str) -> Option<&str> {
    let scheme = header.get(..BEARER.len())?;
    if !scheme.eq_ignore_ascii_case(BEARER) {
        return None;
    }
    Some(header[BEARER.len()..].trim()).filter(|t| !t.is_empty())
}
