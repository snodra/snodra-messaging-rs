//! Turns the forwarded `authorization` header into an [`Identity`].

use std::sync::atomic::{AtomicBool, Ordering};

use super::{Identity, JwksVerifier, RolesClaim};
use crate::error::MessagingError;

/// With a verifier (the service's JWKS): verified identities, invalid tokens rejected
/// (`UNAUTHENTICATED`). Without one a token is never read, so it is never an identity: the caller
/// is anonymous, whatever the token claims, and the token is only forwarded on by the requests the
/// handler sends (`MessageContext::authorization`); noted once per process at `info`. A handler
/// with a guard cannot be registered without a JWKS
/// ([`SnodraMessagingBuilder::start`](crate::SnodraMessagingBuilder::start) refuses it), so no
/// handler acts on claims nobody checked. Either way an absent header is an anonymous caller, and
/// a header that is not `Bearer <token>` is `UNAUTHENTICATED`.
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
