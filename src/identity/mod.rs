//! Who is calling: the identity read from the forwarded JWT, verification, and handler guards.
//!
//! Trusted-by-default model: an absent token is an anonymous caller and the request proceeds;
//! whether anonymity is acceptable is the handler's decision through a [`Guard`].

mod guard;
mod jwks;
mod resolver;

pub use guard::Guard;
pub use jwks::JwksVerifier;
pub use resolver::IdentityResolver;

use serde_json::{Map, Value};

/// The acting user as seen by a handler.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Identity {
    /// Keycloak subject (`sub`).
    pub user_id: Option<String>,
    /// Realm roles (`realm_access.roles`), without any prefix.
    pub roles: Vec<String>,
    /// True when the token's signature was verified by this process.
    pub verified: bool,
}

impl Identity {
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    pub(crate) fn from_claims(claims: &Map<String, Value>, verified: bool) -> Identity {
        Identity {
            user_id: claims
                .get("sub")
                .and_then(Value::as_str)
                .map(str::to_string),
            roles: realm_roles(claims),
            verified,
        }
    }
}

/// Keycloak realm roles from `realm_access.roles`; empty when the claim is absent or malformed.
pub fn realm_roles(claims: &Map<String, Value>) -> Vec<String> {
    claims
        .get("realm_access")
        .and_then(Value::as_object)
        .and_then(|ra| ra.get("roles"))
        .and_then(Value::as_array)
        .map(|roles| {
            roles
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}
