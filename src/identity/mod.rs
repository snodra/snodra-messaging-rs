//! Who is calling: the identity read from the forwarded JWT, verification, and handler guards.
//!
//! An absent token is an anonymous caller and the request proceeds; whether anonymity is
//! acceptable is the handler's decision through a [`Guard`]. A forwarded token is an identity only
//! once verified against the service's JWKS (`Config::jwks_url`): without one every caller is
//! anonymous ([`IdentityResolver`]), and a handler with a guard is a startup error.

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
    /// The token's subject (`sub`).
    pub user_id: Option<String>,
    /// The roles read from the configured claim ([RolesClaim]), without any prefix.
    pub roles: Vec<String>,
    /// True when the token's signature was verified by this process: always, for an identity a
    /// handler sees (without a JWKS there is none).
    pub verified: bool,
}

impl Identity {
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    pub(crate) fn from_claims(
        claims: &Map<String, Value>,
        verified: bool,
        roles_claim: &RolesClaim,
    ) -> Identity {
        Identity {
            user_id: claims
                .get("sub")
                .and_then(Value::as_str)
                .map(str::to_string),
            roles: roles_claim.roles(claims),
            verified,
        }
    }
}

/// Where a token's roles come from: a dotted path into the claims, or the space-separated `scope`.
///
/// Providers disagree, so the claim is configuration (`SNODRA_SECURITY_ROLES_CLAIM`): `roles` by
/// default, `realm_access.roles` for providers that nest them there, `groups` for those that use groups, `scope` for OAuth2 scopes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RolesClaim {
    Scope,
    Path(Vec<String>),
}

impl Default for RolesClaim {
    fn default() -> Self {
        RolesClaim::Path(vec!["roles".to_string()])
    }
}

impl RolesClaim {
    /// Parses a claim path (`realm_access.roles`, `groups`, `scope`); blank means the default.
    pub fn parse(value: &str) -> RolesClaim {
        let value = value.trim();
        if value.is_empty() {
            return RolesClaim::default();
        }
        if value.eq_ignore_ascii_case("scope") {
            return RolesClaim::Scope;
        }
        RolesClaim::Path(
            value
                .split('.')
                .filter(|segment| !segment.is_empty())
                .map(str::to_string)
                .collect(),
        )
    }

    /// The roles this claim holds for `claims`; empty when it is absent or malformed.
    pub fn roles(&self, claims: &Map<String, Value>) -> Vec<String> {
        match self {
            RolesClaim::Scope => claims
                .get("scope")
                .and_then(Value::as_str)
                .map(|scopes| {
                    scopes
                        .split_whitespace()
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            RolesClaim::Path(path) => {
                let Some((first, rest)) = path.split_first() else {
                    return Vec::new();
                };
                let mut node = claims.get(first);
                for segment in rest {
                    node = node.and_then(Value::as_object).and_then(|o| o.get(segment));
                }
                node.and_then(Value::as_array)
                    .map(|roles| {
                        roles
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default()
            }
        }
    }
}
