//! Who is calling: the identity read from the forwarded JWT (filled in by the resolver, Task 2).

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
}
