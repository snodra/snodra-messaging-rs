//! Opt-in authorization for a handler, evaluated before the payload is decoded.

use std::sync::Arc;

use super::Identity;
use crate::context::MessageContext;
use crate::error::MessagingError;

type Predicate = dyn Fn(&Identity, &MessageContext) -> bool + Send + Sync;

/// The Rust counterpart of `@RequiresAuthentication(expression)`: an anonymous caller is rejected
/// with `UNAUTHENTICATED`, a caller the predicate refuses with `PERMISSION_DENIED`.
#[derive(Clone)]
pub struct Guard(Arc<Predicate>);

impl Guard {
    /// `isAuthenticated()`.
    pub fn authenticated() -> Guard {
        Guard(Arc::new(|_, _| true))
    }

    /// `hasRole('role')`.
    pub fn role(role: &str) -> Guard {
        let role = role.to_string();
        Guard(Arc::new(move |id, _| id.has_role(&role)))
    }

    /// `hasAnyRole(...)`.
    pub fn any_role<I, S>(roles: I) -> Guard
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let roles: Vec<String> = roles.into_iter().map(Into::into).collect();
        Guard(Arc::new(move |id, _| roles.iter().any(|r| id.has_role(r))))
    }

    pub fn custom(f: impl Fn(&Identity, &MessageContext) -> bool + Send + Sync + 'static) -> Guard {
        Guard(Arc::new(f))
    }

    pub fn check(
        &self,
        identity: Option<&Identity>,
        ctx: &MessageContext,
    ) -> Result<(), MessagingError> {
        match identity {
            None => Err(MessagingError::Unauthenticated(
                "handler requires authentication".into(),
            )),
            Some(id) if (self.0)(id, ctx) => Ok(()),
            Some(_) => Err(MessagingError::AccessDenied("access denied".into())),
        }
    }
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Guard")
    }
}
