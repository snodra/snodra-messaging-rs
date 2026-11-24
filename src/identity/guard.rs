//! Opt-in authorization for a handler: an anonymous caller is refused before the payload is
//! decoded, the handler's rule is applied once it is.

use std::sync::Arc;

use super::Identity;
use crate::context::MessageContext;
use crate::error::MessagingError;

type Predicate = dyn Fn(&Identity, &MessageContext) -> bool + Send + Sync;

/// The Rust counterpart of `@RequiresAuthentication(expression)`: an anonymous caller is rejected
/// with `UNAUTHENTICATED` before the payload is decoded (so it cannot probe payload shapes), a
/// caller the predicate refuses with `PERMISSION_DENIED` once the payload is decoded (a payload
/// that does not decode is `INVALID_REQUEST` for any authenticated caller).
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

    /// The part of [`check`](Self::check) applied before the payload is decoded: an anonymous
    /// caller is `UNAUTHENTICATED`.
    pub(crate) fn check_caller(identity: Option<&Identity>) -> Result<(), MessagingError> {
        match identity {
            None => Err(MessagingError::Unauthenticated(
                "handler requires authentication".into(),
            )),
            Some(_) => Ok(()),
        }
    }

    pub fn check(
        &self,
        identity: Option<&Identity>,
        ctx: &MessageContext,
    ) -> Result<(), MessagingError> {
        Self::check_caller(identity)?;
        match identity {
            Some(id) if (self.0)(id, ctx) => Ok(()),
            _ => Err(MessagingError::AccessDenied("access denied".into())),
        }
    }
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Guard")
    }
}
