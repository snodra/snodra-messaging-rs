//! Admin API over the parked saga events (the store's saga failures): an event
//! whose saga handler failed all its attempts waits here until the cause is fixed, then is retried
//! (re-delivered to the saga type that failed, applied once) or discarded.
//!
//! A retry runs the type's handlers again with the runtime's retries; it ignores the type's
//! marker (which parking left by design) and deletes the failure in the same transaction that
//! applies the event, so concurrent retries apply it once. A failure that fails again stays, with
//! `attempts` increased and the new error. A discarded event keeps its marker: redelivering the
//! record does not apply it either, and a `discard` is final even when it lands while a retry is
//! failing. A parked event whose saga type does not run in this service (not
//! registered, or sagas disabled) is not retried: the retry is refused and the event keeps its
//! error and attempts.

use std::sync::Arc;

use super::ports::SagaPorts;
use super::runtime::SagaRuntime;
use super::store::SagaFailureDocument;
use crate::error::MessagingError;

#[derive(Clone)]
pub struct SagaFailures {
    /// `None`: a service without a message store, where every call fails.
    inner: Option<(SagaPorts, Arc<SagaRuntime>)>,
}

impl SagaFailures {
    pub fn new(runtime: Arc<SagaRuntime>) -> SagaFailures {
        SagaFailures {
            inner: Some((runtime.ports().clone(), runtime)),
        }
    }

    /// The API of a service without a message store: every call fails saying so.
    pub fn without_store() -> SagaFailures {
        SagaFailures { inner: None }
    }

    fn inner(&self) -> Result<(&SagaPorts, &Arc<SagaRuntime>), MessagingError> {
        self.inner
            .as_ref()
            .map(|(ports, runtime)| (ports, runtime))
            .ok_or_else(|| {
                MessagingError::InvalidMessage(
                    "the saga failures need a message store: add .store(...)".into(),
                )
            })
    }

    /// The parked events of saga type `saga_type` (every type when `None`), the longest waiting
    /// first.
    pub async fn list(
        &self,
        saga_type: Option<&str>,
    ) -> Result<Vec<SagaFailureDocument>, MessagingError> {
        let (ports, _) = self.inner()?;
        let mut tx = ports.tx.plain().await.map_err(|e| e.into_messaging())?;
        ports
            .failures
            .list(&mut tx, saga_type)
            .await
            .map_err(|e| e.into_messaging())
    }

    /// Re-delivers the parked event `id` (`<sagaType>:<eventId>`); `true` when it was applied,
    /// `false` when unknown or failed again. `InvalidMessage` (nothing written) when its saga type
    /// does not run in this service.
    pub async fn retry(&self, id: &str) -> Result<bool, MessagingError> {
        let (ports, _) = self.inner()?;
        let mut tx = ports.tx.plain().await.map_err(|e| e.into_messaging())?;
        let found = ports
            .failures
            .find(&mut tx, id)
            .await
            .map_err(|e| e.into_messaging())?;
        match found {
            Some(failure) => self.inner()?.1.reapply(&failure).await,
            None => Ok(false),
        }
    }

    /// Retries every parked event of saga type `saga_type`; returns how many were applied (0 when
    /// none is parked). `InvalidMessage` (nothing written) when the type does not run in this
    /// service and has parked events.
    pub async fn retry_all(&self, saga_type: &str) -> Result<usize, MessagingError> {
        let parked = self.list(Some(saga_type)).await?;
        let (_, runtime) = self.inner()?;
        if !parked.is_empty() {
            runtime.ensure_runs(saga_type, parked.len())?;
        }
        let mut applied = 0;
        for failure in parked {
            if runtime.reapply(&failure).await? {
                applied += 1;
            }
        }
        Ok(applied)
    }

    /// Drops the parked event `id` without applying it; `false` when unknown.
    pub async fn discard(&self, id: &str) -> Result<bool, MessagingError> {
        let (ports, _) = self.inner()?;
        let mut tx = ports.tx.plain().await.map_err(|e| e.into_messaging())?;
        ports
            .failures
            .delete(&mut tx, id)
            .await
            .map_err(|e| e.into_messaging())
    }
}
