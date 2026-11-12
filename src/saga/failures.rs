//! Admin API over the parked saga events (`saga_failures`, ADR 0021 rulings 3 and 36): an event
//! whose saga handler failed all its attempts waits here until the cause is fixed, then is retried
//! (re-delivered to the saga type that failed, applied once) or discarded.
//!
//! A retry runs the type's handlers again with the runtime's retries; it ignores the type's
//! marker (which parking left by design) and deletes the failure in the same transaction that
//! applies the event, so concurrent retries apply it once. A failure that fails again stays, with
//! `attempts` increased and the new error. A discarded event keeps its marker: redelivering the
//! record does not apply it either, and a `discard` is final even when it lands while a retry is
//! failing (ruling 36). A parked event whose saga type does not run in this service (not
//! registered, or sagas disabled) is not retried: the retry is refused and the event keeps its
//! error and attempts (amended in H1).

use std::sync::Arc;

use super::ports::SagaPorts;
use super::runtime::SagaRuntime;
use super::store::SagaFailureDocument;
use crate::error::MessagingError;

#[derive(Clone)]
pub struct SagaFailures {
    ports: SagaPorts,
    runtime: Arc<SagaRuntime>,
}

impl SagaFailures {
    pub fn new(runtime: Arc<SagaRuntime>) -> SagaFailures {
        SagaFailures {
            ports: runtime.ports().clone(),
            runtime,
        }
    }

    /// The parked events of saga type `saga_type` (every type when `None`), the longest waiting
    /// first.
    pub async fn list(
        &self,
        saga_type: Option<&str>,
    ) -> Result<Vec<SagaFailureDocument>, MessagingError> {
        let mut tx = self
            .ports
            .tx
            .plain()
            .await
            .map_err(|e| e.into_messaging())?;
        self.ports
            .failures
            .list(&mut tx, saga_type)
            .await
            .map_err(|e| e.into_messaging())
    }

    /// Re-delivers the parked event `id` (`<sagaType>:<eventId>`); `true` when it was applied,
    /// `false` when unknown or failed again. `InvalidMessage` (nothing written) when its saga type
    /// does not run in this service.
    pub async fn retry(&self, id: &str) -> Result<bool, MessagingError> {
        let mut tx = self
            .ports
            .tx
            .plain()
            .await
            .map_err(|e| e.into_messaging())?;
        let found = self
            .ports
            .failures
            .find(&mut tx, id)
            .await
            .map_err(|e| e.into_messaging())?;
        match found {
            Some(failure) => self.runtime.reapply(&failure).await,
            None => Ok(false),
        }
    }

    /// Retries every parked event of saga type `saga_type`; returns how many were applied (0 when
    /// none is parked). `InvalidMessage` (nothing written) when the type does not run in this
    /// service and has parked events.
    pub async fn retry_all(&self, saga_type: &str) -> Result<usize, MessagingError> {
        let parked = self.list(Some(saga_type)).await?;
        if !parked.is_empty() {
            self.runtime.ensure_runs(saga_type, parked.len())?;
        }
        let mut applied = 0;
        for failure in parked {
            if self.runtime.reapply(&failure).await? {
                applied += 1;
            }
        }
        Ok(applied)
    }

    /// Drops the parked event `id` without applying it; `false` when unknown.
    pub async fn discard(&self, id: &str) -> Result<bool, MessagingError> {
        let mut tx = self
            .ports
            .tx
            .plain()
            .await
            .map_err(|e| e.into_messaging())?;
        self.ports
            .failures
            .delete(&mut tx, id)
            .await
            .map_err(|e| e.into_messaging())
    }
}
